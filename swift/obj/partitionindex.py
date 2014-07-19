# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
A sqlite-backed index and hashing scheme for object server contents.

It implements the same hashing strategy as hashes.pkl - an md5 of the basenames
of all files, sorted by the full path of the file.  This is horrible, and I
only wish we could find the handsome person responsible for it.

Later, when we're all living in the future, it would be neat to migrate toward
something like the order-independent hashes in the container databases, so we
can update them continuously instead of invalidating and rehashing.  We could
also decouple the number of hash groups from the possible values of a set
number of hex digits.  It's also possible this new hashing strategy could
better support modifying the ring partition power.

File additions and removals are journaled and applied later by the replicator,
because the database updates are really too slow to not be batched up in
transactions.

Existing hashes from hashes.pkl files are re-used, so a full re-indexing isn't
required.  hashes.pkl files are also still written out, to support rolling back
code in the dystopian future where this scheme fails miserably.
"""

import contextlib
import sqlite3
import hashlib
import os
import time
import cPickle as pickle

from swift.common.utils import write_pickle, remove_file as unlink, \
    lock_path, lock_file


JOURNAL_FILE = 'hashes.db.pending-journal'
INDEX_DB_FILE = 'hashes.db'
HASH_FILE = 'hashes.pkl'
PICKLE_PROTOCOL = 2


class SuffixHash(object):
    """
    A sqlite aggregator for re-hashing a suffix worth of filenames.
    """
    def __init__(self):
        self.filelist = []

    def step(self, value):
        self.filelist.append(value)

    def finalize(self):
        self.filelist.sort()
        return hashlib.md5(''.join(self.filelist)).hexdigest()


class PartitionIndexDB(sqlite3.Connection):
    """
    Encapsulates all operations on the partition hashes database.
    Hopefully not too much application logic finds its way in here.
    """
    def __init__(self, path, *args, **kwargs):
        kwargs['check_same_thread'] = False
        database_dir = os.path.dirname(path)
        with lock_path(database_dir):
            database_existed = os.path.exists(path)
            sqlite3.Connection.__init__(self, path, *args, **kwargs)
            self.execute('PRAGMA journal_mode = WAL')
            self.execute('PRAGMA synchronous = NORMAL')
            self.create_aggregate('suffix_hash', 1, SuffixHash)
            self.row_factory = sqlite3.Row
            self.text_factory = str
            if not database_existed:
                self.executescript('''
                    CREATE TABLE filelist (filename TEXT PRIMARY KEY,
                        tombstone_ts REAL, suffix TEXT);
                    CREATE INDEX filelist_tombstone ON filelist (tombstone_ts);
                    CREATE INDEX filelist_suffix ON filelist (suffix);

                    CREATE TABLE hashes (suffix TEXT PRIMARY KEY,
                        hash TEXT DEFAULT NULL,
                        repopulate INTEGER DEFAULT 1);
                    -- repopulate is set to trigger a walk of the filesystem.
                ''')
                self.commit()

    def add_remove_files(self, adds, removes):
        modified_suffixes = set()
        add_args = []
        remove_args = []
        for filename in adds:
            suffix, obj_hash, timestamp, ext = file_info(filename)
            modified_suffixes.add(suffix)
            if ext == 'ts':
                add_args.append((filename, suffix, timestamp))
            else:
                add_args.append((filename, suffix, None))
        for filename in removes:
            suffix, obj_hash, timestamp, ext = file_info(filename)
            modified_suffixes.add(suffix)
            remove_args.append((filename,))
        self.executemany("INSERT OR IGNORE INTO filelist (filename, suffix, "
                         "tombstone_ts) VALUES (?, ?, ?)", add_args)
        self.executemany("DELETE FROM filelist WHERE filename=?", remove_args)
        self.executemany("INSERT OR IGNORE INTO hashes (suffix) VALUES (?)",
                         [(suff,) for suff in modified_suffixes])
        self.executemany('''
            UPDATE hashes SET hash = (SELECT suffix_hash(filename)
            FROM filelist WHERE filelist.suffix = hashes.suffix)
            WHERE suffix = ?
            '''), [(suff,) for suff in modified_suffixes])

    def mark_populated(self, suffix):
        self.execute('UPDATE hashes SET repopulate = 0 WHERE suffix=?', suffix)

    def get_hash(self, suffix):
        return self.execute('SELECT hash FROM hashes WHERE suffix=?',
                            (suffix,)).fetchone()[0]

    def expired_tombstones(self, reclaim_age):
        return [rec[0] for rec in self.execute(
                'SELECT filename FROM filelist WHERE tombstone_ts < ?',
                (time.time() - reclaim_age,))]

    def get_hashes(self):
        hashes = {}
        for rec in self.execute('SELECT suffix, hash, repopulate from hashes'):
            if rec['repopulate'] != 0:
                hashes[rec['suffix']] = None
            else:
                hashes[rec['suffix']] = rec['hash']
        return hashes

    def files_in_suffixes(self, suffixes):
        hashlist = []
        for rec in self.execute(
                'SELECT filename FROM filelist WHERE suffix in (%s)' %
                ','.join('?' * len(suffixes)), suffixes)
            suffix, obj_hash, timestamp, ext = file_info(rec[0])
            hashlist.append((obj_hash, timestamp))
        return hashlist


def file_info(path):
    """
    Parse information from the file's path.

    :param path: path to the file
    :returns: a tuple of (suffix, object hash, timestamp, extension)
    """
    path, basename = os.path.split(path)
    path, obj_hash = os.path.split(path)
    partition_dir, suffix = os.path.split(path)
    suffix = os.path.basename(os.path.dirname(os.path.dirname(path)))
    timestamp, ext = basename.split('.', 1)
    return suffix, obj_hash, timestamp, ext


@contextlib.contextmanager
def _open_partition_db(partition_dir):
    """
    Returns a handle for the partition's hashes.db.
    Applies any pending updates from the journal first.

    :param partition_dir: directory of the partition
    """
    db = sqlite3.connect(os.path.join(partition_dir, INDEX_DB_FILE))
    with lock_file(os.path.join(partition_dir, JOURNAL_FILE)) as journal_file:
        adds = []
        removes = []
        for line in journal_file:
            op, filename = line.strip().split(':', 1)
            if op == 'add':
                adds.append(filename)
            elif op == 'remove':
                removes.append(filename)
        if adds or removes:
            db.add_remove_files(db, adds, removes)
            db.commit()
    try:
        yield db
    except Exception:
        pass
    db.close()


def _journal_op(op, path):
    """
    Append an operation to the journal.

    :param op: 'add' or 'remove' for the operation
    :param path: path of the file
    """
    if not os.path.exists(partition_dir):
        return
    partition_dir = os.path.dirname(os.path.dirname(os.path.dirname(path)))
    path_in_partition = os.path.relpath(path, partition_dir)
    db_journal = os.path.join(partition_dir, JOURNAL_FILE)
    with lock_file(db_journal, append=True, unlink=False) as fp:
        fp.write('%s:%s\n' % (op, path_in_partition))
        os.fdatasync(fp.fileno())


def remove_file(path):
    """
    Remove a file from the partition index.

    :param path: absolute path of the file to be removed from the index.
    """
    _journal_append('remove', path)


def add_file(path):
    """
    Add a file to the partition index.

    :param path: absolute path of the file to be indexed
    """
    _journal_append('add', path)


def _populate_hash(partition_dir, suffix):
    """
    Populate the contents of a suffix directory in the database.

    :raises PathNotDir: if given path is not a valid directory
    :raises OSError: for non-ENOTDIR errors
    """
    path_contents = os.listdir(os.path.join(partition_dir, suffix))
    add_files = []
    for hsh in path_contents:
        hsh_path = os.path.join(partition_dir, suffix, hsh)
        if not os.path.isdir(hsh_path):
            continue
        for filename in os.listdir(hsh_path):
            path = os.path.join(hsh_path, filename)
            if not os.path.isfile(path):
                continue
            path_in_partition = os.path.relpath(path, partition_dir)
            add_files.append(path_in_partition)
    with _open_partition_db(partition_dir) as db:
        db.add_remove_files(db, add_files, [])
        db.mark_populated(suffix)
        db.commit()
        return db.get_hash(suffix)


def get_hashes(partition_dir, reclaim_age):
    """
    Return a list of hashes for the partition.

    :param partition_dir: directory of the partition
    :returns: a dict of {bucket: hash} for the partition
    """
    hashes_file = os.path.join(partition_dir, HASH_FILE)
    records = dict((suff, None) for suff in os.listdir(partition_dir)
                   if os.path.isdir(os.path.join(partition_dir, suff)))
    if os.path.exists(hashes_file):
    try:
        with open(hashes_file, 'rb') as fp:
            records.update(pickle.load(fp))
    except Exception:
        pass
    with _open_partition_db(partition_dir) as db:
        dl = []
        # reap any expired tombstones
        for filename in db.expired_tombstones(reclaim_age):
            file_path = os.path.join(partition_dir, filename)
            try:
                unlink(file_path)
                dl.append(rec[0])
                os.rmdir(os.path.dirname(file_path))
            except OSError:
                pass
        db.add_remove_files(db, [], dl)
        db.commit()
        records.update(db.get_hashes())
    for key, value in records.items():
        if value is None:
            records[key] = _populate_hash(partition_dir, key)
    write_pickle(records, hashes_file, partition_dir, PICKLE_PROTOCOL)
    return records


def yield_hashes(partition_dir, suffixes):
    """
    Return a list of (hash, timestamp) for files in the given suffixes.
    """
    with _open_partition_db(partition_dir) as db:
        return db.files_in_suffixes(suffixes)
