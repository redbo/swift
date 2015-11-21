# Copyright (c) 2010 OpenStack Foundation
#
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

import json
import unittest

from swift.common.swob import Request, Response
from swift.common.middleware import staticweb


meta_map = {
    'c1': {'status': 401},
    'c2': {},
    'c3': {'meta': {'web-index': 'index.html',
                    'web-listings': 't'}},
    'c3b': {'meta': {'web-index': 'index.html',
                     'web-listings': 't'}},
    'c4': {'meta': {'web-index': 'index.html',
                    'web-error': 'error.html',
                    'web-listings': 't',
                    'web-listings-css': 'listing.css',
                    'web-directory-type': 'text/dir'}},
    'c5': {'meta': {'web-index': 'index.html',
                    'web-error': 'error.html',
                    'web-listings': 't',
                    'web-listings-css': 'listing.css'}},
    'c6': {'meta': {'web-listings': 't'}},
    'c7': {'meta': {'web-listings': 'f'}},
    'c8': {'meta': {'web-error': 'error.html',
                    'web-listings': 't',
                    'web-listings-css':
                    'http://localhost/stylesheets/listing.css'}},
    'c9': {'meta': {'web-error': 'error.html',
                    'web-listings': 't',
                    'web-listings-css':
                    '/absolute/listing.css'}},
    'c10': {'meta': {'web-listings': 't'}},
    'c11': {'meta': {'web-index': 'index.html'}},
    'c11a': {'meta': {'web-index': 'index.html',
             'web-directory-type': 'text/directory'}},
    'c12': {'meta': {'web-index': 'index.html',
                     'web-error': 'error.html'}},
    'c13': {'meta': {'web-listings': 'f',
                     'web-listings-css': 'listing.css'}},
}


def mock_get_container_info(env, app, swift_source='SW'):
    container = env['PATH_INFO'].rstrip('/').split('/')[3]
    container_info = meta_map[container]
    container_info.setdefault('status', 200)
    container_info.setdefault('read_acl', '.r:*')
    return container_info


class FakeApp(object):

    def __init__(self, status_headers_body_iter=None):
        self.calls = 0
        self.get_c4_called = False

    def __call__(self, env, start_response):
        self.calls += 1
        if env['PATH_INFO'] == '/':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1':
            return Response(
                status='412 Precondition Failed')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a':
            return Response(status='401 Unauthorized')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c1':
            return Response(status='401 Unauthorized')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c2':
            return self.listing(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c2/one.txt':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3':
            return self.listing(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/index.html':
            return Response(status='200 Ok', body='''
<html>
    <body>
        <h1>Test main index.html file.</h1>
        <p>Visit <a href="subdir">subdir</a>.</p>
        <p>Don't visit <a href="subdir2/">subdir2</a> because it doesn't really
           exist.</p>
        <p>Visit <a href="subdir3">subdir3</a>.</p>
        <p>Visit <a href="subdir3/subsubdir">subdir3/subsubdir</a>.</p>
    </body>
</html>
            ''')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3b':
            return self.listing(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3b/index.html':
            resp = Response(status='204 No Content')
            resp.app_iter = iter([])
            return resp(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdir':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdir/':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdir/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdir3/subsubdir':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdir3/subsubdir/':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdir3/subsubdir/index.html':
            return Response(status='200 Ok', body='index file')(env,
                                                                start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdirx/':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdirx/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdiry/':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdiry/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdirz':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/subdirz/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/unknown':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c3/unknown/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c4':
            self.get_c4_called = True
            return self.listing(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c4/one.txt':
            return Response(
                status='200 Ok',
                headers={'x-object-meta-test': 'value'},
                body='1')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c4/two.txt':
            return Response(status='503 Service Unavailable')(env,
                                                              start_response)
        elif env['PATH_INFO'] == '/v1/a/c4/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c4/subdir/':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c4/subdir/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c4/unknown':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c4/unknown/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c4/404error.html':
            return Response(status='200 Ok', body='''
<html>
    <body style="background: #000000; color: #ffaaaa">
        <p>Chrome's 404 fancy-page sucks.</p>
    </body>
</html>
            '''.strip())(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c5':
            return self.listing(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c5/index.html':
            return Response(status='503 Service Unavailable')(env,
                                                              start_response)
        elif env['PATH_INFO'] == '/v1/a/c5/503error.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c5/unknown':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c5/unknown/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c5/404error.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c6':
            return self.listing(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c6/subdir':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] in ('/v1/a/c7', '/v1/a/c7/'):
            return self.listing(env, start_response)
        elif env['PATH_INFO'] in ('/v1/a/c8', '/v1/a/c8/'):
            return self.listing(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c8/subdir/':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] in ('/v1/a/c9', '/v1/a/c9/'):
            return self.listing(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c9/subdir/':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] in ('/v1/a/c10', '/v1/a/c10/'):
            return self.listing(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c10/\xe2\x98\x83/':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c10/\xe2\x98\x83/\xe2\x98\x83/':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] in ('/v1/a/c11', '/v1/a/c11/'):
            return self.listing(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c11/subdir/':
            return Response(status='200 Ok', headers={
                'Content-Type': 'application/directory'})(
                    env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c11/subdir/index.html':
            return Response(status='200 Ok', body='''
<html>
    <body>
        <h2>c11 subdir index</h2>
    </body>
</html>
            '''.strip())(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c11/subdir2/':
            return Response(status='200 Ok', headers={'Content-Type':
                            'application/directory'})(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c11/subdir2/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] in ('/v1/a/c11a', '/v1/a/c11a/'):
            return self.listing(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c11a/subdir/':
            return Response(status='200 Ok', headers={'Content-Type':
                            'text/directory'})(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c11a/subdir/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c11a/subdir2/':
            return Response(status='200 Ok', headers={'Content-Type':
                            'application/directory'})(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c11a/subdir2/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c11a/subdir3/':
            return Response(status='200 Ok', headers={'Content-Type':
                            'not_a/directory'})(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c11a/subdir3/index.html':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a/c12/index.html':
            return Response(status='200 Ok', body='index file')(env,
                                                                start_response)
        elif env['PATH_INFO'] == '/v1/a/c12/200error.html':
            return Response(status='200 Ok', body='error file')(env,
                                                                start_response)
        else:
            raise Exception('Unknown path %r' % env['PATH_INFO'])

    def listing(self, env, start_response):
        headers = {'x-container-read': '.r:*'}
        if ((env['PATH_INFO'] in (
                '/v1/a/c3', '/v1/a/c4', '/v1/a/c8', '/v1/a/c9'))
            and (env['QUERY_STRING'] ==
                 'delimiter=/&format=json&prefix=subdir/')):
            headers.update({'X-Container-Object-Count': '12',
                            'X-Container-Bytes-Used': '73763',
                            'X-Container-Read': '.r:*',
                            'Content-Type': 'application/json; charset=utf-8'})
            body = '''
                [{"name":"subdir/1.txt",
                  "hash":"5f595114a4b3077edfac792c61ca4fe4", "bytes":20,
                  "content_type":"text/plain",
                  "last_modified":"2011-03-24T04:27:52.709100"},
                 {"name":"subdir/2.txt",
                  "hash":"c85c1dcd19cf5cbac84e6043c31bb63e", "bytes":20,
                  "content_type":"text/plain",
                  "last_modified":"2011-03-24T04:27:52.734140"},
                 {"subdir":"subdir3/subsubdir/"}]
            '''.strip()
        elif env['PATH_INFO'] == '/v1/a/c3' and env['QUERY_STRING'] == \
                'delimiter=/&format=json&prefix=subdiry/':
            headers.update({'X-Container-Object-Count': '12',
                            'X-Container-Bytes-Used': '73763',
                            'X-Container-Read': '.r:*',
                            'Content-Type': 'application/json; charset=utf-8'})
            body = '[]'
        elif env['PATH_INFO'] == '/v1/a/c3' and env['QUERY_STRING'] == \
                'limit=1&format=json&delimiter=/&limit=1&prefix=subdirz/':
            headers.update({'X-Container-Object-Count': '12',
                            'X-Container-Bytes-Used': '73763',
                            'X-Container-Read': '.r:*',
                            'Content-Type': 'application/json; charset=utf-8'})
            body = '''
                [{"name":"subdirz/1.txt",
                  "hash":"5f595114a4b3077edfac792c61ca4fe4", "bytes":20,
                  "content_type":"text/plain",
                  "last_modified":"2011-03-24T04:27:52.709100"}]
            '''.strip()
        elif env['PATH_INFO'] == '/v1/a/c6' and env['QUERY_STRING'] == \
                'limit=1&format=json&delimiter=/&limit=1&prefix=subdir/':
            headers.update({'X-Container-Object-Count': '12',
                            'X-Container-Bytes-Used': '73763',
                            'X-Container-Read': '.r:*',
                            'X-Container-Web-Listings': 't',
                            'Content-Type': 'application/json; charset=utf-8'})
            body = '''
                [{"name":"subdir/1.txt",
                  "hash":"5f595114a4b3077edfac792c61ca4fe4", "bytes":20,
                  "content_type":"text/plain",
                  "last_modified":"2011-03-24T04:27:52.709100"}]
            '''.strip()
        elif env['PATH_INFO'] == '/v1/a/c10' and (
                env['QUERY_STRING'] ==
                'delimiter=/&format=json&prefix=%E2%98%83/' or
                env['QUERY_STRING'] ==
                'delimiter=/&format=json&prefix=%E2%98%83/%E2%98%83/'):
            headers.update({'X-Container-Object-Count': '12',
                            'X-Container-Bytes-Used': '73763',
                            'X-Container-Read': '.r:*',
                            'X-Container-Web-Listings': 't',
                            'Content-Type': 'application/json; charset=utf-8'})
            body = '''
                [{"name":"\u2603/\u2603/one.txt",
                  "hash":"73f1dd69bacbf0847cc9cffa3c6b23a1", "bytes":22,
                  "content_type":"text/plain",
                  "last_modified":"2011-03-24T04:27:52.709100"},
                 {"subdir":"\u2603/\u2603/"}]
            '''.strip()
        elif 'prefix=' in env['QUERY_STRING']:
            return Response(status='204 No Content')(env, start_response)
        elif 'format=json' in env['QUERY_STRING']:
            headers.update({'X-Container-Object-Count': '12',
                            'X-Container-Bytes-Used': '73763',
                            'Content-Type': 'application/json; charset=utf-8'})
            body = '''
                [{"name":"401error.html",
                  "hash":"893f8d80692a4d3875b45be8f152ad18", "bytes":110,
                  "content_type":"text/html",
                  "last_modified":"2011-03-24T04:27:52.713710"},
                 {"name":"404error.html",
                  "hash":"62dcec9c34ed2b347d94e6ca707aff8c", "bytes":130,
                  "content_type":"text/html",
                  "last_modified":"2011-03-24T04:27:52.720850"},
                 {"name":"index.html",
                  "hash":"8b469f2ca117668a5131fe9ee0815421", "bytes":347,
                  "content_type":"text/html",
                  "last_modified":"2011-03-24T04:27:52.683590"},
                 {"name":"listing.css",
                  "hash":"7eab5d169f3fcd06a08c130fa10c5236", "bytes":17,
                  "content_type":"text/css",
                  "last_modified":"2011-03-24T04:27:52.721610"},
                 {"name":"one.txt", "hash":"73f1dd69bacbf0847cc9cffa3c6b23a1",
                  "bytes":22, "content_type":"text/plain",
                  "last_modified":"2011-03-24T04:27:52.722270"},
                 {"name":"subdir/1.txt",
                  "hash":"5f595114a4b3077edfac792c61ca4fe4", "bytes":20,
                  "content_type":"text/plain",
                  "last_modified":"2011-03-24T04:27:52.709100"},
                 {"name":"subdir/2.txt",
                  "hash":"c85c1dcd19cf5cbac84e6043c31bb63e", "bytes":20,
                  "content_type":"text/plain",
                  "last_modified":"2011-03-24T04:27:52.734140"},
                 {"name":"subdir/\u2603.txt",
                  "hash":"7337d028c093130898d937c319cc9865", "bytes":72981,
                  "content_type":"text/plain",
                  "last_modified":"2011-03-24T04:27:52.735460"},
                 {"name":"subdir2", "hash":"d41d8cd98f00b204e9800998ecf8427e",
                  "bytes":0, "content_type":"text/directory",
                  "last_modified":"2011-03-24T04:27:52.676690"},
                 {"name":"subdir3/subsubdir/index.html",
                  "hash":"04eea67110f883b1a5c97eb44ccad08c", "bytes":72,
                  "content_type":"text/html",
                  "last_modified":"2011-03-24T04:27:52.751260"},
                 {"name":"two.txt", "hash":"10abb84c63a5cff379fdfd6385918833",
                  "bytes":22, "content_type":"text/plain",
                  "last_modified":"2011-03-24T04:27:52.825110"},
                 {"name":"\u2603/\u2603/one.txt",
                  "hash":"73f1dd69bacbf0847cc9cffa3c6b23a1", "bytes":22,
                  "content_type":"text/plain",
                  "last_modified":"2011-03-24T04:27:52.935560"}]
            '''.strip()
        else:
            headers.update({'X-Container-Object-Count': '12',
                            'X-Container-Bytes-Used': '73763',
                            'Content-Type': 'text/plain; charset=utf-8'})
            body = '\n'.join(['401error.html', '404error.html', 'index.html',
                              'listing.css', 'one.txt', 'subdir/1.txt',
                              'subdir/2.txt', u'subdir/\u2603.txt', 'subdir2',
                              'subdir3/subsubdir/index.html', 'two.txt',
                              u'\u2603/\u2603/one.txt'])
        return Response(status='200 Ok', headers=headers,
                        body=body)(env, start_response)


class TestStaticWeb(unittest.TestCase):

    def setUp(self):
        self.app = FakeApp()
        self.test_staticweb = staticweb.filter_factory({})(self.app)
        self._orig_get_container_info = staticweb.get_container_info
        staticweb.get_container_info = mock_get_container_info

    def tearDown(self):
        staticweb.get_container_info = self._orig_get_container_info

    def test_app_set(self):
        app = FakeApp()
        sw = staticweb.filter_factory({})(app)
        self.assertEqual(sw.app, app)

    def test_conf_set(self):
        conf = {'blah': 1}
        sw = staticweb.filter_factory(conf)(FakeApp())
        self.assertEqual(sw.conf, conf)

    def test_root(self):
        resp = Request.blank('/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 404)

    def test_version(self):
        resp = Request.blank('/v1').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 412)

    def test_account(self):
        resp = Request.blank('/v1/a').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 401)

    def test_container1(self):
        resp = Request.blank('/v1/a/c1').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 401)

    def test_container1_web_mode_explicitly_off(self):
        resp = Request.blank('/v1/a/c1',
                             headers={'x-web-mode': 'false'}).get_response(
                                 self.test_staticweb)
        self.assertEqual(resp.status_int, 401)

    def test_container1_web_mode_explicitly_on(self):
        resp = Request.blank('/v1/a/c1',
                             headers={'x-web-mode': 'true'}).get_response(
                                 self.test_staticweb)
        self.assertEqual(resp.status_int, 404)

    def test_container2(self):
        resp = Request.blank('/v1/a/c2').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.content_type, 'text/plain')
        self.assertEqual(len(resp.body.split('\n')),
                         int(resp.headers['x-container-object-count']))

    def test_container2_web_mode_explicitly_off(self):
        resp = Request.blank(
            '/v1/a/c2',
            headers={'x-web-mode': 'false'}).get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.content_type, 'text/plain')
        self.assertEqual(len(resp.body.split('\n')),
                         int(resp.headers['x-container-object-count']))

    def test_container2_web_mode_explicitly_on(self):
        resp = Request.blank(
            '/v1/a/c2',
            headers={'x-web-mode': 'true'}).get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 404)

    def test_container2onetxt(self):
        resp = Request.blank(
            '/v1/a/c2/one.txt').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 404)

    def test_container2json(self):
        resp = Request.blank(
            '/v1/a/c2?format=json').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.content_type, 'application/json')
        self.assertEqual(len(json.loads(resp.body)),
                         int(resp.headers['x-container-object-count']))

    def test_container2json_web_mode_explicitly_off(self):
        resp = Request.blank(
            '/v1/a/c2?format=json',
            headers={'x-web-mode': 'false'}).get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.content_type, 'application/json')
        self.assertEqual(len(json.loads(resp.body)),
                         int(resp.headers['x-container-object-count']))

    def test_container2json_web_mode_explicitly_on(self):
        resp = Request.blank(
            '/v1/a/c2?format=json',
            headers={'x-web-mode': 'true'}).get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 404)

    def test_container3(self):
        resp = Request.blank('/v1/a/c3').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 301)
        self.assertEqual(resp.headers['location'],
                         'http://localhost/v1/a/c3/')

    def test_container3indexhtml(self):
        resp = Request.blank('/v1/a/c3/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('Test main index.html file.' in resp.body)

    def test_container3subsubdir(self):
        resp = Request.blank(
            '/v1/a/c3/subdir3/subsubdir').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 301)

    def test_container3subsubdircontents(self):
        resp = Request.blank(
            '/v1/a/c3/subdir3/subsubdir/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, 'index file')

    def test_container3subdir(self):
        resp = Request.blank(
            '/v1/a/c3/subdir/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('Listing of /v1/a/c3/subdir/' in resp.body)
        self.assertTrue('</style>' in resp.body)
        self.assertTrue('<link' not in resp.body)
        self.assertTrue('listing.css' not in resp.body)

    def test_container3subdirx(self):
        resp = Request.blank(
            '/v1/a/c3/subdirx/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 404)

    def test_container3subdiry(self):
        resp = Request.blank(
            '/v1/a/c3/subdiry/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 404)

    def test_container3subdirz(self):
        resp = Request.blank(
            '/v1/a/c3/subdirz').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 301)

    def test_container3unknown(self):
        resp = Request.blank(
            '/v1/a/c3/unknown').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue("Chrome's 404 fancy-page sucks." not in resp.body)

    def test_container3bindexhtml(self):
        resp = Request.blank('/v1/a/c3b/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.body, '')

    def test_container4indexhtml(self):
        resp = Request.blank('/v1/a/c4/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('Listing of /v1/a/c4/' in resp.body)
        self.assertTrue('href="listing.css"' in resp.body)

    def test_container4indexhtmlauthed(self):
        resp = Request.blank('/v1/a/c4').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 301)
        resp = Request.blank(
            '/v1/a/c4',
            environ={'REMOTE_USER': 'authed'}).get_response(
                self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        resp = Request.blank(
            '/v1/a/c4', headers={'x-web-mode': 't'},
            environ={'REMOTE_USER': 'authed'}).get_response(
                self.test_staticweb)
        self.assertEqual(resp.status_int, 301)

    def test_container4unknown(self):
        resp = Request.blank(
            '/v1/a/c4/unknown').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue("Chrome's 404 fancy-page sucks." in resp.body)

    def test_container4subdir(self):
        resp = Request.blank(
            '/v1/a/c4/subdir/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('Listing of /v1/a/c4/subdir/' in resp.body)
        self.assertTrue('</style>' not in resp.body)
        self.assertTrue('<link' in resp.body)
        self.assertTrue('href="../listing.css"' in resp.body)
        self.assertEqual(resp.headers['content-type'],
                         'text/html; charset=UTF-8')

    def test_container4onetxt(self):
        resp = Request.blank(
            '/v1/a/c4/one.txt').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)

    def test_container4twotxt(self):
        resp = Request.blank(
            '/v1/a/c4/two.txt').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 503)

    def test_container5indexhtml(self):
        resp = Request.blank('/v1/a/c5/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 503)

    def test_container5unknown(self):
        resp = Request.blank(
            '/v1/a/c5/unknown').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue("Chrome's 404 fancy-page sucks." not in resp.body)

    def test_container6subdir(self):
        resp = Request.blank(
            '/v1/a/c6/subdir').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 301)

    def test_container7listing(self):
        resp = Request.blank('/v1/a/c7/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue('Web Listing Disabled' in resp.body)

    def test_container8listingcss(self):
        resp = Request.blank(
            '/v1/a/c8/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('Listing of /v1/a/c8/' in resp.body)
        self.assertTrue('<link' in resp.body)
        self.assertTrue(
            'href="http://localhost/stylesheets/listing.css"' in resp.body)

    def test_container8subdirlistingcss(self):
        resp = Request.blank(
            '/v1/a/c8/subdir/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('Listing of /v1/a/c8/subdir/' in resp.body)
        self.assertTrue('<link' in resp.body)
        self.assertTrue(
            'href="http://localhost/stylesheets/listing.css"' in resp.body)

    def test_container9listingcss(self):
        resp = Request.blank(
            '/v1/a/c9/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('Listing of /v1/a/c9/' in resp.body)
        self.assertTrue('<link' in resp.body)
        self.assertTrue('href="/absolute/listing.css"' in resp.body)

    def test_container9subdirlistingcss(self):
        resp = Request.blank(
            '/v1/a/c9/subdir/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('Listing of /v1/a/c9/subdir/' in resp.body)
        self.assertTrue('<link' in resp.body)
        self.assertTrue('href="/absolute/listing.css"' in resp.body)

    def test_container10unicodesubdirlisting(self):
        resp = Request.blank(
            '/v1/a/c10/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('Listing of /v1/a/c10/' in resp.body)
        resp = Request.blank(
            '/v1/a/c10/\xe2\x98\x83/').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('Listing of /v1/a/c10/\xe2\x98\x83/' in resp.body)
        resp = Request.blank(
            '/v1/a/c10/\xe2\x98\x83/\xe2\x98\x83/'
        ).get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue(
            'Listing of /v1/a/c10/\xe2\x98\x83/\xe2\x98\x83/' in resp.body)

    def test_container11subdirmarkerobjectindex(self):
        resp = Request.blank('/v1/a/c11/subdir/').get_response(
            self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('<h2>c11 subdir index</h2>' in resp.body)

    def test_container11subdirmarkermatchdirtype(self):
        resp = Request.blank('/v1/a/c11a/subdir/').get_response(
            self.test_staticweb)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue('Index File Not Found' in resp.body)

    def test_container11subdirmarkeraltdirtype(self):
        resp = Request.blank('/v1/a/c11a/subdir2/').get_response(
            self.test_staticweb)
        self.assertEqual(resp.status_int, 200)

    def test_container11subdirmarkerinvaliddirtype(self):
        resp = Request.blank('/v1/a/c11a/subdir3/').get_response(
            self.test_staticweb)
        self.assertEqual(resp.status_int, 200)

    def test_container12unredirectedrequest(self):
        resp = Request.blank('/v1/a/c12/').get_response(
            self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('index file' in resp.body)

    def test_container_404_has_css(self):
        resp = Request.blank('/v1/a/c13/').get_response(
            self.test_staticweb)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue('listing.css' in resp.body)

    def test_container_404_has_no_css(self):
        resp = Request.blank('/v1/a/c7/').get_response(
            self.test_staticweb)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue('listing.css' not in resp.body)
        self.assertTrue('<style' in resp.body)

    def test_subrequest_once_if_possible(self):
        resp = Request.blank(
            '/v1/a/c4/one.txt').get_response(self.test_staticweb)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['x-object-meta-test'], 'value')
        self.assertEqual(resp.body, '1')
        self.assertEqual(self.app.calls, 1)


if __name__ == '__main__':
    unittest.main()
