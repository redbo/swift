Hummingbird
===========

Hummingbird is a golang implementation of some parts of `Openstack Swift <http://swift.openstack.org/>`_.  The idea is to keep the same protocols and on-disk layout, but `improve performance <http://saddlebird.com/>`_ dramatically.

Basic Setup Script
------------------

The following bash script will take a clean `Ubuntu Trusty <https://wiki.ubuntu.com/TrustyTahr/ReleaseNotes?_ga=1.100018781.23264233.1460677422>`_ installation to running a minimal configuration of Swift backed by the Hummingbird object server, similar to the Swift All-In-One setup.  It was developed against a clean install of the Ubuntu 14.04 server .iso image, but should be fairly agnostic to the specific server kick.

It should be ran as a user who has sudo capability, and will be set up to run Hummingbird and Swift as that user.

This installs swift in a virtualenv, which is cleaner, easier, and less fragile than trying to patch the system libraries.  But it does mean you'll have to activate the venv before invoking some commands.

.. code:: bash

    #!/bin/bash
    set -e
    
    # update and install some necessary system dependencies
    
    sudo apt-get update
    sudo apt-get -y upgrade
    sudo apt-get -y install build-essential memcached rsync xfsprogs git-core libffi-dev python-dev liberasurecode-dev python-virtualenv curl
    
    # create a 5GB loopback xfs filesystem - swift doesn't really work with ext4
    
    sudo mkdir /mnt/sdb1
    sudo truncate --size 5G /srv/swift-disk
    sudo mkfs.xfs /srv/swift-disk
    echo '/srv/swift-disk /mnt/sdb1 xfs loop 0 0' | sudo tee -a /etc/fstab
    sudo mount /mnt/sdb1
    
    # set up some directories we'll need
    
    sudo mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4 /var/run/swift /var/run/hummingbird /etc/hummingbird /etc/swift /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4
    sudo ln -s /mnt/sdb1/* /srv
    sudo chown -R "${USER}" /etc/swift /etc/hummingbird /srv/* /var/cache/swift* /var/run/swift /var/run/hummingbird /mnt/sdb1 /srv/*
    mkdir -p ~/bin ~/swift-venv
    
    # set up the path
    
    export PATH="$HOME/swift-venv/bin:$HOME/bin:$PATH"
    echo 'export PATH=$PATH:$HOME/swift-venv/bin:$HOME/bin' >> ~/.bashrc
    
    # fetch swift and swiftclient
    
    git clone 'https://github.com/openstack/swift.git' ~/swift
    git clone https://review.openstack.org/openstack/python-swiftclient.git ~/swiftclient
    
    # install swift and swiftclient in a new virtualenv
    
    virtualenv ~/swift-venv
    ~/swift-venv/bin/pip install -U pip setuptools
    ~/swift-venv/bin/pip install -r ~/swift/requirements.txt
    ~/swift-venv/bin/pip install -r ~/swift/test-requirements.txt
    ~/swift-venv/bin/pip install -r ~/swiftclient/requirements.txt
    cd ~/swift; ~/swift-venv/bin/python setup.py develop
    cd ~/swiftclient; ~/swift-venv/bin/python setup.py develop
    cp ~/swift/doc/saio/bin/* ~/bin
    
    # set up swift's config files
    
    cp -r ~/swift/doc/saio/swift/* /etc/swift
    cp ~/swift/test/sample.conf /etc/swift/test.conf
    find /etc/swift/ -name "*.conf" | xargs sed -i "s/<your-user-name>/${USER}/"
    printf "[swift-hash]\nswift_hash_path_prefix = changeme\nswift_hash_path_suffix = changeme\n" > /etc/swift/swift.conf
    
    # make some rings
    
    remakerings
    
    # set up go
    
    curl 'https://storage.googleapis.com/golang/go1.6.linux-amd64.tar.gz' | sudo tar -xz -C /usr/local
    sudo ln -s /usr/local/go/bin/* /usr/local/bin
    export GOPATH="$HOME/go"
    echo 'export GOPATH=$HOME/go' >> ~/.bashrc
    
    # build hummingbird
    
    mkdir -p ~/go/src/github.com/openstack
    ln -s ~/swift ~/go/src/github.com/openstack/swift
    cd ~/go/src/github.com/openstack/swift; git checkout origin/feature/hummingbird
    cd ~/go/src/github.com/openstack/swift/go; make get test bin/hummingbird
    ln -s ~/go/src/github.com/openstack/swift/go/bin/hummingbird ~/bin
    
    # finally, start up the servers
    
    swift-init start container
    swift-init start account
    swift-init start proxy
    hummingbird start object


To run the swift unit tests from this point,

.. code:: bash

    source ~/swift-venv/bin/activate
    ~/swift/.functests
    deactivate

Further Setup
-------------

The setup script creates a sparse 5GB xfs loopback filesystem to hold swift's data.  This is sufficient to run the functional tests, but you may want to allocate more space, move it to a separate physical device, or use pre-allocated disk space instead.

For logging to work, you will need to configure your syslog to listen for UDP packets.  You can uncomment these lines in /etc/rsyslog.conf:

.. code:: text

    # provides UDP syslog reception
    $ModLoad imudp
    $UDPServerRun 514

Then,

.. code:: bash

    sudo service rsyslog restart

