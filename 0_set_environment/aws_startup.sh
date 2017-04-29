#!/bin/bash
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6
echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.4 multiverse"| sudo tee /etc/apt/sources.list.d/mongodb-org-3.4.list
sudo apt update
sudo apt install -y mongodb-org
sudo service mongod start
sudo apt install -y python virtualenv unzip unbound htop ipython iotop nload vim tmux git
virtualenv ~/mypy2
sudo wget ftp://FTP.INTERNIC.NET/domain/named.cache -O /etc/unbound/root.hints
git clone https://github.com/rthalley/dnspython.git