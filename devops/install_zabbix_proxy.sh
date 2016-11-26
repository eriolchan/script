wget http://repo.zabbix.com/zabbix/3.2/ubuntu/pool/main/z/zabbix-release/zabbix-release_3.2-1+trusty_all.deb
dpkg -i zabbix-release_3.2-1+trusty_all.deb
apt-get update
apt-get install zabbix-proxy-sqlite3 -f
