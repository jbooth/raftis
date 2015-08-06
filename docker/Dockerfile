FROM debian:jessie
RUN apt-get install -y liblmdb-dev
ADD genconfig /bin/genconfig
ADD raftis /bin/raftis
ADD runEtcd.sh /bin/runEtcd
EXPOSE 1103
EXPOSE 6379
CMD /bin/runEtcd http://raftis-dashboard:4001/
