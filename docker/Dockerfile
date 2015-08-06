FROM debian:jessie
RUN apt-get update && apt-get install -y liblmdb-dev
ADD genconfig /bin/genconfig
ADD raftis /bin/raftis
ADD runEtcd.sh /bin/runEtcd
VOLUME [ "/var/raftis" ]
EXPOSE 1103
EXPOSE 6379
CMD /bin/runEtcd http://raftis-dashboard:4001/
