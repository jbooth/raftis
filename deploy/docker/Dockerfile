FROM debian:jessie
RUN apt-get update && apt-get install -y liblmdb-dev
ADD genconfig /bin/genconfig
ADD raftis /bin/raftis
ADD runEtcd.sh /bin/runEtcd
RUN chmod +x /bin/genconfig
RUN chmod +x /bin/raftis
RUN chmod +x /bin/runEtcd
VOLUME [ "/var/raftis" ]
EXPOSE 1103
EXPOSE 4001
CMD /bin/runEtcd 
