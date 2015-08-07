FROM debian:jessie
RUN apt-get update && apt-get install -y redis-tools
ADD testRedis.sh /bin/testRedis
ADD redisCommands.txt /redisCommands.txt
RUN chmod +r /redisCommands.txt
RUN chmod +x /bin/testRedis
CMD /bin/testRedis 
