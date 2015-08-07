FROM debian:jessie
ADD redis-cli /bin/redis-cli
ADD testRedis.sh /bin/testRedis
ADD redisCommands.txt /redisCommands.txt
RUN chmod +r /redisCommands.txt
RUN chmod +x /bin/redis-cli
RUN chmod +x /bin/testRedis
CMD /bin/testRedis 
