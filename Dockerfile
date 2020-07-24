FROM openjdk:8
WORKDIR /
ADD ReadKafkaWriteMongo-1.0-SNAPSHOT.jar ReadKafkaWriteMongo-1.0-SNAPSHOT.jar
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
