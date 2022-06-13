FROM jorgerojasdev/kafka-environment-mock:latest
COPY avro /usr/kem/src/main/resouces
WORKDIR /usr/kem/
RUN mvn clean install
COPY refs /usr/kem/src/main/resources
COPY '*.yml' /usr/kem/src/main/resources
ENTRYPOINT ["mvn", "spring-boot:run"]