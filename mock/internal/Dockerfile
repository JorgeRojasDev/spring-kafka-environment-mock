FROM jorgerojasdev/kafka-environment-mock:latest
ADD environment-mock.yml /usr/kem/src/main/resources/application-embedded.yml
ADD avro /usr/kem/src/main/resouces/avro
WORKDIR /usr/kem/
RUN mvn clean install
ENTRYPOINT ["mvn", "spring-boot:run"]