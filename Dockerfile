FROM maven:3.8.5-jdk-11
COPY src /usr/kem/src
COPY pom.xml /usr/kem
WORKDIR /usr/kem
RUN mvn package