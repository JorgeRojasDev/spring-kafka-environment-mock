FROM maven:3.8.5-jdk-11
COPY plugin /usr/kem/plugin
WORKDIR /usr/kem/plugin
RUN mvn install
COPY src /usr/kem/src
COPY pom.xml /usr/kem
WORKDIR /usr/kem
RUN mvn package