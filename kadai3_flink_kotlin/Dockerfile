FROM maven:3-eclipse-temurin-21
WORKDIR /build
COPY . .
RUN mvn clean package -DskipTests
