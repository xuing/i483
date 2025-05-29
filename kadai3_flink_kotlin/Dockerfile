FROM maven:3-eclipse-temurin-21 as builder
WORKDIR /build
COPY . .
RUN mvn clean package -DskipTests && \
    rename -v target/*.jar app.jar
