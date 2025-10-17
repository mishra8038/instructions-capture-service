#build the application jar file using maven.
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -e -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

#build the docker container for deploying the above made jar file.
FROM eclipse-temurin:21-jdk-alpine
WORKDIR /app
COPY --from=build /app/target/instructions-capture-service-0.1.0.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/app.jar"]