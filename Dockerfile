FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY common common
COPY order-service order-service
COPY inventory-service inventory-service
ARG SERVICE
RUN mvn -B -pl ${SERVICE} -am package -DskipTests
RUN cp ${SERVICE}/target/${SERVICE}-1.0.0.jar /app.jar
FROM eclipse-temurin:21-jre-jammy
RUN useradd --system --uid 10001 app
WORKDIR /app
COPY --from=build /app.jar app.jar
USER 10001
ENTRYPOINT ["java","-XX:MaxRAMPercentage=70","-jar","app.jar"]
