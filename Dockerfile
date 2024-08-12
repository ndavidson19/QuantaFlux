# Build stage
FROM gradle:7.2.0-jdk11 AS build
WORKDIR /app
COPY . /app
RUN gradle build --no-daemon

# Run stage
FROM openjdk:11-jre-slim
WORKDIR /app
COPY --from=build /app/build/libs/stock-etl-1.0-SNAPSHOT.jar /app/stock-etl.jar
CMD ["java", "-jar", "stock-etl.jar"]