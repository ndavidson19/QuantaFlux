FROM openjdk:11-jre-slim

WORKDIR /app

COPY target/stock-etl-1.0-SNAPSHOT.jar /app/stock-etl.jar

CMD ["java", "-jar", "stock-etl.jar"]