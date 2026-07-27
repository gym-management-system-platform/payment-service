# syntax=docker/dockerfile:1
# Build context must be the monorepo root (see docker-compose.yaml).
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /app

COPY gradlew .
COPY gradle gradle
COPY build.gradle ./
COPY kafka-starter kafka-starter
COPY payment-service payment-service

# Root settings.gradle also includes order/inventory — keep only modules we copy.
RUN printf '%s\n' \
    "rootProject.name = 'fitness-system-saga-orchestration'" \
    "include 'kafka-starter'" \
    "include 'payment-service'" \
    > settings.gradle

# mydev.logging lives in local ~/.m2 (mavenLocal), not on Maven Central.
# Compose passes host ~/.m2 as additional context "m2".
RUN --mount=type=bind,from=m2,source=.,target=/root/.m2,ro \
    chmod +x ./gradlew \
    && ./gradlew :payment-service:clean :payment-service:bootJar -x test --no-daemon \
    && cp payment-service/build/libs/$(ls payment-service/build/libs | grep -v plain | head -1) /app/app.jar

FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY --from=builder /app/app.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
