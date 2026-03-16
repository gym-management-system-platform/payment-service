# Используем более простой подход
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /app

# Копируем все исходники
COPY . .

# Собираем напрямую
RUN ./gradlew clean bootJar -x test --no-daemon

# Этап запуска
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY --from=builder /app/build/libs/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]