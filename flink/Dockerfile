# ===== Stage 1: Build =====
# Maven + JDK 이미지에서 fat JAR 빌드
# 호스트에 Java/Maven 설치 불필요
FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /build
COPY pom.xml .

# 의존성 먼저 다운로드 (캐시 활용 - 코드 변경 시 재다운로드 방지)
RUN mvn dependency:go-offline -B

COPY src/ src/
# 2026-09-16: -DskipTests 제거. 늦은 이벤트 가드 테스트가 JAR 생성의 게이트가 되도록 (실행되지 않는 테스트는 게이트가 아니다)
RUN mvn clean package -B

# ===== Stage 2: Output =====
# 빌드된 JAR만 추출 (빌드 도구는 버림 → 최종 이미지 경량화)
FROM alpine:3.19 AS output
COPY --from=builder /build/target/flink-cdc-job-1.0.0.jar /output/flink-cdc-job-1.0.0.jar
