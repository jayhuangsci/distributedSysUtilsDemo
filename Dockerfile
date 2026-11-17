FROM eclipse-temurin:11-jdk-jammy AS build
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends unzip && rm -rf /var/lib/apt/lists/*
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw
RUN ./mvnw -B -q dependency:go-offline
COPY src/ src/
RUN ./mvnw -B -q -DskipTests package dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib

FROM eclipse-temurin:11-jre-jammy
WORKDIR /app
COPY --from=build /app/target/classes/ target/classes/
COPY --from=build /app/target/lib/ lib/
COPY scripts/run.sh scripts/run.sh
COPY sample_input/ sample_input/
RUN chmod +x scripts/run.sh
RUN printf '/app/lib/*' > target/classpath.txt
ENTRYPOINT ["./scripts/run.sh"]
CMD ["demo", "sample_input/raw-documents.txt", "/results/sample"]
