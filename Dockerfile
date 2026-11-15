FROM eclipse-temurin:11-jdk-jammy AS build
WORKDIR /app
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
COPY sample_output/ sample_output/
RUN chmod +x scripts/run.sh
RUN printf '/app/lib/*' > target/classpath.txt
ENV SPARK_LOCAL_IP=127.0.0.1
ENTRYPOINT ["./scripts/run.sh"]
CMD ["demo", "sample_input/smalldata.txt", "/results/sample", "2", "1"]
