# Running and troubleshooting

## Local prerequisites

Use a JDK 11 installation, not only a JRE, for building. `JAVA_HOME` should point to the JDK directory. The wrapper pins Maven 3.9.11 and its ZIP checksum. Install `unzip` on Linux/macOS so the Unix wrapper uses that pinned archive; without it the upstream wrapper switches archive formats. The Docker build installs `unzip`, and the Windows wrapper uses PowerShell extraction. Internet access is needed for the first Maven build and Docker image build. The local launchers reserve up to 2 GB of JVM heap.

```bash
java -version
./mvnw -version
./mvnw --batch-mode verify
```

Hadoop 3.4.2 supports Java 11 at runtime, and Spark 3.5.7 supports Java 11. We compile this application against Java 11; this does not rebuild Hadoop itself. Spark's Scala artifact and our compiler both use the Scala 2.12 line. The pinned versions are a tested compatibility baseline, not a claim that they are the newest upstream releases.

## Inspect results

```bash
cat demo-output/sample/1-hadoop-movie-stats/part-*
cat demo-output/sample/1-hadoop-agreement/_RUN.json
cat demo-output/sample/report.csv
```

Read only `part-*` files as data. Files beginning with `_` or `.` are metadata, success markers, or local filesystem checksums. The built-in comparator reads plain local files/directories and preserves duplicates. It is not an HDFS client or a compressed-output reader. For HDFS results, first use `hdfs dfs -get` to copy the result directory to a new local destination.

Hadoop metrics have stage prefixes: `analytics.*`, `join.*`, `agreement.*`, or `recommend.*`. Input-quality and rating-event counts belong to the first stage reading original CSV; downstream stages read intermediate records and have zero CSV input-quality counts. Shuffle and combiner metrics are the framework's task counters, not a cross-engine performance score. Spark records logical input counts, version, master, parameters, and total elapsed time. Spark's UI and event-log options are supplied by the installed Spark runtime when using `spark-submit`.

## HDFS/YARN deployment

The cluster must already provide HDFS, YARN, Spark, networking, permissions, and Java. Set `HADOOP_CONF_DIR` to the cluster configuration directory and ensure the submitting user can read the input and create the output's parent directory. Use fresh output paths for every run.

The application JAR contains application classes and resources. Hadoop and Spark libraries are excluded, so cluster versions determine the runtime. The tested local dependency combination is Hadoop 3.4.2, Spark 3.5.7, Scala 2.12.18, and Java 11. Adjusting cluster/library versions requires running the parity tests again. A cluster integration test is still needed before claiming deployment on a particular cluster.

Standard Hadoop options can configure jobs without source edits:

```bash
hadoop jar target/ratings-lab-1.0.0.jar \
  io.github.jayhuangsci.ratings.hadoop.HadoopAnalytics \
  -Dmapreduce.framework.name=yarn \
  -Dmapreduce.map.output.compress=true \
  -Dmapreduce.reduce.memory.mb=2048 \
  --task agreement --input hdfs:///user/$USER/ratings-lab/input \
  --output hdfs:///user/$USER/ratings-lab/agreement-run-2 \
  --partitions 4 --min-support 2
```

Do not enable final output compression if you intend to use the plain-text comparator directly. Job-level compression is an operational configuration, not a separate application feature.

## Common issues

| Symptom | Action |
| --- | --- |
| `JAVA_HOME` or `java` missing | Install JDK 11 and point `JAVA_HOME` to its root. Start a fresh terminal if needed. |
| Native Windows `winutils.exe` / `NativeIO` errors | Use the Linux container or WSL2. The repository does not download third-party native Hadoop binaries. |
| Output already exists | Choose a new directory; results are deliberately not deleted or replaced. |
| Invalid row / score outside 1–5 | Correct the headerless input, or use `--bad-records skip` and inspect the rejected-row counter. |
| User exceeds movie limit | Inspect dense-user skew and potential pair volume before raising the explicit limit. |
| Duplicate movie title during join | Provide one row per movie with a unique title. There is no global deduplication policy. |
| Class not found when using `java -jar` | Use `scripts/run.sh`, `hadoop jar ... <class>`, or `spark-submit --class ...`; this is a cluster application JAR. |
| Results differ in file order | Use `compare`; partition filenames and record ordering are not the data contract. |
| Source changes do not appear | Rebuild with `./mvnw verify` before launching existing artifacts. |
| Spark cannot bind a local address | The launchers set `SPARK_LOCAL_IP=127.0.0.1`; check hostname and local firewall settings. |

## Failure cleanup

Validation failures and failed jobs remove their private workspace and do not publish the requested output. An existing destination is never deliberately overwritten. A hard process kill can leave a hidden sibling named `.<output>-work-<UUID>`; inspect ownership and confirm that no job is using it before manually removing an orphan.

The demo's outer directory and a partial `report.csv` may remain after a failed experiment. Individual completed engine outputs remain available for diagnosis, and the process exits unsuccessfully. Start the next experiment under a new root.
