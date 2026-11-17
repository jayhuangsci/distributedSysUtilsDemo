# Distributed deployment and operations

## Reference cluster

`deploy/compose.yml` defines a NameNode, ResourceManager, JobHistoryServer, two workers, and an on-demand client. Each worker runs a DataNode and NodeManager, colocating storage with task execution. Workers register 1536 MiB and two virtual cores each. The application master and each map/reduce container request 512 MiB with a 256 MiB JVM heap. These are small-demo settings; size your real jobs from measured document and task memory use.

The build downloads Apache Hadoop 3.4.2's lean distribution from the Apache CDN, with an archive fallback, and verifies the published SHA-512 checksum. The lean distribution omits the unused AWS SDK bundle. The application JAR includes relocated Jackson/jsoup dependencies, leaving Hadoop to the cluster classpath. No Spark or Scala runtime is required.

```bash
docker compose -f deploy/compose.yml build namenode
docker compose -f deploy/compose.yml up --detach --no-build --wait --wait-timeout 180
docker compose -f deploy/compose.yml run --rm client /opt/demo/cluster-demo.sh
docker compose -f deploy/compose.yml run --rm client hdfs dfs -ls /corpus/output
```

The script waits for two live DataNodes and two registered YARN workers, generates synthetic line shards, uploads them to HDFS, runs the application, downloads results, inspects JSONL invariants, and audits task execution hosts through history APIs. The HDFS input uses replication factor two. `hdfs-blocks.txt`, `yarn-nodes.txt`, the manifest, inspection report and deployment audit form the evidence bundle in CI.

All services share one image. Building the `namenode` target once and starting with `--no-build` avoids exporting the same image separately for every role.

To retain the client-side evidence on your own machine, use a named client container:

```bash
docker compose -f deploy/compose.yml run --name corpus-demo-client client /opt/demo/cluster-demo.sh
docker cp corpus-demo-client:/app/evidence/. ./cluster-evidence
docker rm corpus-demo-client
```

The client container must have a fresh name if a previous one is still present. HDFS output paths include a run timestamp and are never overwritten. Service data persists in named Docker volumes across `docker compose down`. Removing those volumes discards the demo cluster's data.

The demo exposes only NameNode, ResourceManager and history web interfaces on localhost. It does not configure Kerberos, TLS or a production service topology. Container workers on one host are a reproducible distributed execution environment, not a physical multi-host benchmark.

## Existing HDFS/YARN installation

Use a configured Hadoop 3.4.x / Java 11 client environment. Build the JAR with JDK 11 and ensure `HADOOP_CONF_DIR` points to the cluster's core, HDFS, YARN and MapReduce configuration files. The submitting identity needs input read permission and permission to create the output parent directory.

```bash
./mvnw --batch-mode -DskipTests package
export HADOOP_CONF_DIR=/etc/hadoop/conf
./deploy/submit-yarn.sh hdfs:///datasets/raw-text hdfs:///datasets/corpus/run-001 \
  --reducers 8 --chunk-tokens 256 --chunk-overlap 32 --compression gzip
```

The equivalent direct submission uses the main class from the JAR manifest:

```bash
hadoop jar target/hadoop-corpus-pipeline-2.0.0.jar \
  -Dmapreduce.framework.name=yarn \
  -Dmapreduce.map.output.compress=true \
  -Dmapreduce.map.memory.mb=2048 \
  -Dmapreduce.map.java.opts=-Xmx1536m \
  --input hdfs:///datasets/raw-text --output hdfs:///datasets/corpus/run-002 \
  --reducers 8 --compression gzip
```

`CORPUS_JAR` can override the application path used by the submission script. Authentication and scheduling policies are supplied by the existing Hadoop environment. The demo audit utility uses unauthenticated REST endpoints; adapt auditing to secured cluster access rather than exposing those APIs publicly.

## Where parallelism comes from

1. Large uncompressed line files split into independent map inputs. Many input shards also create independent tasks.
2. Reducers partition prepared documents by SHA-256 key and collapse duplicates across the whole input.
3. The map-only export job processes the intermediate SequenceFiles and writes separate task shards.
4. YARN allocates containers on available NodeManagers; HDFS replicas make inputs accessible from either worker.

Use moderately sized line shards instead of thousands of tiny files. Whole-file mode is appropriate for bounded documents, not enormous individual files. Compressed inputs are one mapper per file; use multiple compressed shards for parallel execution. More reducers can spread work and create more intermediates, but add scheduling and shuffle overhead. This repository makes no throughput or speedup claim without a measured run.

## Inspecting results and metrics

```bash
hdfs dfs -ls hdfs:///datasets/corpus/run-001/train
hdfs dfs -cat hdfs:///datasets/corpus/run-001/_MANIFEST.json
hdfs dfs -get hdfs:///datasets/corpus/run-001 ./downloaded-corpus
./scripts/run.sh inspect ./downloaded-corpus
```

The local inspector understands `.jsonl` and `.jsonl.gz`, checks duplicate chunk IDs and split isolation, and compares counts with the manifest. It stores IDs in memory, so use it for demos or suitably bounded datasets. The preparation pipeline itself does not retain the corpus on the client; Hadoop counters and cluster logs are the scalable operational view.

Manifest counters distinguish raw document count, rejection reasons, accepted inputs, unique documents, duplicates removed, redaction matches, split document/chunk counts, and tokens written including overlap. Each job also reports shuffle/combiner counters, filesystem bytes, job ID and elapsed milliseconds. Failed tasks can be retried by Hadoop; only successful committed job output is published.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Maven archive checksum failure in a slim Linux environment | Install `unzip`; the pinned checksum is for the ZIP distribution. Both Docker builds install it. |
| Native Windows `winutils` / `NativeIO` errors | Use Linux containers or WSL2 for Hadoop filesystem operations. |
| Jobs stay ACCEPTED | Check available YARN memory, AM limits, virtual cores, queue policy and worker registration. |
| ApplicationMaster class not found | Check `HADOOP_MAPRED_HOME`, the MapReduce application classpath and propagated environment. |
| HDFS safe mode blocks writes | Wait for the two DataNodes and inspect NameNode health. |
| Input is read but no records survive | Inspect rejection counters and input mode; choose thresholds suitable for the corpus. |
| Duplicate output destination | Choose a fresh path; no overwrite flag is provided. |
| JSONL has an empty validation directory | The dataset is small or validation percentage is zero; hash splitting is approximate. |
| History audit reports one execution host | Confirm both workers registered and submit enough independent input shards to exercise both. |
| A killed client left a hidden work directory | Confirm that the owning job is no longer active before manually removing its UUID workspace. |

CI uploads service and worker logs even on failures and tears down its owned cluster volumes after collecting evidence.
