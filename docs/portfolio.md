# Portfolio focus: Java/Hadoop training-data engineering

## Objective

Build and deploy a distributed batch system that converts raw text into usable JSONL examples for AI pretraining workflows. The project centers on Java MapReduce, HDFS storage, YARN task placement, exact deduplication, data quality, and reproducible output contracts.

## Resume bullets

- Built a **Java/Hadoop text-preparation pipeline with two MapReduce jobs**, implementing extraction, cleaning, pattern redaction, distributed deduplication, and JSONL training-chunk export.
- Deployed an **HDFS/YARN cluster with two worker containers**, validating job completion and task execution hosts through ResourceManager and JobHistoryServer APIs.
- Added deterministic document splits, bounded chunking, quality/shuffle counters, automated tests, Docker demos, and GitHub Actions verification.

Use the cluster execution bullet once the distributed workflow is green, and retain the container qualification. Do not describe it as a physical multi-machine deployment or quote generator sizes as measured throughput.

## Interview walkthrough

1. Show how a splittable input reader preserves whole-document boundaries across map tasks.
2. Explain why redaction precedes the content digest and how the global shuffle finds cross-file duplicates.
3. Open `PreparedDocumentWritable`; discuss associative merging, Hadoop's reused value objects, and deterministic provenance selection.
4. Explain why document-level hashing keeps chunks of an exact duplicate out of both training and validation.
5. Show chunk boundaries and why overlap does not create a redundant final chunk.
6. Open the manifest and distinguish rejected documents, duplicate inputs, unique documents, chunks and overlap-included tokens.
7. Show the two completed YARN jobs and successful task attempts on worker1 and worker2.
8. Discuss lexical versus model-specific tokenization, heuristic privacy coverage, small-file overhead and production resource sizing.

## Evidence

The local integration tests run real Hadoop jobs with one and three reducers, tiny input split sizes, gzip input/output, malformed UTF-8, oversized documents, empty input, and protected output destinations. The distributed CI job additionally uses HDFS and YARN containers and records execution-host evidence. The synthetic generator is seeded and deliberately places duplicate documents in different input shards.

The prior ratings/Spark implementation is preserved at the `ratings-lab-v1.0.0` tag. It is historical context, not the objective of the current Java/Hadoop project.
