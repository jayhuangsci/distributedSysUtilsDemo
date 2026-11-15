# Portfolio and interview notes

## Project summary

Ratings Lab is a Java/Scala batch analytics project with equivalent Hadoop MapReduce and Spark implementations. It extends the repository's original unfinished movie-rating tasks with statistics, agreement-based item neighbors, reproducible data generation, and automated correctness checks.

The strongest evidence is executable: CI runs the original golden fixtures, a richer independent reference calculation, multiple partition counts, invalid input and resource-limit cases, the documented demos, and the Docker image. The README's commands are exercised rather than described as future work.

## Resume wording

Adapt these to reflect the parts you understand and can explain:

- Built a movie-rating analytics toolkit in Java Hadoop MapReduce and Scala Spark, implementing seven equivalent batch operations, distributed movie-pair joins, and deterministic top-K recommendations.
- Designed mergeable aggregation state, custom Hadoop Writable serialization, input-quality counters, and staged output publication; verified engine parity against golden fixtures and independent reference calculations across partition counts.
- Created seeded synthetic workloads, result-comparison utilities, reproducible experiment reports, a Linux container, and GitHub Actions verification for local distributed execution.

Do not add data-volume, throughput, speedup, production-use, or cluster-deployment claims without measured evidence from a real run. The current sample has three movies and thirteen ratings. Generator capacity is not a benchmark result. Local MapReduce and Spark execution exercise the real engine APIs but do not demonstrate multi-node fault tolerance.

## A short walkthrough

1. Show the README objective and the two pipeline diagrams.
2. Run the sample demo, open `report.csv`, and show a `MATCH` result.
3. Explain why a movie-pair join must shuffle by user before aggregating by canonical movie pair.
4. Open `StatsWritable` and explain why averages are computed after merging counts and sums.
5. Show `_RUN.json`, including input-quality and shuffle counters.
6. Demonstrate that an existing output path is rejected and that invalid rows fail before publication.
7. Open the parity test and explain its independent movie-by-movie oracle and partition-count checks.
8. Discuss the quadratic dense-user case and why the guard fails instead of silently truncating data.

## Questions to prepare for

| Question | Relevant implementation |
| --- | --- |
| Why can a combiner run zero or multiple times? | Associative count/statistics state with an identity; no correctness dependence on combiner execution. |
| Why not average partial averages? | Partial groups can have different counts; merge sums and counts instead. |
| What makes recommendations repeatable? | Canonical pair keys, exact integer support, deterministic title tie breaks, explicit rank. |
| How is input validation atomic at row level? | Parse the entire movie record before emitting any event. |
| How do Spark retries affect counters? | Counts come from actions on persisted validated input, not input accumulators. |
| Where is state bounded? | Top-K heaps and the per-user movie guard; pair output volume can still be quadratic. |
| What does the output transaction guarantee? | Complete staged computation followed by no-overwrite rename on the same local filesystem/HDFS. |
| What is not established by this project? | Production recommendation quality, multi-node scalability, HA behavior, object-store transactions. |

## Provenance

The repository began with unfinished `Task1`–`Task4` Java/Scala files, two word-count examples, and scripts tied to a particular teaching cluster. The original fixture and expected task outputs remain. The original Task 4 meaning was inferred from those samples and is now explicitly defined as equal-score agreement. Git history preserves the starting point; the current source implements named commands in a standard Maven layout.
