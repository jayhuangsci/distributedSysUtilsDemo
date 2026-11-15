# Verified examples

`edge-movie-stats.csv` and `edge-recommendations.csv` are actual normalized results from the Linux [engine parity run](https://github.com/jayhuangsci/distributedSysUtilsDemo/actions/runs/43770436885) at commit `a638f46`.

Input: [`src/test/resources/ratings-edge-cases.csv`](../../src/test/resources/ratings-edge-cases.csv). Parameters: three shuffle partitions, `--top-k 2`, `--min-support 1`. Both Hadoop and Spark matched an independent reference calculation. The data includes commas and quotes in titles, Unicode, an unrated movie, and a rating in user column twelve.

CI compares these checked-in snapshots with newly generated parity results. They are correctness examples, not a scale benchmark. To reproduce them on Linux or macOS:

```bash
./mvnw verify
cat target/parity-evidence/movie-stats-p3.csv
cat target/parity-evidence/recommend-p3.csv
```
