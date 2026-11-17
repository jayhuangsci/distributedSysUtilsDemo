# Actual sample output

[`training-record.json`](training-record.json) is one complete record from the [local Hadoop verification run](https://github.com/jayhuangsci/distributedSysUtilsDemo/actions/runs/44188064646), commit `b0b2c1e`. It demonstrates phone/IP redaction, lexical tokens, document identity, token offsets, and hashed source provenance.

Input: [`sample_input/raw-documents.txt`](../../sample_input/raw-documents.txt). Recipe: `scripts/run.sh demo`, with 32-token chunks, overlap 8, two reducers, and 50% validation buckets.

| Measured quantity | Result |
| --- | ---: |
| Input documents | 12 |
| Accepted input documents | 9 |
| Rejected documents | 3 |
| Unique prepared documents | 7 |
| Duplicate copies removed | 2 |
| Train / validation documents | 4 / 3 |
| Train / validation chunks | 7 / 6 |
| Exported tokens, including overlap | 327 |

[`inspection.json`](inspection.json) is the inspector's report from the same run. These small-fixture counts demonstrate correctness, not throughput or physical multi-host scalability. Source IDs can change with the qualified input path; document and chunk IDs remain stable for the same prepared text and chunking parameters.
