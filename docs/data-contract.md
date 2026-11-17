# Training-data contract

## Input documents

- UTF-8 plain text or HTML. There is no CSV/JSON input parser, PDF extractor, or Office-document extractor.
- `--input-mode lines`: one physical line is one document. Multiline source content should be serialized as one line or provided using file mode. Uncompressed files are split across mappers; compressed files are not split.
- `--input-mode files`: one file is one document, with embedded newlines allowed. Large files are rejected by the configured document limit rather than cut into partial documents.
- Input paths can be local paths, HDFS URIs, directories, or Hadoop globs. Directory traversal is recursive. Hadoop's usual hidden-file rules ignore names starting with `_` or `.`.
- Hive TEXTFILE storage may be read directly from its HDFS location if its lines match this contract. Binary Hive storage formats must be exported to compatible text first. The application does not query the metastore.

## Cleaning and rejection

HTML extraction removes scripts, styles, navigation, headers/footers, forms and selected non-content nodes. Auto mode recognizes common HTML tags; force `--content-type text` to preserve text that merely resembles HTML. Text is NFKC-normalized; selected invisible/control characters are removed and whitespace is collapsed. Case is preserved.

Rejections have mutually exclusive document counters: oversized bytes, invalid UTF-8, empty content, too short, low letter ratio, or too many tokens. Minimum-length rules apply to prepared content. Letter ratio divides meaningful letters by visible characters; inserted redaction tags do not contribute letters. These are explicit heuristics, not a learned quality score or a factuality assessment.

Common pattern classes are email addresses, US-style phone numbers, IPv4 addresses, Luhn-valid card-like numbers, and HTTP/HTTPS/www URLs. Replacements are `[EMAIL]`, `[PHONE]`, `[IP]`, `[CARD]`, and `[URL]`. URLs are removed in full, including query strings. Coverage is limited: personal names, addresses, other phone formats, contextual secrets, and arbitrary identifiers are not comprehensively detected. Hashing source paths does not itself establish anonymization.

The `redacted_*` counters count matches in accepted input documents before deduplication. A record's `document_redactions` describes the selected representative whole document, not only that chunk. It is not a count of sensitive matches remaining in output.

## Identity, deduplication and splitting

`document_id = SHA256(prepared_text)`, after extraction, normalization and redaction. This performs exact normalized-text deduplication and can collapse documents whose only differences were masked values. It does not find semantically equivalent text or arbitrary near-duplicates.

`source_occurrences` is the number of accepted copies merged into a document. One physical record is exported per chunk, regardless of occurrence count. `source.id` hashes the qualified path; the representative is chosen deterministically by source hash and offset. Changing source locations can change provenance without changing text/document/chunk identity.

The validation bucket is `unsigned(first_64_digest_bits) mod 100 < validation_percent`. All chunks from that document receive the same split. Small datasets may have an empty split; the ratio is approximate, not stratified or exactly balanced.

Chunk identity includes the document ID, chunk number, schema version and lexical-tokenizer/window fingerprint. Changing reducer count or compression leaves chunk IDs unchanged. Changing chunk size/overlap changes them. Token offsets are zero-based and end-exclusive.

## JSONL and downstream use

Every `.jsonl` line is one independent JSON object; `.jsonl.gz` uses gzip compression. See [training-record.schema.json](training-record.schema.json) for the full record structure. `token_count` equals the token array length and `token_end - token_start`. The token count in the manifest includes repeated overlap tokens.

The lexical tokenizer keeps redaction tags intact, handles Unicode letters and punctuation, and emits individual Han characters. These tokens are useful for chunking and inspection. They are not token IDs from a pretrained model, and a lexical-token limit does not guarantee a model-specific context length. For causal language-model pretraining, load each record's `text` and apply the intended model's tokenizer and packing rules downstream.

No instruction/response labels, model weights, or training evaluation results are generated. JSONL is the implemented export format; the reference diagram's Parquet/TFRecord/WebDataset alternatives are not advertised as existing exporters.

The generator uses synthetic, seeded text. For your own corpora, dataset licensing, consent, access control and evaluation remain properties of the source and deployment, not guarantees supplied by these transformation rules.
