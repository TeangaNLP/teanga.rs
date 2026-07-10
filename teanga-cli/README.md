# Teanga Command Line Interface

This is the command line interface for the Teanga platform. It provides an 
executable that can manipulate data using the Teanga data model.

## Installation

To install the Teanga CLI, you can use [cargo](https://doc.rust-lang.org/cargo/):

```bash
cargo install --git https://github.com/teangaNLP/teanga.rs teanga-cli
```

## Usage

The Teanga CLI provides a number of subcommands that can be used to manipulate
data. The following is a list of the available subcommands:

```
Usage: teanga <COMMAND>

Commands:
  load     Load a file into the corpus
  convert  Convert a Teanga Corpus
  show     Show one or all documents in a corpus
  help     Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

### Convert Command

```
Convert a Teanga Corpus

Usage: teanga convert [OPTIONS] <INPUT> <OUTPUT>

Arguments:
  <INPUT>   The file to convert
  <OUTPUT>  The output file

Options:
  -i, --input-format <INPUT_FORMAT>
          The format of the input file [default: guess] [possible values: json, jsonl, yaml, tcf, guess]
  -o, --output-format <OUTPUT_FORMAT>
          The format of the output file [default: guess] [possible values: json, jsonl, yaml, tcf, guess]
  -m, --meta-file <META_FILE>
          The meta information, as a separate YAML file (required for JSONL)
      --compression <COMPRESSION>
          The string compression method (for TCF output only). It is best to use `smaz` for English corpora and `generate` for other languages [default: smaz] [possible values: smaz, shoco, generate]
      --compression-bytes <COMPRESSION_BYTES>
          The number of bytes to use for generate string compression (for TCF output only, only used if compression is set to generate) [default: 1000000]
  -h, --help
          Print help
```

### Load Command

```
Load a file into the corpus

Usage: teanga load [OPTIONS] <FILE> <DB>

Arguments:
  <FILE>  The file to load
  <DB>    The path to the DB

Options:
      --meta <META>  The meta information, as a separate YAML file
      --jsonl        Read the file as JSONL (one JSON object per line)
  -h, --help         Print help
```

### Show Command

```
Show one or all documents in a corpus

Usage: teanga show [OPTIONS] <PATH>

Arguments:
  <PATH>  Path to a corpus file, or to a DB directory (as used by `load`)

Options:
      --doc-id <DOC_ID>  Only show the document with this ID
  -f, --format <FORMAT>  The format of the input file (ignored if `path` is a DB directory) [default: guess] [possible values: json, jsonl, yaml, cuac, guess]
      --meta <META>      Meta information as a separate YAML file (required for JSONL input)
  -h, --help             Print help
```

`PATH` can be either a corpus file (format is guessed from the extension,
`.gz` is handled transparently) or a DB directory created by `teanga load`.
Each document is printed with its underlying text first, followed by each
group of annotation layers that share a common tokenization, rendered as an
aligned table with one row per layer (e.g. `pos` and `lemma`, both based on
`words`, line up under a shared `words` row). Output wraps to the current
terminal width (80 columns if it can't be detected, e.g. when redirected to
a file), regrouping the annotation rows together at each wrap point so a
line and its annotations always stay together:

```
$ teanga show corpus.yaml --doc-id Ewyz
=== Ewyz ===
text: The quick fox jumps

words: The quick fox  jumps
lemma: the quick fox  jump
pos  : det adj   noun verb
```
