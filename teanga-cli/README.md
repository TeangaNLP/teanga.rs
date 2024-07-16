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
