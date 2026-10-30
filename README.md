Teanga Core Library (Rust)
==========================

This is the core library for the Teanga project. It is written in Rust and provides the core functionality for the Teanga project.
Teanga is a library for representing NLP annotations and pipelines. 
For more details of the project see the [Teanga project page](https://teanga.io/).

## Usage (Rust)

To use the Teanga core library in your Rust project, add the following to your `Cargo.toml` file:

```toml
[dependencies]
teanga = "0.1.0"
```

Then, in your Rust code, you can use the Teanga core library as follows:

```rust
use teanga::Corpus;

let corpus = Corpus::new();
corpus.add_doc(("text", "This is a test."));
```

## Usage (Python)

This can also be integrated with the core Python library and provides
persistence and fast access to the data structures in the Rust library.

Install it together with the core library from PyPI:

```bash
pip install teanga[pyo3]
```

Prebuilt wheels are available for Linux (x86_64 and aarch64, glibc and
musl), macOS (Intel and Apple Silicon) and Windows (x64), for CPython 3.10
and later. On other platforms pip builds from source, which requires a
[Rust toolchain](https://rustup.rs/).

Then, in your Python code, you can use the Teanga core library 
by specifying the `db` property as follows:

```python
import teanga

corpus = teanga.Corpus(db="test.db")
```
