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

You should install the library from the wheels available under the 
release link.

First you need to determine the platform you are using. The tag should correspond to a download on the release page.

```python
from packaging.tags import sys_tags
for tag in sys_tags():
    print(str(tag))
```

Then you can install the library using the appropriate wheel file.

```bash
TAG=cp310-cp310-manylinux_2_34_x86_64
pip install https://github.com/TeangaNLP/teanga2/releases/download/dev-latest-linux/teanga-0.1.0-$TAG.whl
```

Then, in your Python code, you can use the Teanga core library 
by specifying the `db` property as follows:

```python
import teanga

corpus = teanga.Corpus(db="test.db")
```
