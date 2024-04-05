import teanga_pyo3.teanga # if this fails the Rust code is not installed
import teanga

def test_create_corpus():
    corpus = teanga.Corpus(db="tmp.db", new=True)
    corpus.add_layer_meta("text")
    print(corpus.meta["text"].base)
    _doc = corpus.add_doc("This is a document.")


