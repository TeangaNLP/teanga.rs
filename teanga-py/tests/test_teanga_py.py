import teanga_pyo3.teanga as teangadb# if this fails the Rust code is not installed
from teanga import Corpus, Document
import os
import shutil

def test_teangadb_installed():
    if os.path.exists("tmp.db"):
        shutil.rmtree("tmp.db")
    teangadb.Corpus("tmp.db")

def test_create_corpus():
    corpus = Corpus(db="tmp.db", new=True)
    corpus.add_layer_meta("text")
    print(corpus.meta["text"].base)
    _doc = corpus.add_doc("This is a document.")


def test_add_doc():
    corpus = Corpus(db="tmp.db", new=True)
    corpus.add_layer_meta("text")
    _doc = corpus.add_doc("This is a document.")

    corpus = Corpus(db="tmp.db", new=True)
    corpus.add_layer_meta("en", layer_type="characters")
    corpus.add_layer_meta("nl", layer_type="characters")
    _doc = corpus.add_doc(en="This is a document.", nl="Dit is een document.")

def test_doc_ids():
    corpus = Corpus(db="tmp.db", new=True)
    corpus.add_layer_meta("text")
    _doc = corpus.add_doc("This is a document.")
    assert corpus.doc_ids == ['Kjco']
 
def test_docs():
    corpus = Corpus(db="tmp.db", new=True)
    corpus.add_layer_meta("text")
    _doc = corpus.add_doc("This is a document.")
    assert (str(corpus.docs) == "[('Kjco', Document('Kjco', " +
    "{'text': CharacterLayer('This is a document.')}))]")
 
def test_doc_by_id():
    corpus = Corpus(db="tmp.db", new=True)
    corpus.add_layer_meta("text")
    _doc = corpus.add_doc("This is a document.")
    assert (str(corpus.doc_by_id("Kjco")) == 
    "Document('Kjco', {'text': CharacterLayer('This is a document.')})")

def test_meta():
    corpus = Corpus(db="tmp.db", new=True)
    corpus.add_layer_meta("text")
    assert (str(corpus.meta) ==
        "{'text': LayerDesc(layer_type='characters', base=None, data=None, " +
            "link_types=None, target=None, default=None, meta={})}")
 
def test_to_yaml_str():
    corpus = Corpus(db="tmp.db", new=True)
    corpus.add_layer_meta("text")
    _doc = corpus.add_doc("This is a document.")
    assert (corpus.to_yaml_str() ==
        '_meta:\n    text:\n        type: characters\n\
Kjco:\n    text: This is a document.\n')
