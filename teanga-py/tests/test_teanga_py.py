import teanga_pyo3.teanga as teangadb# if this fails the Rust code is not installed
from teanga import Corpus, read_json_str, read_yaml_str, read_cuac
import os
import shutil
import sys
import tempfile
from collections import Counter

def test_teangadb_installed():
    if os.path.exists("tmp.db"):
        shutil.rmtree("tmp.db")
    teangadb.Corpus("tmp.db")

def test_create_corpus():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    print(corpus.meta["text"].base)
    _doc = corpus.add_doc("This is a document.")


def test_add_doc():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    _doc = corpus.add_doc("This is a document.")

    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("en", layer_type="characters")
    corpus.add_layer_meta("nl", layer_type="characters")
    _doc = corpus.add_doc(en="This is a document.", nl="Dit is een document.")

def test_doc_ids():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    _doc = corpus.add_doc("This is a document.")
    assert corpus.doc_ids == ['Kjco']
 
def test_docs():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    _doc = corpus.add_doc("This is a document.")
    assert (str(list(corpus.docs)) == "[Document('Kjco', " +
    "{'text': 'This is a document.'})]")
 
def test_doc_by_id():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    _doc = corpus.add_doc("This is a document.")
    assert (str(corpus.doc_by_id("Kjco")) == 
    "Document('Kjco', {'text': 'This is a document.'})")

def test_meta():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    assert (str(corpus.meta) ==
        "{'text': LayerDesc(layer_type='characters', base=None, data=None, " +
            "link_types=None, target=None, default=None, meta={})}")
 
def test_to_yaml_str():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    _doc = corpus.add_doc("This is a document.")
    assert (corpus.to_yaml_str() ==
        '_meta:\n    text:\n        type: characters\n\
Kjco:\n    text: This is a document.\n')

def test_to_json_str():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    _doc = corpus.add_doc("This is a document.")
    assert (corpus.to_json_str() ==
        '{"_meta":{"text":{"type":"characters"}},\
"Kjco":{"text":"This is a document."}}')

def test_read_json_str():
    read_json_str('{"_meta": {"text": {"type": \
"characters"}},"Kjco": {"text": "This is a document."}}', "<memory>")

def test_read_yaml_str():
    read_yaml_str("_meta:\n  text:\n    type: characters\n\
Kjco:\n   text: This is a document.\n", "<memory>")
 
def test_document_setitem():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    corpus.add_layer_meta("pos", layer_type="seq", base="words", data="string")
    doc = corpus.add_doc("This is a document.")
    doc["words"] = [(0,4), (5,7), (8,9), (10,18), (18,19)]
    doc["pos"] = ["DT", "VBZ", "DT", "NN", "."]
    assert (str(doc) ==
        "Document('Kjco', {'text': 'This is a document.', \
'words': SpanLayer([[0, 4], [5, 7], [8, 9], [10, 18], [18, 19]]), \
'pos': SeqLayer(['DT', 'VBZ', 'DT', 'NN', '.'])})")
    assert (str(corpus.doc_by_id("Kjco")) ==
        "Document('Kjco', {'text': 'This is a document.', \
'words': SpanLayer([[0, 4], [5, 7], [8, 9], [10, 18], [18, 19]]), \
'pos': SeqLayer(['DT', 'VBZ', 'DT', 'NN', '.'])})")

def test_add_layers():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    corpus.add_layer_meta("pos", layer_type="seq", base="words", data="string")
    doc = corpus.add_doc("This is a document.")
    doc.add_layers({"words": [(0,4), (5,7), (8,9), (10,18), (18,19)], \
            "pos": ["DT", "VBZ", "DT", "NN", "."]})
 
def test_text_for_layer():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    corpus.add_layer_meta("pos", layer_type="seq", base="words", data="string")
    doc = corpus.add_doc("This is a document.")
    doc.words = [[0,4], [5,7], [8,9], [10,18], [18,19]]
    doc.pos = ["DT", "VBZ", "DT", "NN", "."]
    list(doc.text_for_layer("text"))

def test_char_layers():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    doc = corpus.add_doc("This")
    assert (doc.text.data == [None, None, None, None])
    assert (doc.text.text == ['This'])
    assert (doc.text.indexes("text") == [(0, 1), (1, 2), (2, 3), (3, 4)])

def test_seq_layers():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    corpus.add_layer_meta("pos", layer_type="seq", base="words", data="string")
    doc = corpus.add_doc("This is a document.")
    doc.words = [[0,4], [5,7], [8,9], [10,18], [18,19]]
    doc.pos = ["DT", "VBZ", "DT", "NN", "."]
    assert (doc.pos.data == ['DT', 'VBZ', 'DT', 'NN', '.'])
    assert (doc.pos.text == ['This', 'is', 'a', 'document', '.'])
    assert (doc.pos.indexes("pos") == [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)])
    assert (doc.pos.indexes("text") == [(0, 4), (5, 7), (8, 9), (10, 18), (18, 19)])

def test_span_layer():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    doc = corpus.add_doc("This is a document.")
    doc.words = [[0,4], [5,7], [8,9], [10,18], [18,19]]
    assert (doc.words.data == [None, None, None, None, None])
    assert (doc.words.text == ['This', 'is', 'a', 'document', '.'])
    assert (doc.words.indexes("words") == [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)])
    assert (doc.words.indexes("text") == [(0, 4), (5, 7), (8, 9), (10, 18), (18, 19)])

def test_elem_layer():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    corpus.add_layer_meta("is_noun", layer_type="element", base="words", data="string")
    doc = corpus.add_doc("This is a document.")
    doc.words = [[0,4], [5,7], [8,9], [10,18], [18,19]]
    doc.is_noun = [[3, "Yes"]]
    assert (doc.is_noun.data == ["Yes"])
    assert (doc.is_noun.text == ['document'])
    assert (doc.is_noun.indexes("is_noun") == [(0, 1)])
    assert (doc.is_noun.indexes("words") == [(3, 4)])
    assert (doc.is_noun.indexes("text") == [(10, 18)])

def test_update_docs():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")

    doc = corpus.add_doc("This is a document.")
    doc_ids1 = str(corpus.doc_ids)
    doc.words = [[0,4], [5,7], [8,9], [10,18], [18,19]]
    doc_ids2 = str(corpus.doc_ids)

    assert(doc_ids1 == doc_ids2)
    assert(doc.text.text[0] == "This is a document.")
    assert(doc.words.text == ['This', 'is', 'a', 'document', '.'])


def test_read_yaml_str2():
    corpus = read_yaml_str("_meta:\n  text:\n    type: characters\n"
    "  author:\n    type: characters\nwiDv:\n   text: This is a document.\n"
    "   author: John Doe\n", "<memory>")

    for doc in corpus.docs:
        assert(doc.text.text[0] == "This is a document.")
        assert(doc.author.text[0] == "John Doe")

def test_text_freq():
    corpus = Corpus()
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    doc = corpus.add_doc("This is a document.")
    doc.words = [(0, 4), (5, 7), (8, 9), (10, 18)]
    assert (corpus.text_freq("words") == 
            Counter({'This': 1, 'is': 1, 'a': 1, 'document': 1}))
    assert (corpus.text_freq("words", lambda x: "i" in x) == 
            Counter({'This': 1, 'is': 1}))
 
def test_val_freq():
    corpus = Corpus()
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    corpus.add_layer_meta("pos", layer_type="seq", base="words",
                           data=["NOUN", "VERB", "ADJ"])
    doc = corpus.add_doc("Colorless green ideas sleep furiously.")
    doc.words = [(0, 9), (10, 15), (16, 21), (22, 28), (29, 37)]
    doc.pos = ["ADJ", "ADJ", "NOUN", "VERB", "ADV"]
    assert (corpus.val_freq("pos") ==
        Counter({'ADJ': 2, 'NOUN': 1, 'VERB': 1, 'ADV': 1}))
    assert (corpus.val_freq("pos", ["NOUN", "VERB"]) ==
        Counter({'NOUN': 1, 'VERB': 1}))
    assert (corpus.val_freq("pos", lambda x: x[0] == "A") ==
        Counter({'ADJ': 2, 'ADV': 1}))
 
# Require fix of #29 in teanga2
#def test_tcf():
#    corpus = Corpus()
#    corpus.add_layer_meta("text")
#    corpus.add_layer_meta("words", layer_type="span", base="text")
#    doc = corpus.add_doc("This is a document.")
#    doc.words = [(0, 4), (5, 7), (8, 9), (10, 18)]
#    tmpfile = tempfile.mkstemp(suffix=".tcf")[1]
#    print(tmpfile, file=sys.stderr)
#    corpus.to_tcf(tmpfile)
#    read_tcf(tmpfile)


