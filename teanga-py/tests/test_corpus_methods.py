### Systematic test of all `Corpus` methods to assure that the Rust implementation
### provides similar results to the Python implementation.
from teanga import Corpus, LayerDesc, read_json_str, read_yaml_str, read_cuac, read_json, read_yaml, from_url
from io import StringIO
import json
import os

def create_basic_corpus():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    corpus.add_layer_meta("pos", layer_type="seq", base="words", data="string")
    doc = corpus.add_doc("This is a document.")
    doc.words = [(0,4), (5,7), (8,9), (10,18), (18,19)]
    doc.pos = ["DT", "VBZ", "DT", "NN", "."]
    doc = corpus.add_doc("Colorless green ideas sleep furiously.")
    doc.words = [(0,8), (9,16), (17,22), (23,30), (31,40)]
    doc.pos = ["JJ", "JJ", "NNS", "VBZ", "RB"]
    return corpus

def test_doc_ids():
    corpus = create_basic_corpus()
    assert corpus.doc_ids == ['Kjco', '9wpe']

def test_meta():
    corpus = create_basic_corpus()
    assert corpus.meta == {
            "text": LayerDesc(layer_type="characters"),
            "words": LayerDesc(layer_type="span", base="text"),
            "pos": LayerDesc(layer_type="seq", base="words", data="string")
            }

def test_doc_by_id():
    corpus = create_basic_corpus()
    assert corpus.doc_by_id('9wpe').text == "Colorless green ideas sleep furiously."


def test_add_layer_meta():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    corpus.add_layer_meta("pos", layer_type="seq", base="words", data="string")
    assert "text" in corpus.meta

def test_add_doc():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    doc = corpus.add_doc("This is a document.")
    assert doc.text == "This is a document."
    assert doc.id is not None

def test_update_doc():
    corpus = create_basic_corpus()
    doc = corpus.doc_by_id('Kjco')
    doc.text = "This is an updated document."
    assert doc.text == "This is an updated document."
    assert doc.id == 'JHEz'

def test_docs():
    corpus = create_basic_corpus()
    doc_iter = corpus.docs
    assert next(doc_iter).text == "This is a document."
    assert next(doc_iter).text == "Colorless green ideas sleep furiously."
        

def test_set_meta():
    corpus = create_basic_corpus()
    new_meta = corpus.meta.copy()
    new_meta["foo"] = LayerDesc(layer_type="characters")
    corpus.meta = new_meta
    assert corpus.meta["foo"] == LayerDesc(layer_type="characters")

def test_search():
    corpus = Corpus(db="<memory>", new=True)
    corpus.add_layer_meta("text")
    corpus.add_layer_meta("words", layer_type="span", base="text")
    corpus.add_layer_meta("pos", layer_type="seq", base="words",
                           data=["NOUN", "VERB", "ADJ", "ADV"])
    corpus.add_layer_meta("lemma", layer_type="seq", base="words",
                           data="string")
    doc = corpus.add_doc("Colorless green ideas sleep furiously.")
    doc.words = [(0, 9), (10, 15), (16, 21), (22, 27), (28, 37)]
    doc.pos = ["ADJ", "ADJ", "NOUN", "VERB", "ADV"]
    doc.lemma = ["colorless", "green", "idea", "sleep", "furiously"]
    assert(list(corpus.search(pos="NOUN")) == ['9wpe'])
    assert(list(corpus.search(pos=["NOUN", "VERB"])) == ['9wpe'])
    assert(list(corpus.search(pos={"$in": ["NOUN", "VERB"]})) == ['9wpe'])
    assert(list(corpus.search(pos={"$regex": "N.*"})) == ['9wpe'])
    assert(list(corpus.search(pos="VERB", lemma="sleep")) == ['9wpe'])
    assert(list(corpus.search(pos="VERB", words="idea")) == [])
    assert(list(corpus.search(pos="VERB", words="ideas")) == ['9wpe'])
    assert(list(corpus.search({"pos": "VERB", "lemma": "sleep"})) == ['9wpe'])
    assert(list(corpus.search({"$and": {"pos": "VERB", "lemma": "sleep"}})) == ['9wpe'])

YAML_BASIC_CORPUS = """_meta:
    pos:
        type: seq
        base: words
        data: string
    text:
        type: characters
    words:
        type: span
        base: text
Kjco:
    pos: ["DT","VBZ","DT","NN","."]
    text: This is a document.
    words: [[0,4],[5,7],[8,9],[10,18],[18,19]]
9wpe:
    pos: ["JJ","JJ","NNS","VBZ","RB"]
    text: Colorless green ideas sleep furiously.
    words: [[0,8],[9,16],[17,22],[23,30],[31,40]]
"""

def test_to_yaml():
    corpus = create_basic_corpus()
    s = StringIO()
    corpus.to_yaml(s)
    assert s.getvalue() == YAML_BASIC_CORPUS

def test_to_yaml_str():
    corpus = create_basic_corpus()
    yaml_str = corpus.to_yaml_str()
    assert yaml_str == YAML_BASIC_CORPUS

def test_to_json():
    corpus = create_basic_corpus()
    s = StringIO()
    corpus.to_json(s)
    data = json.loads(s.getvalue())
    assert data == {
        "_meta": {
            "text": {"type": "characters"},
            "words": {"type": "span", "base": "text"},
            "pos": {"type": "seq", "base": "words", "data": "string"}
        },
        "Kjco": {
            "text": "This is a document.",
            "words": [[0, 4], [5, 7], [8, 9], [10, 18], [18, 19]],
            "pos": ["DT", "VBZ", "DT", "NN", "."]
        },
        "9wpe": {
            "text": "Colorless green ideas sleep furiously.",
            "words": [[0, 8], [9, 16], [17, 22], [23, 30], [31, 40]],
            "pos": ["JJ", "JJ", "NNS", "VBZ", "RB"]
        }
    }

def test_to_cuac():
    corpus = create_basic_corpus()
    tmpfile = "tmp.cuac"
    corpus.to_cuac(tmpfile)
    # Delete the temporary file after testing
    os.remove(tmpfile)

def test_read_json_str():
    corpus = create_basic_corpus()
    json_str = corpus.to_json_str()
    corpus2 = read_json_str(json_str, "<memory>")
    assert corpus2.order == corpus.order

def test_read_json():
    corpus = create_basic_corpus()
    tmpfile = "tmp.read_json.json"
    corpus.to_json(tmpfile)
    corpus2 = read_json(tmpfile, "<memory>")
    os.remove(tmpfile)
    assert corpus2.order == corpus.order

def test_read_yaml_str():
    corpus = create_basic_corpus()
    yaml_str = corpus.to_yaml_str()
    corpus2 = read_yaml_str(yaml_str, "<memory>")
    assert corpus2.order == corpus.order

def test_read_yaml():
    corpus = create_basic_corpus()
    tmpfile = "tmp.read_yaml.yaml"
    corpus.to_yaml(tmpfile)
    corpus2 = read_yaml(tmpfile, "<memory>")
    os.remove(tmpfile)
    assert corpus2.order == corpus.order

def test_from_url():
    corpus = create_basic_corpus()
    tmpfile = "tmp.from_url.yaml"
    corpus.to_yaml(tmpfile)
    corpus2 = from_url("file://" + os.path.abspath(tmpfile), "<memory>")
    os.remove(tmpfile)
    assert corpus2.order == corpus.order

def test_read_cuac():
    corpus = create_basic_corpus()
    tmpfile = "tmp.read_cuac.cuac"
    corpus.to_cuac(tmpfile)
    corpus2 = read_cuac(tmpfile, "<memory>")
    os.remove(tmpfile)
    assert corpus2.order == corpus.order




