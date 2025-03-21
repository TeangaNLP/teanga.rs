use std::sync::mpsc::{Sender, Receiver, channel};
use crate::document::Document;
use crate::layer::Layer;
use crate::{Corpus, LayerDesc, LayerType, DataType, Value, TeangaResult, IntoLayer, DocumentContent, teanga_id, WriteableCorpus, TeangaYamlError};
use std::collections::HashMap;
use std::cell::RefCell;


pub struct ChannelCorpusSender {
    meta: HashMap<String, LayerDesc>,
    order: Vec<String>,
    tx: Sender<ChannelCorpusMessage>
}

pub struct ChannelCorpusReceiver {
    meta: RefCell<HashMap<String, LayerDesc>>,
    rx: Receiver<ChannelCorpusMessage>
}

enum ChannelCorpusMessage {
    Document((String, Document)),
    Meta((String, LayerDesc)),
    End
}

pub fn channel_corpus() -> (ChannelCorpusSender, ChannelCorpusReceiver) {
    let (tx, rx) = channel();
    (ChannelCorpusSender { meta: HashMap::new(), order: Vec::new(), tx }, ChannelCorpusReceiver { meta: RefCell::new(HashMap::new()), rx })
}

impl ChannelCorpusSender {
    pub fn close(&self) {
        self.tx.send(ChannelCorpusMessage::End).unwrap();
    }

    pub fn read_yaml_header<'de, R: std::io::Read>(&mut self, r: R) -> Result<(), TeangaYamlError> {
        Ok(crate::serialization::read_yaml_meta(r, self)?)
    }
}

impl Corpus for ChannelCorpusSender {
    type LayerStorage = Layer;
    type Content = Document;

    fn add_layer_meta(&mut self, name: String, layer_type: LayerType, 
        base: Option<String>, data: Option<DataType>, link_types: Option<Vec<String>>, 
        target: Option<String>, default: Option<Layer>,
        meta : HashMap<String, Value>) -> TeangaResult<()> {
        let ld = LayerDesc {
            layer_type,
            base,
            data,
            link_types,
            target,
            default,
            meta
        };
        self.meta.insert(name.clone(), ld.clone());
        self.tx.send(ChannelCorpusMessage::Meta((name, ld))).unwrap();
        Ok(())
    }

    fn add_doc<D : IntoLayer, DC : DocumentContent<D>>(&mut self, content : DC) -> TeangaResult<String> {
        let doc = Document::new(content, &self.meta)?;
        let id = teanga_id(&self.order, &doc);
        self.order.push(id.clone());
        self.tx.send(ChannelCorpusMessage::Document((id.clone(), doc))).unwrap();
        Ok(id)
    }

    fn update_doc<D : IntoLayer, DC: DocumentContent<D>>(&mut self, _id : &str, _content : DC) -> TeangaResult<String> {
        panic!("Not possible for channel corpus");
    }

    fn remove_doc(&mut self, _id : &str) -> TeangaResult<()> {
        panic!("Not possible for channel corpus");
    }

    fn get_doc_by_id(&self, _id : &str) -> TeangaResult<Document> {
        panic!("Not possible for channel corpus");
    }

    fn get_docs(&self) -> Vec<String> {
        panic!("Not possible for channel corpus");
    }

    fn get_meta(&self) -> &HashMap<String, LayerDesc> {
        &self.meta
    }

    fn get_order(&self) -> &Vec<String> {
        &self.order
    }
}

impl WriteableCorpus for ChannelCorpusSender {
    fn set_meta(&mut self, meta : HashMap<String, LayerDesc>) -> TeangaResult<()> {
        self.meta = meta;
        Ok(())
    }

    fn set_order(&mut self, order : Vec<String>) -> TeangaResult<()> {
        self.order = order;
        Ok(())
    }
}



impl Corpus for ChannelCorpusReceiver {
    type LayerStorage = Layer;
    type Content = Document;

    fn add_layer_meta(&mut self, _name: String, _layer_type: LayerType, 
        _base: Option<String>, _data: Option<DataType>, _link_types: Option<Vec<String>>, 
        _target: Option<String>, _default: Option<Layer>,
        _meta : HashMap<String, Value>) -> TeangaResult<()> {
        panic!("Not possible for channel corpus");
    }

    fn add_doc<D : IntoLayer, DC : DocumentContent<D>>(&mut self, _content : DC) -> TeangaResult<String> {
        panic!("Not possible for channel corpus");
    }

    fn update_doc<D : IntoLayer, DC: DocumentContent<D>>(&mut self, _id : &str, _content : DC) -> TeangaResult<String> {
        panic!("Not possible for channel corpus");
    }

    fn remove_doc(&mut self, _id : &str) -> TeangaResult<()> {
        panic!("Not possible for channel corpus");
    }

    fn get_doc_by_id(&self, _id : &str) -> TeangaResult<Document> {
        panic!("Not possible for channel corpus");
    }

    fn get_docs(&self) -> Vec<String> {
        panic!("Not possible for channel corpus");
    }

    fn get_meta(&self) -> &HashMap<String, LayerDesc> {
        panic!("Not possible for channel corpus");
    }

    fn get_order(&self) -> &Vec<String> {
        panic!("Not possible for channel corpus");
    }


    fn iter_docs<'a>(&'a self) -> Box<dyn Iterator<Item=TeangaResult<Document>> + 'a> {
        Box::new(ChannelCorpusIterator { rx: &self.rx, meta : self.meta.clone() }.map(|x| x.map(|(_, doc)| doc)))
    }
    /// Iterate over all documents in the corpus with their IDs
    fn iter_doc_ids<'a>(&'a self) -> Box<dyn Iterator<Item=TeangaResult<(String, Document)>> + 'a> {
        Box::new(ChannelCorpusIterator { rx: &self.rx , meta : self.meta.clone() })
    }
}

struct ChannelCorpusIterator<'a> {
    rx: &'a Receiver<ChannelCorpusMessage>,
    meta: RefCell<HashMap<String, LayerDesc>>
}

impl Iterator for ChannelCorpusIterator<'_> {
    type Item = TeangaResult<(String, Document)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.rx.recv().unwrap() {
                ChannelCorpusMessage::Document((id, doc)) => return Some(Ok((id, doc))),
                ChannelCorpusMessage::End => return None,
                ChannelCorpusMessage::Meta((id, ld)) => {
                    self.meta.borrow_mut().insert(id, ld);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread;

    #[test]
    fn test_channel_corpus() {
        let (mut tx, rx) = channel_corpus();
        tx.build_layer("text").add().unwrap();
        tx.build_doc().layer("text", "bar").unwrap().add().unwrap();
        tx.close();
        for res in rx.iter_doc_ids() {
            let (_id, doc) = res.unwrap();
            assert_eq!(doc.text("text", tx.get_meta()).unwrap(), vec!["bar"]);
        }
    }

    #[test]
    fn test_channel_corpus_multithreaded() {
        let (mut tx, rx) = channel_corpus();
        let meta = tx.get_meta().clone();
        thread::spawn(move || {
            tx.build_layer("text").add().unwrap();
            tx.build_doc().layer("text", "bar").unwrap().add().unwrap();
            tx.close();
        });
        thread::spawn(move || {
            for res in rx.iter_doc_ids() {
                let (_id, doc) = res.unwrap();
                assert_eq!(doc.text("text", &meta).unwrap(), vec!["bar"]);
            }
        });
    }
}


