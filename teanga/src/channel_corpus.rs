use std::sync::mpsc::{Sender, Receiver, channel};
use crate::document::Document;
use crate::layer::Layer;
use crate::{Corpus, LayerDesc, LayerType, DataType, Value, TeangaResult, IntoLayer, DocumentContent, teanga_id, WriteableCorpus, TeangaYamlError};
use std::collections::HashMap;


pub struct ChannelCorpusSender {
    meta: HashMap<String, LayerDesc>,
    order: Vec<String>,
    tx: Sender<ChannelCorpusMessage>,
    tx2: Sender<HashMap<String, LayerDesc>>
}

pub struct ChannelCorpusPrereceiver {
    rx: Receiver<ChannelCorpusMessage>,
    rx2: Receiver<HashMap<String, LayerDesc>>
}

impl ChannelCorpusPrereceiver {
    pub fn await_meta(self) -> ChannelCorpusReceiver {
        let meta = self.rx2.recv().unwrap();
        ChannelCorpusReceiver { meta, rx: self.rx }
    }
}


pub struct ChannelCorpusReceiver {
    meta: HashMap<String, LayerDesc>,
    rx: Receiver<ChannelCorpusMessage>
}

enum ChannelCorpusMessage {
    Document((String, Document)),
    End
}

pub fn channel_corpus() -> (ChannelCorpusSender, ChannelCorpusPrereceiver) {
    let (tx, rx) = channel();
    let (tx2, rx2) = channel();
    (ChannelCorpusSender { meta: HashMap::new(), order: Vec::new(), tx, tx2 }, ChannelCorpusPrereceiver { rx, rx2 })
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

    fn add_layer_meta(&mut self, _name: String, _layer_type: LayerType, 
        _base: Option<String>, _data: Option<DataType>, _link_types: Option<Vec<String>>, 
        _target: Option<String>, _default: Option<Layer>,
        _meta : HashMap<String, Value>) -> TeangaResult<()> {
        panic!("Not possible for channel corpus");
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
        self.tx2.send(self.meta.clone()).unwrap();
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
        &self.meta
    }

    fn get_order(&self) -> &Vec<String> {
        panic!("Not possible for channel corpus");
    }


    fn iter_docs<'a>(&'a self) -> Box<dyn Iterator<Item=TeangaResult<Document>> + 'a> {
        Box::new(ChannelCorpusIterator { rx: &self.rx }.map(|x| x.map(|(_, doc)| doc)))
    }
    /// Iterate over all documents in the corpus with their IDs
    fn iter_doc_ids<'a>(&'a self) -> Box<dyn Iterator<Item=TeangaResult<(String, Document)>> + 'a> {
        Box::new(ChannelCorpusIterator { rx: &self.rx })
    }
}

struct ChannelCorpusIterator<'a> {
    rx: &'a Receiver<ChannelCorpusMessage>
}

impl Iterator for ChannelCorpusIterator<'_> {
    type Item = TeangaResult<(String, Document)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.rx.recv().unwrap() {
                ChannelCorpusMessage::Document((id, doc)) => return Some(Ok((id, doc))),
                ChannelCorpusMessage::End => return None,
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
        let mut meta = HashMap::new();
        meta.insert("text".to_string(), LayerDesc::new("text", LayerType::characters, None, None, None, None, None, HashMap::new()).unwrap());
        tx.set_meta(meta).unwrap();
        tx.build_doc().layer("text", "bar").unwrap().add().unwrap();
        tx.close();
        let rx = rx.await_meta();
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
            let mut meta = HashMap::new();
            meta.insert("text".to_string(), LayerDesc::new("text", LayerType::characters, None, None, None, None, None, HashMap::new()).unwrap());
            tx.set_meta(meta).unwrap();
            tx.build_doc().layer("text", "bar").unwrap().add().unwrap();
            tx.close();
        });
        thread::spawn(move || {
            let rx = rx.await_meta();
            for res in rx.iter_doc_ids() {
                let (_id, doc) = res.unwrap();
                assert_eq!(doc.text("text", &meta).unwrap(), vec!["bar"]);
            }
        });
    }
}


