use std::sync::mpsc::{Sender, Receiver, channel};
use crate::document::Document;
use crate::{WriteableCorpus, ReadableCorpus, LayerDesc, TeangaResult, IntoLayer, DocumentContent, teanga_id, TeangaYamlError};
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
        Ok(crate::serialization::read_yaml_with_config(r, self, crate::SerializationSettings::new().header_only())?)
    }
}

impl WriteableCorpus for ChannelCorpusSender {
    fn add_doc<D : IntoLayer, DC : DocumentContent<D>>(&mut self, content : DC) -> TeangaResult<String> {
        let doc = Document::new(content, &self.meta)?;
        let id = teanga_id(&self.order, &doc);
        self.order.push(id.clone());
        self.tx.send(ChannelCorpusMessage::Document((id.clone(), doc))).unwrap();
        Ok(id)
    }

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



impl ReadableCorpus for ChannelCorpusReceiver {
    fn get_meta(&self) -> &HashMap<String, LayerDesc> {
        &self.meta
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
    use crate::LayerType;

    #[test]
    fn test_channel_corpus() {
        let (mut tx, rx) = channel_corpus();
        let mut meta = HashMap::new();
        meta.insert("text".to_string(), LayerDesc::new("text", LayerType::characters, None, None, None, None, None, HashMap::new()).unwrap());
        tx.set_meta(meta).unwrap();
        tx.add_doc(vec![("text".to_string(), "bar")]).unwrap();
        tx.close();
        let rx = rx.await_meta();
        for res in rx.iter_doc_ids() {
            let (_id, doc) = res.unwrap();
            assert_eq!(doc.text("text", rx.get_meta()).unwrap(), vec!["bar"]);
        }
    }
    

    #[test]
    fn test_channel_corpus_multithreaded() {
        let (mut tx, rx) = channel_corpus();
        thread::spawn(move || {
            let mut meta = HashMap::new();
            meta.insert("text".to_string(), LayerDesc::new("text", LayerType::characters, None, None, None, None, None, HashMap::new()).unwrap());
            tx.set_meta(meta).unwrap();
            tx.add_doc(vec![("text".to_string(), "bar")]).unwrap();
            tx.close();
        });
        thread::spawn(move || {
            let rx = rx.await_meta();
            for res in rx.iter_doc_ids() {
                let (_id, doc) = res.unwrap();
                assert_eq!(doc.text("text", &rx.get_meta()).unwrap(), vec!["bar"]);
            }
        });
    }
}


