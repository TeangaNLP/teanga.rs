use std::sync::mpsc::{Sender, Receiver, channel};
use crate::document::Document;
use crate::{WriteableCorpus, ReadableCorpus, LayerDesc, TeangaResult, IntoLayer, DocumentContent, teanga_id, TeangaYamlError};
use std::collections::HashMap;
use std::cell::Cell;


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
        ChannelCorpusReceiver { meta, rx: self.rx, ended: Cell::new(false) }
    }
}


pub struct ChannelCorpusReceiver {
    meta: HashMap<String, LayerDesc>,
    rx: Receiver<ChannelCorpusMessage>,
    // Set once the sender has closed, so that reading again after the end
    // returns no documents rather than waiting forever or panicking
    ended: Cell<bool>
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
        Box::new(ChannelCorpusIterator { rx: &self.rx, ended: &self.ended }.map(|x| x.map(|(_, doc)| doc)))
    }
    /// Iterate over all documents in the corpus with their IDs
    fn iter_doc_ids<'a>(&'a self) -> Box<dyn Iterator<Item=TeangaResult<(String, Document)>> + 'a> {
        Box::new(ChannelCorpusIterator { rx: &self.rx, ended: &self.ended })
    }
}

struct ChannelCorpusIterator<'a> {
    rx: &'a Receiver<ChannelCorpusMessage>,
    ended: &'a Cell<bool>
}

impl Iterator for ChannelCorpusIterator<'_> {
    type Item = TeangaResult<(String, Document)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ended.get() {
            return None;
        }
        match self.rx.recv() {
            Ok(ChannelCorpusMessage::Document((id, doc))) => Some(Ok((id, doc))),
            // The sender may also be dropped without calling close()
            Ok(ChannelCorpusMessage::End) | Err(_) => {
                self.ended.set(true);
                None
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

    fn text_meta() -> HashMap<String, LayerDesc> {
        let mut meta = HashMap::new();
        meta.insert("text".to_string(), LayerDesc::new("text", LayerType::characters, None, None, None, None, None, HashMap::new()).unwrap());
        meta
    }

    #[test]
    fn test_channel_corpus_read_past_end() {
        let (mut tx, rx) = channel_corpus();
        tx.set_meta(text_meta()).unwrap();
        tx.add_doc(vec![("text".to_string(), "bar")]).unwrap();
        tx.close();
        drop(tx);
        let rx = rx.await_meta();
        let mut iter = rx.iter_doc_ids();
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
        assert!(rx.iter_doc_ids().next().is_none());
    }

    #[test]
    fn test_channel_corpus_sender_dropped_without_close() {
        let (mut tx, rx) = channel_corpus();
        tx.set_meta(text_meta()).unwrap();
        tx.add_doc(vec![("text".to_string(), "bar")]).unwrap();
        drop(tx);
        let rx = rx.await_meta();
        assert_eq!(rx.iter_docs().count(), 1);
    }

    /// Generating a Shoco model reads ahead, and on a small corpus it reads
    /// to the end before the documents are written (issue #13)
    #[test]
    fn test_channel_corpus_write_cuac_generate() {
        let (mut tx, rx) = channel_corpus();
        tx.set_meta(text_meta()).unwrap();
        tx.add_doc(vec![("text".to_string(), "bar")]).unwrap();
        tx.add_doc(vec![("text".to_string(), "baz")]).unwrap();
        tx.close();
        drop(tx);
        let rx = rx.await_meta();
        let mut data = Vec::new();
        crate::write_cuac_with_config(&mut data, &rx, &crate::CuacConfig::new()
            .with_string_compression(crate::StringCompressionMethod::GenerateShocoModel(1000))).unwrap();
        let mut corpus = crate::SimpleCorpus::new();
        crate::read_cuac(&mut data.as_slice(), &mut corpus).unwrap();
        assert_eq!(corpus.iter_docs().count(), 2);
    }
}
