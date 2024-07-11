/// Teanga Compressed Format
use crate::{Layer, LayerDesc, Document};
use std::collections::HashMap;
use ciborium::from_reader;
use thiserror::Error;
use crate::{TeangaResult, TeangaError, WriteableCorpus};
use std::io::BufRead;

use crate::tcf::{TCFResult, TCFError};
use crate::tcf::index::Index;
use crate::tcf::tcf::{TCF, TCF_EMPTY_LAYER};



fn bytes_to_layer(bytes : &[u8], idx : &mut Index, layer_desc : &LayerDesc) -> TCFResult<(Layer, usize)> {
    let (tcf, len) = TCF::from_bytes(bytes, 0, layer_desc)?;
    Ok((tcf.to_layer(idx, layer_desc), len))
}

pub enum ReadLayerResult<Layer> {
    Layer(Layer),
    Empty,
    Eof
}

fn read_layer<R : BufRead>(bytes : &mut R, idx : &mut Index, layer_desc : &LayerDesc) -> TCFResult<ReadLayerResult<Layer>> {
    match TCF::from_reader(bytes, layer_desc)? {
        ReadLayerResult::Layer(tcf) => Ok(ReadLayerResult::Layer(tcf.to_layer(idx, layer_desc))),
        ReadLayerResult::Empty => Ok(ReadLayerResult::Empty),
        ReadLayerResult::Eof => Ok(ReadLayerResult::Eof)
    }
}


pub fn bytes_to_doc(bytes : &[u8], offset : usize,
    meta_keys : &Vec<String>,
    meta : &HashMap<String, LayerDesc>,
    cache : &mut Index) -> TeangaResult<Document> {
    let mut layers = Vec::new();
    let mut i = offset;
    for key in meta_keys.iter() {
        if bytes[i] != TCF_EMPTY_LAYER {
            let (layer, n) = bytes_to_layer(&bytes[i..], 
                cache, meta.get(key).ok_or_else(|| TeangaError::DocumentKeyError(key.clone()))?)?;
            layers.push((key.clone(), layer));
            i += n;
        } else {
            i += 1;
        }
    }
    Document::new(layers, meta)
}



#[derive(Error, Debug)]
pub enum ReadDocError {
    #[error("Model error: {0}")]
    TeangaError(#[from] TeangaError),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Document key error: {0}")]
    DocumentKeyError(String),
    #[error("TCF error: {0}")]
    TCFError(#[from] TCFError)
}


pub fn read_doc<R : BufRead>(input : &mut R, meta_keys : &Vec<String>,
    meta : &HashMap<String, LayerDesc>, cache : &mut Index) -> Result<Option<Document>, ReadDocError> {
    let mut layers = Vec::new();
    for key in meta_keys.iter() {
        let layer_desc = meta.get(key)
            .ok_or_else(|| ReadDocError::DocumentKeyError(key.clone()))?;
        match read_layer(input, cache, layer_desc)? {
            ReadLayerResult::Layer(layer) => {
                layers.push((key.clone(), layer));
            },
            ReadLayerResult::Empty => {
            },
            ReadLayerResult::Eof => {
                return Ok(None);
            }
        }
    }
    Ok(Some(Document::new(layers, meta)?))
}


#[derive(Error, Debug)]
pub enum TCFReadError {
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Teanga error: {0}")]
    TeangaError(#[from] TeangaError),
    #[error("Ciborium error: {0}")]
    CiboriumError(#[from] ciborium::de::Error<std::io::Error>),
    #[error("TCF read error: {0}")]
    TCFError(#[from] ReadDocError)
}


pub fn read_tcf<R: std::io::BufRead, C: WriteableCorpus>(
    input : &mut R, corpus : &mut C) -> Result<(), TCFReadError> {
    let mut meta_bytes = vec![0u8; 4];
    input.read_exact(meta_bytes.as_mut_slice())?;
    let len = u32::from_be_bytes([meta_bytes[0], meta_bytes[1], meta_bytes[2], meta_bytes[3]]) as usize;
    let mut meta_bytes = vec![0u8; len];
    input.read_exact(meta_bytes.as_mut_slice())?;
    let meta : HashMap<String, LayerDesc> = from_reader(meta_bytes.as_slice())?;
    corpus.set_meta(meta.clone());
    let mut cache = Index::new();
    let mut meta_keys : Vec<String> = meta.keys().cloned().collect();
    meta_keys.sort();
    while let Some(doc) = read_doc(input, &meta_keys, &meta, &mut cache)? {
        corpus.add_doc(doc)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SimpleCorpus, build_layer, LayerType, DataType, Corpus, IntoLayer};
    use crate::tcf::write::write_tcf;

    #[test]
    fn test_read_doc() {
        let mut corpus = SimpleCorpus::new();
        build_layer(&mut corpus, "text").add().unwrap();
        build_layer(&mut corpus, "document")
            .layer_type(LayerType::div)
            .base("characters")
            .default(Layer::L1(vec![0]))
            .add().unwrap();
        build_layer(&mut corpus, "url")
            .layer_type(LayerType::seq)
            .base("document")
            .data(DataType::String)
            .add().unwrap();
        println!("str: {:?}", smaz::compress(b"Beginners BBQ Class Taking Place in Missoula!\nDo you want to get better at making delicious BBQ? You will have the opportunity, put this on your calendar now. Thursday, September 22nd join World Class BBQ Champion, Tony Balay from Lonestar Smoke Rangers. He will be teaching a beginner level class for everyone who wants to get better with their culinary skills.\nHe will teach you everything you need to know to compete in a KCBS BBQ competition, including techniques, recipes, timelines, meat selection and trimming, plus smoker and fire information.\nThe cost to be in the class is $35 per person, and for spectators it is free. Included in the cost will be either a t-shirt or apron and you will be tasting samples of each meat that is prepared."));
        let doc_id = corpus.add_doc(
            vec![("text".to_string(), 
                "Beginners BBQ Class Taking Place in Missoula!\nDo you want to get better at making delicious BBQ? You will have the opportunity, put this on your calendar now. Thursday, September 22nd join World Class BBQ Champion, Tony Balay from Lonestar Smoke Rangers. He will be teaching a beginner level class for everyone who wants to get better with their culinary skills.\nHe will teach you everything you need to know to compete in a KCBS BBQ competition, including techniques, recipes, timelines, meat selection and trimming, plus smoker and fire information.\nThe cost to be in the class is $35 per person, and for spectators it is free. Included in the cost will be either a t-shirt or apron and you will be tasting samples of each meat that is prepared.".to_string()),
                ("url".to_string(),
                "https://klyq.com/beginners-bbq-class-taking-place-in-missoula/".to_string())
            ]).unwrap();
        let mut doc = corpus.get_doc_by_id(&doc_id).unwrap();
        doc.set("url", Layer::LS(vec!["https://klyq.com/beginners-bbq-class-taking-place-in-missoula/".to_string()]));
        let mut data : Vec<u8> = Vec::new();
        write_tcf(&mut data, &corpus).unwrap();
        let mut corpus2 = SimpleCorpus::new();
        read_tcf(&mut data.as_slice(), &mut corpus2).unwrap();
        assert_eq!(corpus, corpus2);
    }

    #[test]
    fn test_read_doc_2() {
        let mut corpus = SimpleCorpus::new();
        build_layer(&mut corpus, "text").add().unwrap();
        let _doc_id = corpus.add_doc(vec![(
            "text".to_string(),
            "Test string".to_string())]).unwrap();
        let mut data : Vec<u8> = Vec::new();
        write_tcf(&mut data, &corpus).unwrap();
        assert_eq!(data, vec![0, 0, 0, 23, 161, 100, 116, 101, 120, 116, 161, 100, 116, 121, 112, 101, 106, 99, 104, 97, 114, 97, 99, 116, 101, 114, 115, 0, 0, 7, 254, 84, 54, 35, 77, 114, 84]);
        let mut corpus2 = SimpleCorpus::new();
        read_tcf(&mut data.as_slice(), &mut corpus2).unwrap();
    }

    #[test]
    fn test_serialize_3() {
        let mut corpus = SimpleCorpus::new();
        build_layer(&mut corpus, "text").add().unwrap();
        build_layer(&mut corpus, "document")
            .layer_type(LayerType::div)
            .base("characters")
            .default(Layer::L1(vec![0]))
            .add().unwrap();
        build_layer(&mut corpus, "url")
            .layer_type(LayerType::seq)
            .base("document")
            .data(DataType::String)
            .add().unwrap();
        build_layer(&mut corpus, "timestamp")
            .layer_type(LayerType::seq)
            .base("document")
            .data(DataType::String)
            .add().unwrap();
        build_layer(&mut corpus, "words")
            .layer_type(LayerType::span)
            .base("characters")
            .add().unwrap();
        build_layer(&mut corpus, "pos")
            .layer_type(LayerType::seq)
            .base("words")
            .data(DataType::Enum(vec!["ADJ", "ADP", "PUNCT", "ADV", "AUX", 
                    "SYM", "INTJ", "CCONJ", "X", "NOUN", "DET", "PROPN", 
                    "NUM", "VERB", "PART", "PRON", "SPACE", "SCONJ"].into_iter().map(|s| s.to_owned()).collect()))
            .add().unwrap();
        build_layer(&mut corpus, "lemma")
            .layer_type(LayerType::seq)
            .base("words")
            .data(DataType::String)
            .add().unwrap();
        corpus.add_doc(vec![(
            "text".to_string(),
            "Beginners BBQ Class Taking Place in Missoula!\nDo you want to get better at making delicious BBQ? You will have the opportunity, put this on your calendar now. Thursday, September 22nd join World Class BBQ Champion, Tony Balay from Lonestar Smoke Rangers. He will be teaching a beginner level class for everyone who wants to get better with their culinary skills.\nHe will teach you everything you need to know to compete in a KCBS BBQ competition, including techniques, recipes, timelines, meat selection and trimming, plus smoker and fire information.\nThe cost to be in the class is $35 per person, and for spectators it is free. Included in the cost will be either a t-shirt or apron and you will be tasting samples of each meat that is prepared.".into_layer(&corpus.get_meta()["text"]).unwrap()),
            ("timestamp".to_string(),
            vec!["2019-04-25T12:57:54Z"].into_layer(&corpus.get_meta()["timestamp"]).unwrap()),
            ("url".to_string(),
            vec!["https://klyq.com/beginners-bbq-class-taking-place-in-missoula/"].into_layer(&corpus.get_meta()["url"]).unwrap()),
            ("words".to_string(),
             vec![(0, 9), (10, 13), (14, 19), (20, 26), (27, 32), (33, 35), (36, 44), (44, 45), (45, 46), (46, 48), (49, 52), (53, 57), (58, 60), (61, 64), (65, 71), (72, 74), (75, 81), (82, 91), (92, 95), (95, 96), (97, 100), (101, 105), (106, 110), (111, 114), (115, 126), (126, 127), (128, 131), (132, 136), (137, 139), (140, 144), (145, 153), (154, 157), (157, 158), (159, 167), (167, 168), (169, 178), (179, 183), (184, 188), (189, 194), (195, 200), (201, 204), (205, 213), (213, 214), (215, 219), (220, 225), (226, 230), (231, 239), (240, 245), (246, 253), (253, 254), (255, 257), (258, 262), (263, 265), (266, 274), (275, 276), (277, 285), (286, 291), (292, 297), (298, 301), (302, 310), (311, 314), (315, 320), (321, 323), (324, 327), (328, 334), (335, 339), (340, 345), (346, 354), (355, 361), (361, 362), (362, 363), (363, 365), (366, 370), (371, 376), (377, 380), (381, 391), (392, 395), (396, 400), (401, 403), (404, 408), (409, 411), (412, 419), (420, 422), (423, 424), (425, 429), (430, 433), (434, 445), (445, 446), (447, 456), (457, 467), (467, 468), (469, 476), (476, 477), (478, 487), (487, 488), (489, 493), (494, 503), (504, 507), (508, 516), (516, 517), (518, 522), (523, 529), (530, 533), (534, 538), (539, 550), (550, 551), (551, 552), (552, 555), (556, 560), (561, 563), (564, 566), (567, 569), (570, 573), (574, 579), (580, 582), (583, 584), (584, 586), (587, 590), (591, 597), (597, 598), (599, 602), (603, 606), (607, 617), (618, 620), (621, 623), (624, 628), (628, 629), (630, 638), (639, 641), (642, 645), (646, 650), (651, 655), (656, 658), (659, 665), (666, 667), (668, 669), (669, 670), (670, 675), (676, 678), (679, 684), (685, 688), (689, 692), (693, 697), (698, 700), (701, 708), (709, 716), (717, 719), (720, 724), (725, 729), (730, 734), (735, 737), (738, 746), (746, 747)].into_layer(&corpus.get_meta()["words"]).unwrap()),
             ("pos".to_string(),
              vec!["NOUN", "PROPN", "PROPN", "PROPN", "PROPN", "ADP", "PROPN", "PUNCT", "SPACE", "AUX", "PRON", "VERB", "PART", "VERB", "ADJ", "ADP", "VERB", "ADJ", "PROPN", "PUNCT", "PRON", "AUX", "VERB", "DET", "NOUN", "PUNCT", "VERB", "PRON", "ADP", "PRON", "NOUN", "ADV", "PUNCT", "PROPN", "PUNCT", "PROPN", "NOUN", "VERB", "PROPN", "PROPN", "PROPN", "PROPN", "PUNCT", "PROPN", "PROPN", "ADP", "PROPN", "PROPN", "PROPN", "PUNCT", "PRON", "AUX", "AUX", "VERB", "DET", "NOUN", "NOUN", "NOUN", "ADP", "PRON", "PRON", "VERB", "PART", "VERB", "ADJ", "ADP", "PRON", "ADJ", "NOUN", "PUNCT", "SPACE", "PRON", "AUX", "VERB", "PRON", "PRON", "PRON", "VERB", "PART", "VERB", "PART", "VERB", "ADP", "DET", "PROPN", "PROPN", "NOUN", "PUNCT", "VERB", "NOUN", "PUNCT", "NOUN", "PUNCT", "NOUN", "PUNCT", "NOUN", "NOUN", "CCONJ", "NOUN", "PUNCT", "CCONJ", "NOUN", "CCONJ", "NOUN", "NOUN", "PUNCT", "SPACE", "DET", "NOUN", "PART", "AUX", "ADP", "DET", "NOUN", "AUX", "SYM", "NUM", "ADP", "NOUN", "PUNCT", "CCONJ", "ADP", "NOUN", "PRON", "AUX", "ADJ", "PUNCT", "VERB", "ADP", "DET", "NOUN", "AUX", "AUX", "CCONJ", "DET", "NOUN", "PUNCT", "NOUN", "CCONJ", "NOUN", "CCONJ", "PRON", "AUX", "AUX", "VERB", "NOUN", "ADP", "DET", "NOUN", "PRON", "AUX", "VERB", "PUNCT"].into_layer(&corpus.get_meta()["pos"]).unwrap()),
             ("lemma".to_string(),
              vec!["beginner", "BBQ", "Class", "Taking", "Place", "in", "Missoula", "!", "\n", "do", "you", "want", "to", "get", "well", "at", "make", "delicious", "BBQ", "?", "you", "will", "have", "the", "opportunity", ",", "put", "this", "on", "your", "calendar", "now", ".", "Thursday", ",", "September", "22nd", "join", "World", "Class", "BBQ", "Champion", ",", "Tony", "Balay", "from", "Lonestar", "Smoke", "Rangers", ".", "he", "will", "be", "teach", "a", "beginner", "level", "class", "for", "everyone", "who", "want", "to", "get", "well", "with", "their", "culinary", "skill", ".", "\n", "he", "will", "teach", "you", "everything", "you", "need", "to", "know", "to", "compete", "in", "a", "KCBS", "BBQ", "competition", ",", "include", "technique", ",", "recipe", ",", "timeline", ",", "meat", "selection", "and", "trimming", ",", "plus", "smoker", "and", "fire", "information", ".", "\n", "the", "cost", "to", "be", "in", "the", "class", "be", "$", "35", "per", "person", ",", "and", "for", "spectator", "it", "be", "free", ".", "include", "in", "the", "cost", "will", "be", "either", "a", "t", "-", "shirt", "or", "apron", "and", "you", "will", "be", "taste", "sample", "of", "each", "meat", "that", "be", "prepare", "."].into_layer(&corpus.get_meta()["lemma"]).unwrap())]).unwrap();
        let mut data : Vec<u8> = Vec::new();
        write_tcf(&mut data, &corpus).unwrap();
        let mut corpus2 = SimpleCorpus::new();
        read_tcf(&mut data.as_slice(), &mut corpus2).unwrap();
        for (docid1, docid2) in corpus.get_docs().iter().zip(corpus2.get_docs().iter()) {
            let doc1 = corpus.get_doc_by_id(docid1).unwrap();
            let doc2 = corpus.get_doc_by_id(docid2).unwrap();
            assert_eq!(doc1.keys(), doc2.keys());
            for key in doc1.keys() {
                println!("key: {}", key);
                assert_eq!(doc1[&key], doc2[&key]);
            }
        }
        assert_eq!(corpus.meta, corpus2.meta);
        assert_eq!(corpus.order, corpus2.order);
        assert_eq!(corpus.content.keys().collect::<Vec<&String>>(), corpus2.content.keys().collect::<Vec<&String>>());
        for (k, doc) in corpus.content.iter() {
            let doc2 = corpus2.content.get(k);
            let mut keys1 = doc.content.keys().collect::<Vec<&String>>();
            keys1.sort();
            let mut keys2 = doc2.unwrap().content.keys().collect::<Vec<&String>>();
            keys2.sort();
            assert_eq!(keys1, keys2);
            for (k2, l2) in doc.content.iter() {
                let l = doc2.unwrap().content.get(k2).unwrap();
                assert_eq!(l, l2);
            }
        }
        assert_eq!(corpus.content, corpus2.content);
        //assert_eq!(corpus, corpus2);
     }


}
