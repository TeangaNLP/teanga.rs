//! Serialization support for Teanga
use crate::{WriteableCorpus, ReadableCorpus, LayerDesc, Layer, TeangaJsonError, Document};
use itertools::Itertools;
use serde::Deserializer;
use serde::de::Visitor;
use serde::ser::{Serializer, SerializeMap};
use std::cmp::min;
use std::collections::HashMap;
use std::io::BufRead;
use std::io::Read;
use std::io::Write;
use thiserror::Error;

struct TeangaVisitor2<'a, C : WriteableCorpus>(&'a mut C, bool);

impl <'de,'a, C: WriteableCorpus> Visitor<'de> for TeangaVisitor2<'a, C> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string representing a corpus")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where A: serde::de::MapAccess<'de>
    {
        let mut order = None;
        while let Some(ref key) = map.next_key::<String>()? {
            if key == "_meta" {
                let data = map.next_value::<HashMap<String, LayerDesc>>()?;
                self.0.set_meta(data)
                    .map_err(serde::de::Error::custom)?;
            } else if !self.1 && key == "_order" {
                order = Some(map.next_value::<Vec<String>>()?);
            } else if !self.1 {
                let doc = map.next_value::<HashMap<String, Layer>>()?;
                let id = self.0.add_doc(doc).map_err(serde::de::Error::custom)?;
                if id[..min(id.len(), key.len())] != key[..min(id.len(), key.len())] {
                    return Err(serde::de::Error::custom(format!("Document fails hash check: {} != {}", id, key)))
                }
            }
        }
        if let Some(order) = order {
            self.0.set_order(order)
                .map_err(serde::de::Error::custom)?;
        }
        Ok(())
    }
}

fn corpus_serialize<C : ReadableCorpus, S>(c : &C, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer
{
    let mut map = serializer.serialize_map(Some(3))?;
    map.serialize_entry("_meta", &c.get_meta())?;
    for res in c.iter_doc_ids() {
        let (id, doc) = res.map_err(serde::ser::Error::custom)?;
        map.serialize_entry(&id, &doc)?;
    }
    map.end()
}

/// Write a corpus as pretty YAML
///
/// # Arguments
///
/// * `corpus` - The corpus to write
/// * `writer` - The writer to write to
///
/// # Returns
///
/// A result indicating success or failure
pub fn pretty_yaml_serialize<W : Write, C: ReadableCorpus>(corpus: &C, mut writer: W) -> Result<(), SerializeError> {
    writer.write_all(b"_meta:\n")?;
    for name in corpus.get_meta().keys().sorted() {
        let meta = &corpus.get_meta()[name];
        writer.write_all(b"    ")?;
        writer.write_all(name.as_bytes())?;
        writer.write_all(b":\n")?;
        writer.write_all(b"        type: ")?;
        writer.write_all(serde_yml::to_string(&meta.layer_type)?.as_bytes())?;
        if meta.base != Some("".to_string()) && meta.base != None {
            writer.write_all(b"        base: ")?;
            writer.write_all(serde_yml::to_string(&meta.base)?.as_bytes())?;
        }
        if let Some(ref data) = meta.data {
            writer.write_all(b"        data: ")?;
            writer.write_all(serde_yml::to_string(data)?.as_bytes())?;
        }
        if let Some(ref values) = meta.link_types {
            writer.write_all(b"        link_types: ")?;
            writer.write_all(serde_json::to_string(values)?.as_bytes())?;
            writer.write_all(b"\n")?;
        }
        if let Some(ref target) = meta.target {
            writer.write_all(b"        target: ")?;
            writer.write_all(serde_yml::to_string(target)?.as_bytes())?;
        }
        if let Some(ref default) = meta.default {
            writer.write_all(b"        default: ")?;
            writer.write_all(serde_json::to_string(default)?.as_bytes())?;
            writer.write_all(b"\n")?;
        }
    }
    for res in corpus.iter_doc_ids() {
        let (id, doc) = res?;
        writer.write_all(id.as_bytes())?;
        writer.write_all(b":\n")?;
        for name in doc.keys().iter().sorted() {
            let layer = &doc[name];
            if let Layer::Characters(_) = layer {
                writer.write_all(b"    ")?;
                writer.write_all(name.as_bytes())?;
                writer.write_all(b": ")?;
                writer.write_all(serde_yml::to_string(layer)?.as_bytes())?;
            } else {
                writer.write_all(b"    ")?;
                writer.write_all(name.as_bytes())?;
                writer.write_all(b": ")?;
                writer.write_all(serde_json::to_string(layer)?.as_bytes())?;
                writer.write_all(b"\n")?;
            }
        }
    }
    Ok(())
}

/// Read a corpus from JSON
///
/// # Arguments
///
/// * `reader` - The reader to read from
/// * `corpus` - The corpus to read into
pub fn read_json<'de, R: Read, C: WriteableCorpus>(reader: R, corpus : &mut C) -> Result<(), serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    deserializer.deserialize_any(TeangaVisitor2(corpus, false))
}

/// Read only the metadata from a JSON file
///
/// # Arguments
///
/// * `reader` - The reader to read from
/// * `corpus` - The corpus to read into
pub fn read_json_meta<'de, R: Read, C: WriteableCorpus>(reader: R, corpus : &mut C) -> Result<(), serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    deserializer.deserialize_any(TeangaVisitor2(corpus, true))
}

/// Read a corpus from YAML
///
/// # Arguments
///
/// * `reader` - The reader to read from
/// * `corpus` - The corpus to read into
/// * `meta_only` - Whether to read only the metadata
pub fn read_yaml<'de, R: Read, C: WriteableCorpus>(reader: R, corpus : &mut C) -> Result<(), SerializeError> {
    let char_iter = reader.bytes().filter_map(Result::ok).map(|b| b as char);
    let parser = yaml_rust::parser::Parser::new(char_iter);
    let mut reader = YamlStreamReader { parser };
    while let Some((key, value)) = reader.next_entry()? {
        if key == "_meta" {
            corpus.set_meta(serde_json::from_value(value)?)?;
        } else if key == "_order" {
            corpus.set_order(serde_json::from_value(value)?)?;
        } else {
            let doc : HashMap<String, Layer> = serde_json::from_value(value)?;
            let id = corpus.add_doc(doc)?;
            if id[..min(id.len(), key.len())] != key[..min(id.len(), key.len())] {
                panic!("Document fails hash check: {} != {}", id, key);
            }
        }
    }
    Ok(())
}

// Read only the metadata from a YAML file
//
// # Arguments
//
// * `reader` - The reader to read from
// * `corpus` - The corpus to read into
pub fn read_yaml_meta<'de, R: Read, C: WriteableCorpus>(reader: R, corpus : &mut C) -> Result<(), SerializeError> {
    let char_iter = reader.bytes().filter_map(Result::ok).map(|b| b as char);
    let parser = yaml_rust::parser::Parser::new(char_iter);
    let mut reader = YamlStreamReader { parser };
    while let Some((key, value)) = reader.next_entry()? {
        if key == "_meta" {
            corpus.set_meta(serde_json::from_value(value)?)?;
        } else if key == "_order" {
            corpus.set_order(serde_json::from_value(value)?)?;
        }
    }
    Ok(())
}

/// Read a corpus from JSONL. That is a file with one JSON document per line. 
/// As this format does not have metadata, the corpus must have already been
/// initialized with metadata.
///
/// # Arguments
///
/// * `reader` - The reader to read from
/// * `corpus` - The corpus to read into
pub fn read_jsonl<'de, R: BufRead, C : WriteableCorpus>(reader: R, corpus : &mut C) -> Result<(), TeangaJsonError> {
    for line in reader.lines() {
        let doc : HashMap<String, Layer> = serde_json::from_str(&line?)?;
        corpus.add_doc(doc)?;
    }
    Ok(())
}

/// Read a single line of JSON as a JSON-L document
///
/// # Arguments
///
/// * `line` - The line to read
/// * `corpus` - The corpus to read into
pub fn read_jsonl_line<'de>(line: String, meta : &HashMap<String, LayerDesc>) -> Result<Document, TeangaJsonError> {
        let doc : HashMap<String, Layer> = serde_json::from_str(&line)?;
        Ok(Document::new(doc, meta)?)
}

/// Write a corpus as JSON
///
/// # Arguments
///
/// * `writer` - The writer to write to
/// * `corpus` - The corpus to write
pub fn write_json<W : Write, C : ReadableCorpus>(mut writer : W, corpus : &C) -> Result<(), serde_json::Error>  {
    let mut ser = serde_json::Serializer::new(&mut writer);
    corpus_serialize(corpus, &mut ser)
}

/// Write a corpus as YAML
///
/// # Arguments
///
/// * `writer` - The writer to write to
/// * `corpus` - The corpus to write
pub fn write_yaml<W : Write, C : ReadableCorpus>(mut writer : W, corpus : &C) -> Result<(), serde_yml::Error>  {
    let mut ser = serde_yml::Serializer::new(&mut writer);
    corpus_serialize(corpus, &mut ser)
}


/// Write a corpus as JSONL. This will not write the metadata of the corpus.
///
/// # Arguments
///
/// * `writer` - The writer to write to
/// * `corpus` - The corpus to write
pub fn write_jsonl<W : Write, C : ReadableCorpus>(mut writer : W, corpus : &C) -> Result<(), SerializeError> {
    for res in corpus.iter_doc_ids() {
        let (_, doc) = res?;
        serde_json::to_writer(&mut writer, &doc)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

use yaml_rust::parser::{Event, Parser};
use yaml_rust::scanner::{TScalarStyle, TokenType};
use yaml_rust::yaml::Yaml;

struct YamlStreamReader<T : Iterator<Item=char>> {
    parser : Parser<T>
}

impl <T : Iterator<Item=char>> YamlStreamReader<T> {
    fn next_entry(&mut self) -> Result<Option<(String, serde_json::Value)>, SerializeError> {
        loop {
            let (event, marker) = self.parser.peek()?;
            match event {
                Event::StreamStart => {
                    self.parser.next()?;
                },
                Event::StreamEnd => return Ok(None),
                Event::DocumentStart => {
                    self.parser.next()?;
                },
                Event::DocumentEnd => return Ok(None),
                Event::MappingStart(_) => {
                    self.parser.next()?;
                    break;
                },
                Event::MappingEnd => {
                    self.parser.next()?;
                    return Ok(None);
                },
                Event::Scalar(_, _, _, _) => {
                    break;
                },
                _ => {
                    return Err(SerializeError::YamlFormat("Expected mapping start".to_string(), marker.clone()));
                }
            }
        }
        let (event, marker) = self.parser.next()?;
        let key = match event {
            Event::Scalar(value, _, _, _) => {
                value
            },
            _ => {
                return Err(SerializeError::YamlFormat("Expected scalar".to_string(), marker));
            }
        };
        Ok(Some((key, self.read_value()?)))
    }

    fn read_value(&mut self) -> Result<serde_json::Value, SerializeError> {
        let (event, marker) = self.parser.next()?;
        match event {
            Event::Scalar(key, style, aid, tag) => {
                let s = yaml_to_json(scalar_to_yaml(key, style, aid, tag));
                Ok(s)
            },
            Event::SequenceStart(_) => {
                self.read_seq()
            }
            Event::MappingStart(_) => {
                self.read_obj()
            }
            _ => {
                return Err(SerializeError::YamlFormat("Expected scalar, map or sequence".to_string(), marker));
            }
        }
    }

    fn read_seq(&mut self) -> Result<serde_json::Value, SerializeError> {
        let mut seq = Vec::new();
        loop {
            let (event, _) = self.parser.peek()?;
            match event {
                Event::SequenceEnd => {
                    self.parser.next()?;
                    break;
                },
                _ => {
                    seq.push(self.read_value()?);
                }
            }
        }
        Ok(serde_json::Value::Array(seq))
    }

    fn read_obj(&mut self) -> Result<serde_json::Value, SerializeError> {
        let mut obj = serde_json::Map::new();
        loop {
            let (event, marker) = self.parser.next()?;
            match event {
                Event::MappingEnd => {
                    break;
                },
                Event::Scalar(key, _, _, _) => {
                    obj.insert(key, self.read_value()?);
                },
                _ => {
                    return Err(SerializeError::YamlFormat("Expected scalar".to_string(), marker));
                }
            }
        }
        Ok(serde_json::Value::Object(obj))
    }
}

fn yaml_to_json(yaml : Yaml) -> serde_json::Value {
    match yaml {
        Yaml::Array(v) => {
            let mut arr = Vec::new();
            for item in v {
                arr.push(yaml_to_json(item));
            }
            serde_json::Value::Array(arr)
        },
        Yaml::Hash(v) => {
            let mut obj = serde_json::Map::new();
            for (key, value) in v {
                obj.insert(key.as_str().unwrap().to_string(), yaml_to_json(value));
            }
            serde_json::Value::Object(obj)
        },
        Yaml::String(v) => serde_json::Value::String(v),
        Yaml::Integer(v) => serde_json::Value::Number(serde_json::Number::from(v)),
        Yaml::Real(v) => serde_json::Value::Number(serde_json::Number::from_f64(v.parse::<f64>().unwrap()).unwrap()),
        Yaml::Boolean(v) => serde_json::Value::Bool(v),
        Yaml::Null => serde_json::Value::Null,
        _ => serde_json::Value::Null,
    }
}

fn scalar_to_yaml(v : String, style : TScalarStyle, _aid : usize, tag : Option<TokenType>) -> Yaml {
    if style != TScalarStyle::Plain {
        Yaml::String(v)
    } else if let Some(TokenType::Tag(ref handle, ref suffix)) = tag {
        if handle == "!!" {
            match suffix.as_ref() {
                "bool" => {
                    // "true" or "false"
                    match v.parse::<bool>() {
                        Err(_) => Yaml::BadValue,
                        Ok(v) => Yaml::Boolean(v),
                    }
                }
                "int" => match v.parse::<i64>() {
                    Err(_) => Yaml::BadValue,
                    Ok(v) => Yaml::Integer(v),
                },
                "float" => match parse_f64(&v) {
                    Some(_) => Yaml::Real(v),
                    None => Yaml::BadValue,
                },
                "null" => match v.as_ref() {
                    "~" | "null" => Yaml::Null,
                    _ => Yaml::BadValue,
                },
                _ => Yaml::String(v),
            }
        } else {
            Yaml::String(v)
        }
    } else {
        // Datatype is not specified, or unrecognized
        Yaml::from_str(&v)
    }
}

fn parse_f64(v: &str) -> Option<f64> {
    match v {
        ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF" => Some(f64::INFINITY),
        "-.inf" | "-.Inf" | "-.INF" => Some(f64::NEG_INFINITY),
        ".nan" | "NaN" | ".NAN" => Some(f64::NAN),
        _ => v.parse::<f64>().ok(),
    }
}

/// A serialization error
#[derive(Error,Debug)]
pub enum SerializeError {
    /// An error occurred during JSON serialization
    #[error("Json error: {0}")]
    Json(#[from] serde_json::Error),
    /// An error occurred during YAML serialization
    #[error("Yaml error: {0}")]
    Yaml(#[from] serde_yml::Error),
    /// A generic I/O Error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// An error with the data was encountered
    #[error("Teanga model error: {0}")]
    Teanga(#[from] crate::TeangaError),
    /// An error when formatting was encountered
    #[error("IO error: {0}")]
    Fmt(#[from] std::fmt::Error),
    /// An error in decoding UTF-8
    #[error("UTF8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    /// An error in decoding YAML
    #[error("YAML error: {0}")]
    Yaml2(#[from] yaml_rust::ScanError),
    /// A format error in the yaml
    #[error("YAML format error: {0}")]
    YamlFormat(String, yaml_rust::scanner::Marker),
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimpleCorpus;
    use std::collections::HashSet;
    use serde_json::json;
    use crate::Corpus;

    #[test]
    fn test_yaml_stream_reader() {
        let doc = "_meta:
    text:
        type: characters
    tokens:
        type: span
        base: text
_order: [\"ecWc\"]
ecWc:
    text: This is an example
    tokens: [[0, 4], [5, 7], [8, 10], [11, 18]]
";
        let mut yaml_stream_reader = YamlStreamReader { parser: Parser::new(doc.chars()) };
        assert_eq!(("_meta".to_string(), json!({
            "text": {
                "type": "characters"
            },
            "tokens": {
                "type": "span",
                "base": "text"
            }
        })), yaml_stream_reader.next_entry().unwrap().unwrap());
        assert_eq!(("_order".to_string(), json!(["ecWc"])), yaml_stream_reader.next_entry().unwrap().unwrap());
        assert_eq!(("ecWc".to_string(), json!({
            "text": "This is an example",
            "tokens": [[0, 4], [5, 7], [8, 10], [11, 18]]
        })), yaml_stream_reader.next_entry().unwrap().unwrap());
    }
        

    #[test]
    fn test_deserialize_yaml() {
        let doc = "_meta:
    text:
        type: characters
    tokens:
        type: span
        base: text
_order: [\"ecWc\"]
ecWc:
    text: This is an example
    tokens: [[0, 4], [5, 7], [8, 10], [11, 18]]
";
        let mut corpus = SimpleCorpus::new();
        read_yaml(doc.as_bytes(), &mut corpus).unwrap();
    }

    #[test]
    fn test_deserialize_json() {
        let doc = r#"{
    "_meta": {
        "text": {
            "type": "characters"
        },
        "tokens": {
            "type": "span",
            "base": "text"
        }
    },
    "_order": [
        "ecWc"
    ],
    "ecWc": {
        "text": "This is an example",
        "tokens": [
            [
                0,
                4
            ],
            [
                5,
                7
            ],
            [
                8,
                10
            ],
            [
                11,
                18
            ]
        ]
    }
}"#;
        let mut corpus = SimpleCorpus::new();
        read_json(doc.as_bytes(), &mut corpus).unwrap();
    }

    #[test]
    fn test_serialize_yaml() {
        let mut corpus = SimpleCorpus::new();
        corpus.add_layer_meta("text".to_string(), crate::LayerType::characters,
           None, None, None, None, None, HashMap::new()).unwrap();
        corpus.add_layer_meta("tokens".to_string(), crate::LayerType::span,
            Some("text".to_string()), None, None, None, None, HashMap::new()).unwrap();
        let doc = HashMap::from_iter(vec![("text".to_string(), Layer::Characters("This is an example".to_string())),
                                           ("tokens".to_string(), Layer::L2(vec![(0, 4), (5, 7), (8, 10), (11, 18)]))]);
        corpus.add_doc(doc).unwrap();
        let mut out = Vec::new();
        write_yaml(&mut out, &corpus).unwrap();
    }

    #[test]
    fn test_pretty_yaml() {
        let mut corpus = SimpleCorpus::new();
        corpus.add_layer_meta("text".to_string(), crate::LayerType::characters,
           None, None, None, None, None, HashMap::new()).unwrap();
        corpus.add_layer_meta("tokens".to_string(), crate::LayerType::span,
            Some("text".to_string()), None, None, None, None, HashMap::new()).unwrap();
        let doc = HashMap::from_iter(vec![("text".to_string(), Layer::Characters("This is an example".to_string())),
                                           ("tokens".to_string(), Layer::L2(vec![(0, 4), (5, 7), (8, 10), (11, 18)]))]);
        corpus.add_doc(doc).unwrap();
        let mut out = Vec::new();
        pretty_yaml_serialize(&corpus, &mut out).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(),
            "_meta:\n    text:\n        type: characters\n    tokens:\n        type: span\n        base: text\necWc:\n    text: This is an example\n    tokens: [[0,4],[5,7],[8,10],[11,18]]\n");
    }
 
    #[test]
    fn test_1() {
        let mut corpus = SimpleCorpus::new();
        read_yaml("_meta:\n  text:\n    type: characters\nKjco:\n   text: This is a document.\n".as_bytes(), 
            &mut corpus).unwrap();
    }

    #[test]
    fn test_2() {
        let data = "_meta:
  text:
    type: characters
  document:
    type: div
    base: characters".to_string();
 
        read_yaml_meta(data.as_bytes(), &mut SimpleCorpus::new()).unwrap();
    }

    #[test]
    fn test_data() {
        let data = "_meta:
  text:
    type: characters
aeW7:
  text: Foobar
  _created: 08.09.2016 00:29
  _newpar: null";

        let mut corpus = SimpleCorpus::new();
        read_yaml(data.as_bytes(), &mut corpus).unwrap();
    }

    #[test]
    fn test_twitter() {
        let data = "_meta:
    text:
        type: characters
dkJv:
    text: hopeless for tmr :(
    _user: '{\"screen_name\": \"yuwraxkim\", \"time_zone\": \"Jakarta\", \"profile_background_image_url\":
  \"http://pbs.twimg.com/profile_background_images/585476378365014016/j1mvQu3c.png\",
  \"profile_background_image_url_https\": \"https://pbs.twimg.com/profile_background_images/585476378365014016/j1mvQu3c.png\",
  \"default_profile_image\": false, \"url\": null, \"profile_text_color\": \"000000\", \"following\":
  false, \"listed_count\": 3, \"entities\": {\"description\": {\"urls\": []}}, \"utc_offset\":
  25200, \"profile_sidebar_border_color\": \"000000\", \"name\": \"yuwra\", \"favourites_count\":
  196, \"followers_count\": 1281, \"location\": \"wearegsd;favor;pucukfams;barbx\", \"protected\":
  false, \"notifications\": false, \"profile_image_url_https\": \"https://pbs.twimg.com/profile_images/622631732399898624/kmYsX_k1_normal.jpg\",
  \"profile_use_background_image\": true, \"profile_image_url\": \"http://pbs.twimg.com/profile_images/622631732399898624/kmYsX_k1_normal.jpg\",
  \"lang\": \"id\", \"statuses_count\": 19710, \"friends_count\": 1264, \"profile_banner_url\":
  \"https://pbs.twimg.com/profile_banners/3078803375/1433287528\", \"geo_enabled\": true,
  \"is_translator\": false, \"contributors_enabled\": false, \"profile_sidebar_fill_color\":
  \"000000\", \"created_at\": \"Sun Mar 08 05:43:40 +0000 2015\", \"verified\": false, \"profile_link_color\":
  \"000000\", \"is_translation_enabled\": false, \"has_extended_profile\": false, \"id_str\":
  \"3078803375\", \"follow_request_sent\": false, \"profile_background_color\": \"000000\",
  \"default_profile\": false, \"profile_background_tile\": true, \"id\": 3078803375, }'
    _retweet_count: '0'
    _favorited: 'false'
    _entities: '{\"hashtags\": [], \"user_mentions\": [], \"urls\": [], \"symbols\": []}'
    _source: <a href=\"https://mobile.twitter.com\" rel=\"nofollow\">Mobile Web (M2)</a>
    _truncated: 'false'
    _is_quote_status: 'false'
    _lang: en
    _retweeted: 'false'
    _created_at: Fri Jul 24 10:42:49 +0000 2015
    _metadata: '{\"iso_language_code\": \"en\", \"result_type\": \"recent\"}'
    _favorite_count: '0'
    _id_str: '624530164626534400'
    _id: '624530164626534400'".to_string();
        let mut corpus = SimpleCorpus::new();
        read_yaml(data.as_bytes(), &mut corpus).unwrap();
        let mut buf = Vec::new();
        write_yaml(&mut buf, &corpus).unwrap();
        let out_yaml = String::from_utf8(buf).unwrap().replace("\n", "");
        eprintln!("{}", out_yaml);
        let left_tokens : HashSet<&str> = HashSet::from_iter(out_yaml.split(" "));
        let data = data.replace("\n", "");
        let right_tokens = HashSet::from_iter(data.split(" "));
        //for a in left_tokens.difference(&right_tokens) {
        //    eprintln!("{}", a);
        //}
        //for a in right_tokens.difference(&left_tokens) {
        //    eprintln!("{}", a);
        //}
        assert_eq!(left_tokens, right_tokens);
    }
}
