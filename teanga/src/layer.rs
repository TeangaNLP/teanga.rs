/// This module contains the definition of the Layer and LayerDesc structs, as well as the LayerType and DataType enums.
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use crate::{TeangaError, TeangaResult, Value};
use serde::ser::SerializeSeq;
use itertools::Itertools;


/// Traits for converting a value into a Layer
pub trait IntoLayer {
    fn into_layer(self, meta : &LayerDesc) -> TeangaResult<Layer>;
}

impl IntoLayer for Layer {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(self)
    }
}

impl IntoLayer for String {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::Characters(self))
    }
}

impl IntoLayer for &str {
fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::Characters(self.to_string()))
    }
}

#[derive(Debug,Clone,Serialize,Deserialize)]
/// A layer description
pub struct LayerDesc {
    #[serde(rename = "type")]
    pub layer_type: LayerType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<DataType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_types: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Layer>,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub meta: HashMap<String, Value>, 
}

impl LayerDesc {
    pub fn new(name: &str, layer_type: LayerType, 
        base: Option<String>, data: Option<DataType>, link_types: Option<Vec<String>>, 
        target: Option<String>, default: Option<Layer>,
        meta: HashMap<String, Value>) -> TeangaResult<LayerDesc> {
        if layer_type == LayerType::characters && base != Some("".to_string()) && base != None {
            return Err(TeangaError::ModelError(
                    format!("Layer {} of type characters cannot be based on another layer", name)))
        }

        if layer_type != LayerType::characters && (base == Some("".to_string()) || base == None) {
            return Err(TeangaError::ModelError(
                format!("Layer {} of type {} must be based on another layer", name, layer_type)))
        }

        Ok(LayerDesc {
            layer_type,
            base,
            data,
            link_types,
            target,
            default,
            meta
         })
    }
}

#[derive(Debug,Clone,PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Layer {
    Characters(String),
    L1(Vec<u32>),
    L2(Vec<(u32,u32)>),
    L3(Vec<(u32,u32,u32)>),
    LS(Vec<String>),
    L1S(Vec<(u32,String)>),
    L2S(Vec<(u32,u32,String)>),
    L3S(Vec<(u32,u32,u32,String)>),
    MetaLayer(Vec<HashMap<String, Value>>)
}

#[allow(non_camel_case_types)]
#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub enum LayerType {
    characters,
    seq,
    div,
    element,
    span
}

impl Display for LayerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LayerType::characters => write!(f, "characters"),
            LayerType::seq => write!(f, "seq"),
            LayerType::div => write!(f, "div"),
            LayerType::element => write!(f, "element"),
            LayerType::span => write!(f, "span")
        }
    }
}

#[derive(Debug,Clone,PartialEq)]
pub enum DataType {
    String,
    Enum(Vec<String>),
    Link
}

impl Serialize for DataType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: serde::Serializer {
        match self {
            DataType::String => serializer.serialize_str("string"),
            DataType::Enum(vals) => {
                let mut seq = serializer.serialize_seq(Some(vals.len()))?;
                for val in vals {
                    seq.serialize_element(val)?;
                }
                seq.end()
            },
            DataType::Link => serializer.serialize_str("link")
        }
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D>(deserializer: D) -> Result<DataType, D::Error> where D: serde::Deserializer<'de> {
        struct DataTypeVisitor;
        impl<'de> serde::de::Visitor<'de> for DataTypeVisitor {
            type Value = DataType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or an array of strings")
            }

            fn visit_str<E>(self, value: &str) -> Result<DataType, E> where E: serde::de::Error {
                match value {
                    "string" => Ok(DataType::String),
                    "String" => Ok(DataType::String),
                    "link" => Ok(DataType::Link),
                    "Link" => Ok(DataType::Link),
                    _ => Err(serde::de::Error::invalid_value(serde::de::Unexpected::Str(value), &self))
                }
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<DataType, A::Error> where A: serde::de::SeqAccess<'de> {
                let mut vals = Vec::new();
                while let Some(val) = seq.next_element()? {
                    vals.push(val);
                }
                Ok(DataType::Enum(vals))
            }
        }
        deserializer.deserialize_any(DataTypeVisitor)
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DataType::String => write!(f, "string"),
            DataType::Enum(vals) => write!(f, "enum({})", vals.iter().join(",")),
            DataType::Link => write!(f, "link"),
        }
    }
}


