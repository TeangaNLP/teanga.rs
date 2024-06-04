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

impl IntoLayer for Vec<u32> {
    fn into_layer(self, meta : &LayerDesc) -> TeangaResult<Layer> {
        if meta.layer_type == LayerType::seq {
            Ok(Layer::L1(self))
        } else if meta.layer_type == LayerType::div {
            Ok(Layer::L1(self))
        } else if meta.layer_type == LayerType::element {
            Ok(Layer::L1(self))
        } else {
            Err(TeangaError::ModelError(
                format!("Layer type L1 not supported for layer type {}", meta.layer_type)))
        }
    }
}

impl IntoLayer for Vec<(u32, u32)> {
    fn into_layer(self, meta : &LayerDesc) -> TeangaResult<Layer> {
        if meta.layer_type == LayerType::div {
            Ok(Layer::L2(self))
        } else if meta.layer_type == LayerType::element {
            Ok(Layer::L2(self))
        } else if meta.layer_type == LayerType::span {
            Ok(Layer::L2(self))
        } else {
            Err(TeangaError::ModelError(
                format!("Layer type L2 not supported for layer type {}", meta.layer_type)))
        }
    }
}

impl IntoLayer for Vec<(u32, u32, u32)> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::L3(self))
    }
}

impl IntoLayer for Vec<String> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::LS(self))
    }
}

impl IntoLayer for Vec<&'static str> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::LS(self.iter().map(|s| s.to_string()).collect()))
    }
}

impl IntoLayer for Vec<(u32, String)> {
    fn into_layer(self, meta : &LayerDesc) -> TeangaResult<Layer> {
        if meta.layer_type == LayerType::div {
            Ok(Layer::L1S(self))
        } else if meta.layer_type == LayerType::element {
            Ok(Layer::L1S(self))
        } else if meta.layer_type == LayerType::seq {
            Ok(Layer::L1S(self))
        } else {
            Err(TeangaError::ModelError(
                format!("Layer type L1S not supported for layer type {}", meta.layer_type)))
        }
    }
}

impl IntoLayer for Vec<(u32, &'static str)> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::L1S(self.iter().map(|(i, s)| (*i, s.to_string())).collect()))
    }
}

impl IntoLayer for Vec<(u32, u32, String)> {
    fn into_layer(self, meta : &LayerDesc) -> TeangaResult<Layer> {
        if meta.layer_type == LayerType::div {
            Ok(Layer::L2S(self))
        } else if meta.layer_type == LayerType::element {
            Ok(Layer::L2S(self))
        } else if meta.layer_type == LayerType::span {
            Ok(Layer::L2S(self))
        } else {
            Err(TeangaError::ModelError(
                format!("Layer type L2S not supported for layer type {}", meta.layer_type)))
        }
    }
}

impl IntoLayer for Vec<(u32, u32, &'static str)> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::L2S(self.iter().map(|(i, j, s)| (*i, *j, s.to_string())).collect()))
    }
}

impl IntoLayer for Vec<(u32, u32, u32, String)> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::L3S(self))
    }
}

impl IntoLayer for Vec<(u32, u32, u32, &'static str)> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::L3S(self.iter().map(|(i, j, k, s)| (*i, *j, *k, s.to_string())).collect()))
    }
}

#[derive(Debug,Clone,Serialize,Deserialize,Default,PartialEq)]
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

impl Layer {
    /// Get the indexes part of the layer relative to the base layer
    /// 
    /// # Arguments
    /// layer_desc - The layer description
    /// n - The length of the base layer
    pub fn indexes(&self, layer_desc: &LayerDesc, n : u32) -> Vec<(u32, u32)> {
        match self {
            Layer::Characters(s) => vec![(0u32, s.len() as u32)],
            Layer::L1(indexes) => {
                if layer_desc.layer_type == LayerType::seq {
                    (0..indexes.len()).map(|i| (i as u32, (i + 1) as u32)).collect()
                } else if layer_desc.layer_type == LayerType::div {
                    (0..indexes.len()).map(|i| (indexes[i], if i == indexes.len() - 1 { n } else { indexes[i + 1] })).collect()
                } else if layer_desc.layer_type == LayerType::element {
                    (0..indexes.len()).map(|i| (indexes[i], indexes[i] + 1)).collect()
                } else {
                    panic!("Layer type L1 not supported for layer type {}", layer_desc.layer_type)
                }
            },
            Layer::L2(indexes) => {
                if layer_desc.layer_type == LayerType::div {
                    (0..indexes.len()).map(|i| (indexes[i].0, if i == indexes.len() - 1 { n } else { indexes[i + 1].0 })).collect()
                } else if layer_desc.layer_type == LayerType::element {
                    (0..indexes.len()).map(|i| (indexes[i].0, indexes[i].1)).collect()
                } else if layer_desc.layer_type == LayerType::span {
                    indexes.clone()
                } else {
                    panic!("Layer type L2 not supported for layer type {}", layer_desc.layer_type)
                }
            }
            Layer::L3(indexes) => indexes.iter().map(|&(i, j, _)| (i, j)).collect(),
            Layer::LS(indexes) => (0..indexes.len()).map(|i| (i as u32, (i + 1) as u32)).collect(),
            Layer::L1S(indexes) => {
                if layer_desc.layer_type == LayerType::div {
                    (0..indexes.len()).map(|i| (indexes[i].0, if i == indexes.len() - 1 { n } else { indexes[i + 1].0 })).collect()
                } else if layer_desc.layer_type == LayerType::element {
                    (0..indexes.len()).map(|i| (indexes[i].0, indexes[i].0 + 1)).collect()
                } else if layer_desc.layer_type == LayerType::seq {
                    (0..indexes.len()).map(|i| (i as u32, (i + 1) as u32)).collect()
                } else {
                    panic!("Layer type L1S not supported for layer type {}", layer_desc.layer_type)
                }
            },
            Layer::L2S(indexes) => {
                if layer_desc.layer_type == LayerType::div {
                    (0..indexes.len()).map(|i| (indexes[i].0, if i == indexes.len() - 1 { n } else { indexes[i + 1].0 })).collect()
                } else if layer_desc.layer_type == LayerType::element {
                    (0..indexes.len()).map(|i| (indexes[i].0, indexes[i].1)).collect()
                } else if layer_desc.layer_type == LayerType::span {
                    indexes.iter().map(|&(i, j, _)| (i, j)).collect()
                } else {
                    panic!("Layer type L2S not supported for layer type {}", layer_desc.layer_type)
                }
            },
            Layer::L3S(indexes) => indexes.iter().map(|&(i, j, _, _)| (i, j)).collect(),
            Layer::MetaLayer(_) => Vec::new()
        }
    }

    /// Get the data part of the layer
    pub fn data(&self, layer_desc : &LayerDesc) -> Vec<TeangaData> {
       match self {
           Layer::Characters(s) => vec![TeangaData::None; s.len()],
           Layer::L1(indexes) => {
                if layer_desc.layer_type == LayerType::seq {
                    indexes.iter().map(|i| TeangaData::Link(*i)).collect()
                } else if layer_desc.layer_type == LayerType::div || 
                    layer_desc.layer_type == LayerType::element {
                    vec![TeangaData::None; indexes.len()]
                } else {
                    panic!("Layer type L1 not supported for layer type {}", layer_desc.layer_type)
                }
            },
            Layer::L2(indexes) => {
                if layer_desc.layer_type == LayerType::div ||
                    layer_desc.layer_type == LayerType::element {
                    indexes.iter().map(|(_, j)| TeangaData::Link(*j)).collect()
                } else if layer_desc.layer_type == LayerType::span {
                    vec![TeangaData::None; indexes.len()]
                } else {
                    panic!("Layer type L2 not supported for layer type {}", layer_desc.layer_type)
                }
            }
            Layer::L3(indexes) => indexes.iter().map(|&(_, _, k)| TeangaData::Link(k)).collect(),
            Layer::LS(indexes) => indexes.iter().map(|s| TeangaData::String(s.clone())).collect(),
            Layer::L1S(indexes) => {
                if layer_desc.layer_type == LayerType::div ||
                    layer_desc.layer_type == LayerType::element {
                    indexes.iter().map(|(_, s)| TeangaData::String(s.clone())).collect()
                } else if layer_desc.layer_type == LayerType::seq {
                    indexes.iter().map(|(i, s)| TeangaData::TypedLink(*i, s.clone())).collect()
                } else {
                    panic!("Layer type L1S not supported for layer type {}", layer_desc.layer_type)
                }
            },
            Layer::L2S(indexes) => {
                if layer_desc.layer_type == LayerType::div ||
                    layer_desc.layer_type == LayerType::element {
                    indexes.iter().map(|(_, j, s)| TeangaData::TypedLink(*j, s.clone())).collect()
                } else if layer_desc.layer_type == LayerType::span {
                    indexes.iter().map(|(_, _, s)| TeangaData::String(s.clone())).collect()
                } else {
                    panic!("Layer type L2S not supported for layer type {}", layer_desc.layer_type)
                }
            },
            Layer::L3S(indexes) => indexes.iter().map(|(_, _, k, s)| TeangaData::TypedLink(*k, s.clone())).collect(),
            Layer::MetaLayer(_) => Vec::new()
        }
    }

    /// Get the number of annotatable elements in this layer
    pub fn len(&self) -> usize {
        match self {
            Layer::Characters(s) => s.len(),
            Layer::L1(indexes) => indexes.len(),
            Layer::L2(indexes) => indexes.len(),
            Layer::L3(indexes) => indexes.len(),
            Layer::LS(indexes) => indexes.len(),
            Layer::L1S(indexes) => indexes.len(),
            Layer::L2S(indexes) => indexes.len(),
            Layer::L3S(indexes) => indexes.len(),
            Layer::MetaLayer(_) => 0
        }
    }

    /// Get the characters part of the layer
    ///
    /// Returns None if the layer is not of type characters
    pub fn characters(&self) -> Option<&str> {
        match self {
            Layer::Characters(c) => Some(c),
            _ => None
        }
    }
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

impl Default for LayerType {
    fn default() -> Self {
        LayerType::characters
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

#[derive(Debug,Clone,PartialEq,Eq,Hash)]
pub enum TeangaData {
    None,
    String(String),
    Link(u32),
    TypedLink(u32, String)
}
