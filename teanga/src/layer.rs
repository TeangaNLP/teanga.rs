//! Layers in a document
//!
//! This module contains the definition of the Layer and LayerDesc structs, as well as the LayerType and DataType enums.
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use crate::{TeangaError, TeangaResult, Value};
use serde::ser::SerializeSeq;
use itertools::Itertools;
use crate::Document;


/// Traits for converting a value into a Layer
pub trait IntoLayer {
    /// Convert the value into a Layer
    ///
    /// # Arguments
    ///
    /// * `meta` - The metadata for the layer
    ///
    /// # Returns
    ///
    /// The converted layer
    fn into_layer(self, meta : &LayerDesc) -> TeangaResult<Layer>;
    /// Convert the value into a metadata layer
    fn into_meta_layer(self) -> TeangaResult<Layer>;
}

impl IntoLayer for Layer {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(self)
    }

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        match self {
            Layer::MetaLayer(v) => Ok(Layer::MetaLayer(v)),
            Layer::Characters(s) => Ok(Layer::MetaLayer(Value::String(s))),
            Layer::L1(indexes) => Ok(Layer::MetaLayer(Value::Array(indexes.into_iter().map(|i| Value::Int(i as i32)).collect()))),
            Layer::L2(indexes) => Ok(Layer::MetaLayer(Value::Array(indexes.into_iter().map(|(i, j)| Value::Array(vec![Value::Int(i as i32), Value::Int(j as i32)])).collect()))),
            Layer::L3(indexes) => Ok(Layer::MetaLayer(Value::Array(indexes.into_iter().map(|(i, j, k)| Value::Array(vec![Value::Int(i as i32), Value::Int(j as i32), Value::Int(k as i32)])).collect()))),
            Layer::LS(indexes) => Ok(Layer::MetaLayer(Value::Array(indexes.into_iter().map(|s| Value::String(s)).collect()))),
            Layer::L1S(indexes) => Ok(Layer::MetaLayer(Value::Array(indexes.into_iter().map(|(i, s)| Value::Array(vec![Value::Int(i as i32), Value::String(s)])).collect()))),
            Layer::L2S(indexes) => Ok(Layer::MetaLayer(Value::Array(indexes.into_iter().map(|(i, j, s)| Value::Array(vec![Value::Int(i as i32), Value::Int(j as i32), Value::String(s)])).collect()))),
            Layer::L3S(indexes) => Ok(Layer::MetaLayer(Value::Array(indexes.into_iter().map(|(i, j, k, s)| Value::Array(vec![Value::Int(i as i32), Value::Int(j as i32), Value::Int(k as i32), Value::String(s)])).collect()))),
        }
    }
}

impl IntoLayer for String {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::Characters(self))
    }
    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::String(self)))
    }
}

impl IntoLayer for &str {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::Characters(self.to_string()))
    }

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::String(self.to_string())))
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

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::Array(self.into_iter().map(|i| Value::Int(i as i32)).collect())))
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

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::Array(self.into_iter().map(|(i, j)| Value::Array(vec![Value::Int(i as i32), Value::Int(j as i32)])).collect())))
    }
}

impl IntoLayer for Vec<(u32, u32, u32)> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::L3(self))
    }

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::Array(self.into_iter().map(|(i, j, k)| Value::Array(vec![Value::Int(i as i32), Value::Int(j as i32), Value::Int(k as i32)])).collect())))
    }
}

impl IntoLayer for Vec<String> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::LS(self))
    }

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::Array(self.into_iter().map(|s| Value::String(s)).collect())))
    }
}

impl IntoLayer for Vec<&'static str> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::LS(self.iter().map(|s| s.to_string()).collect()))
    }

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::Array(self.iter().map(|s| Value::String(s.to_string())).collect())))
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

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::Array(self.into_iter().map(|(i, s)| Value::Array(vec![Value::Int(i as i32), Value::String(s)])).collect())))
    }
}

impl IntoLayer for Vec<(u32, &'static str)> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::L1S(self.iter().map(|(i, s)| (*i, s.to_string())).collect()))
    }

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::Array(self.iter().map(|(i, s)| Value::Array(vec![Value::Int(*i as i32), Value::String(s.to_string())])).collect())))
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

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::Array(self.into_iter().map(|(i, j, s)| Value::Array(vec![Value::Int(i as i32), Value::Int(j as i32), Value::String(s)])).collect())))
    }
}

impl IntoLayer for Vec<(u32, u32, &'static str)> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::L2S(self.iter().map(|(i, j, s)| (*i, *j, s.to_string())).collect()))
    }

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::Array(self.iter().map(|(i, j, s)| Value::Array(vec![Value::Int(*i as i32), Value::Int(*j as i32), Value::String(s.to_string())])).collect())))
    }
}

impl IntoLayer for Vec<(u32, u32, u32, String)> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::L3S(self))
    }

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::Array(self.into_iter().map(|(i, j, k, s)| Value::Array(vec![Value::Int(i as i32), Value::Int(j as i32), Value::Int(k as i32), Value::String(s)])).collect())))
    }
}

impl IntoLayer for Vec<(u32, u32, u32, &'static str)> {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::L3S(self.iter().map(|(i, j, k, s)| (*i, *j, *k, s.to_string())).collect()))
    }

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        Ok(Layer::MetaLayer(Value::Array(self.iter().map(|(i, j, k, s)| Value::Array(vec![Value::Int(*i as i32), Value::Int(*j as i32), Value::Int(*k as i32), Value::String(s.to_string())])).collect())))
    }
}

#[derive(Debug,Clone,Serialize,Deserialize,Default,PartialEq)]
/// A layer description
pub struct LayerDesc {
    /// The type of the layer
    #[serde(rename = "type")]
    pub layer_type: LayerType,
    /// The name of the base layer for this layer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    /// The data type for this layer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<DataType>,
    /// The link types for this layer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_types: Option<Vec<String>>,
    /// The target layer for this layer if it is a link layer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// The default values for this layer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Layer>,
    /// The metadata for this layer
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

/// A layer in a document
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
    MetaLayer(Value)
}

impl Layer {
    /// Extract this a single idx as a div or element layer
    fn extract_1_idx<'a>(&'a self) -> TeangaResult<Box<dyn Iterator<Item = u32> + 'a>> {
        if let Layer::L1(indexes) = self {
            Ok(Box::new(indexes.iter().map(|i| *i)))
        } else if let Layer::L1S(indexes) = self {
            Ok(Box::new(indexes.iter().map(|(i, _)| *i)))
        } else if let Layer::L2(indexes) = self {
            Ok(Box::new(indexes.iter().map(|(i, _)| *i)))
        } else if let Layer::L2S(indexes) = self {
            Ok(Box::new(indexes.iter().map(|(i, _, _)| *i)))
        } else {
            Err(TeangaError::ModelError("Layer is not of type div or element".to_string()))
        }
    }

    // Extract this as two indexes as a span layer
    fn extract_2_idx<'a>(&'a self) -> TeangaResult<Box<dyn Iterator<Item = (u32, u32)> + 'a>> {
        if let Layer::L2(indexes) = self {
            Ok(Box::new(indexes.iter().map(|(i, j)| (*i, *j))))
        } else if let Layer::L2S(indexes) = self {
            Ok(Box::new(indexes.iter().map(|(i, j, _)| (*i, *j))))
        } else if let Layer::L3(indexes) = self {
            Ok(Box::new(indexes.iter().map(|(i, j, _)| (*i, *j))))
        } else if let Layer::L3S(indexes) = self {
            Ok(Box::new(indexes.iter().map(|(i, j, _, _)| (*i, *j))))
        } else {
            Err(TeangaError::ModelError("Layer is not of type span".to_string()))
        }
    }

    /// Get the indexes for the layer relative to a base layer
    ///
    /// # Arguments
    ///
    /// * `layer_name` - The name of this layer
    /// * `target_layer` - The name of the layer to get the indexes in
    /// * `doc` - The document to get the indexes from
    /// * `meta` - The metadata for the document
    ///
    /// # Returns
    ///
    /// A vector of indexes for the target layer
    pub fn indexes(&self, layer_name : &str, target_layer: &str, doc : &Document, 
        meta : &HashMap<String, LayerDesc>) -> TeangaResult<Vec<(usize, usize)>> {
        let layer_desc = meta.get(layer_name).ok_or_else(
            || TeangaError::LayerNotFoundError(layer_name.to_string()))?;
        match layer_desc.layer_type {
            LayerType::characters => {
                if let Layer::Characters(s) = self {
                    if target_layer != layer_name {
                        Err(TeangaError::IndexingError(layer_name.to_string(), target_layer.to_string()))
                    } else {
                        Ok((0..s.len()).zip(1..s.len() + 1).collect())
                    }
                } else {
                    Err(TeangaError::ModelError(
                        format!("Layer {} is not of type characters", layer_name)))
                }
            },
            LayerType::seq => {
                if target_layer == layer_name {
                    Ok((0..self.len()).zip(1..self.len() + 1).collect())
                } else {
                    if let Some(base_layer) = &layer_desc.base {
                        doc.indexes(base_layer, target_layer, meta)
                    } else {
                        Err(TeangaError::LayerNotFoundError(layer_desc.base.clone().unwrap()))
                    }
                }
            },
            LayerType::span => {
                let indexes = self.extract_2_idx()?;
                if target_layer == layer_name {
                    Ok((0..self.len()).zip(1..self.len() + 1).collect())
                } else if let Some(ref base_layer) = layer_desc.base {
                    if target_layer == base_layer {
                        Ok(indexes.map(|(i, j)| (i as usize, j as usize)).collect())
                    } else {
                        let subindexes = doc.indexes(&base_layer, target_layer, meta)?;
                        Ok(indexes.map(|(i, j)| (subindexes[i as usize].0, subindexes[(j - 1) as usize].1)).collect())
                    }
                } else {
                        Err(TeangaError::ModelError(
                            format!("Layer {} is not based on another layer", layer_name)))
                }
            },
            LayerType::div => {
                let indexes = self.extract_1_idx()?;
                if target_layer == layer_name {
                    Ok((0..self.len()).zip(1..self.len() + 1).collect())
                } else if let Some(ref base_layer) = layer_desc.base {
                    if target_layer == base_layer {
                        let end = doc.indexes(&base_layer, target_layer, meta)?.len();
                        let mut pairwise = Vec::new();
                        let mut last = None;
                        for i in indexes {
                            if let Some(l) = last {
                                pairwise.push((l, i as usize));
                            }
                            last = Some(i as usize);
                        }
                        if let Some(l) = last {
                            pairwise.push((l, end));
                        }
                        Ok(pairwise)
                    } else {
                        let subindexes = doc.indexes(&base_layer, target_layer, meta)?;
                        let mut pairwise = Vec::new();
                        let mut last : Option<usize> = None;
                        for i in indexes {
                            if let Some(l) = last {
                                pairwise.push((subindexes[l].0, subindexes[i as usize].0));
                            }
                            last = Some(i as usize);
                        }
                        if let Some(l) = last {
                            pairwise.push((subindexes[l].0, subindexes[l].1));
                        }
                        Ok(pairwise)
                    }
                } else {
                    Err(TeangaError::ModelError(
                        format!("Layer {} is not based on another layer", layer_name)))
                }
            }
            LayerType::element => {
                let indexes = self.extract_1_idx()?;
                if target_layer == layer_name {
                    Ok((0..self.len()).zip(1..self.len() + 1).collect())
                } else if let Some(ref base_layer) = layer_desc.base {
                    if target_layer == base_layer {
                        Ok(indexes.map(|i| (i as usize, i as usize + 1)).collect())
                    } else {
                        let subindexes = doc.indexes(&base_layer, target_layer, meta)?;
                        Ok(indexes.map(|i| subindexes[i as usize]).collect())
                    }
                } else {
                    Err(TeangaError::ModelError(
                        format!("Layer {} is not based on another layer", layer_name)))
                }
            }
        }
    }

    /// Get the data part of the layer
    ///
    /// # Arguments
    ///
    /// * `layer_desc` - The metadata for the layer
    ///
    /// # Returns
    ///
    /// The data of this layer
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

    /// Get the data and indexes for the layer
    pub fn indexes_data(&self, layer_name : &str, target_layer: &str, doc : &Document, 
        meta : &HashMap<String, LayerDesc>) -> TeangaResult<Vec<(usize, usize, TeangaData)>> {
        Ok(self.indexes(layer_name, target_layer, doc, meta)?
            .into_iter()
            .zip(self.data(meta.get(layer_name).ok_or_else(
                || TeangaError::LayerNotFoundError(layer_name.to_string()))?))
            .map(|(i, d)| (i.0, i.1, d))
            .collect())
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

/// The types of layers supported by Teanga
#[allow(non_camel_case_types)]
#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub enum LayerType {
    /// A plain text layer consisting of a single Unicode String
    characters,
    /// A sequence of data in one-to-one correspondence with the base layer
    seq,
    /// A division of the base layer into non-overlapping segments
    div,
    /// A reference to individual elements in the base layer
    element,
    /// A reference to spans of text in the base layer
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

/// The types of data supported by Teanga
#[derive(Debug,Clone,PartialEq)]
pub enum DataType {
    /// Plain string data
    String,
    /// A value for a set of enumerated values
    Enum(Vec<String>),
    /// A link to another annotation in this layer or another layer in the documnent
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

/// A data value in a Teanga document
#[derive(Debug,Clone,PartialEq,Eq,Hash,PartialOrd,Ord)]
pub enum TeangaData {
    None,
    String(String),
    Link(u32),
    TypedLink(u32, String)
}

impl Into<TeangaData> for String {
    fn into(self) -> TeangaData {
        TeangaData::String(self)
    }
}

impl Into<TeangaData> for &str {
    fn into(self) -> TeangaData {
        TeangaData::String(self.to_string())
    }
}

impl Into<TeangaData> for u32 {
    fn into(self) -> TeangaData {
        TeangaData::Link(self)
    }
}

impl Into<TeangaData> for (u32, String) {
    fn into(self) -> TeangaData {
        TeangaData::TypedLink(self.0, self.1)
    }
}
