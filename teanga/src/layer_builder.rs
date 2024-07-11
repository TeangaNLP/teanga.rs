//! Utility for constructing layer metadata in a corpus.
//!
//! # Examples
//! ```rust
//! use teanga::{SimpleCorpus, Corpus, LayerType};
//! let mut corpus = SimpleCorpus::new();
//! corpus.build_layer("text").add();
//! corpus.build_layer("words")
//!   .base("text")
//!   .layer_type(LayerType::span)
//!   .add();
//! ```
use crate::{Corpus, DocumentContent, IntoLayer, Value, DataType, Layer, LayerType, TeangaResult};
use std::collections::HashMap;

/// Build a layer in a corpus
///
/// # Arguments
///
/// * `corpus` - The corpus to add the layer to
/// * `name` - The name of the layer
///
/// # Returns
///
/// A builder object for the layer
pub fn build_layer<'a, LayerStorage : IntoLayer, Content : DocumentContent<LayerStorage>, ICorpus : Corpus<LayerStorage=LayerStorage,Content=Content>>(corpus :&'a mut ICorpus, name: &str) -> LayerBuilderImpl<'a, LayerStorage, Content, ICorpus> {
    LayerBuilderImpl {
        corpus,
        name: name.to_string(),
        layer_type: LayerType::characters,
        base: None,
        data: None,
        link_types: None,
        target: None,
        default: None,
        meta: HashMap::new()
    }
}

/// A layer metadata builder
pub struct LayerBuilderImpl<'a, LayerStorage : IntoLayer, Content : DocumentContent<LayerStorage>, ICorpus : Corpus<LayerStorage=LayerStorage,Content=Content>> {
    corpus: &'a mut ICorpus,
    name: String,
    layer_type: LayerType,
    base: Option<String>,
    data: Option<DataType>,
    link_types: Option<Vec<String>>,
    target: Option<String>,
    default: Option<Layer>,
    meta: HashMap<String, Value>
}

impl<'a, LayerStorage : IntoLayer, Content : DocumentContent<LayerStorage>, ICorpus : Corpus<LayerStorage=LayerStorage,Content=Content>> LayerBuilderImpl<'a, LayerStorage, Content, ICorpus> {
    /// Commit the layer metadata to the corpus
    pub fn add(self) -> TeangaResult<()> {
        self.corpus.add_layer_meta(
            self.name, self.layer_type, self.base, self.data, self.link_types, self.target, self.default, self.meta)
    }

    /// Set the layer type
    pub fn layer_type(mut self, layer_type: LayerType) -> Self {
        self.layer_type = layer_type;
        self
    }

    /// Set the base layer
    pub fn base(mut self, base: &str) -> Self {
        self.base = Some(base.to_string());
        self
    }

    /// Set the data type
    pub fn data(mut self, data: DataType) -> Self {
        self.data = Some(data);
        self
    }

    /// Set the link types
    pub fn link_types(mut self, link_types: Vec<String>) -> Self {
        self.link_types = Some(link_types);
        self
    }

    /// Set the target layer
    pub fn target(mut self, target: &str) -> Self {
        self.target = Some(target.to_string());
        self
    }

    /// Set the default values for the layer
    pub fn default(mut self, default: Layer) -> Self {
        self.default = Some(default);
        self
    }

    /// Set a metadata key/value for the layer
    pub fn meta(mut self, key: &str, value: Value) -> Self {
        self.meta.insert(key.to_string(), value);
        self
    }
}
