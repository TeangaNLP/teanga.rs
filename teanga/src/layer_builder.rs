use crate::{Corpus, DocumentContent, IntoLayer, Value, DataType, Layer, LayerType, TeangaResult};
use std::collections::HashMap;

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
    pub fn add(self) -> TeangaResult<()> {
        self.corpus.add_layer_meta(
            self.name, self.layer_type, self.base, self.data, self.link_types, self.target, self.default, self.meta)
    }

    pub fn layer_type(mut self, layer_type: LayerType) -> Self {
        self.layer_type = layer_type;
        self
    }

    pub fn base(mut self, base: &str) -> Self {
        self.base = Some(base.to_string());
        self
    }

    pub fn data(mut self, data: DataType) -> Self {
        self.data = Some(data);
        self
    }

    pub fn link_types(mut self, link_types: Vec<String>) -> Self {
        self.link_types = Some(link_types);
        self
    }

    pub fn target(mut self, target: &str) -> Self {
        self.target = Some(target.to_string());
        self
    }

    pub fn default(mut self, default: Layer) -> Self {
        self.default = Some(default);
        self
    }

    pub fn meta(mut self, key: &str, value: Value) -> Self {
        self.meta.insert(key.to_string(), value);
        self
    }
}
