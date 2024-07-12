use pyo3::prelude::*;
use std::collections::HashMap;
use crate::{PyLayerDesc, PyLayerType, PyValue, PyDataType, PyRawLayer};
use pyo3::types::PyByteArray;
use teanga::{LayerDesc, teanga_id, Document, Index, 
    bytes_to_doc, doc_content_to_bytes};
use teanga::SmazCompression;

#[pyclass]
pub struct TCFPyCorpus {
    pub meta : HashMap<String, LayerDesc>,
    pub meta_keys : Vec<String>,
    pub data : Py<PyByteArray>,
    pub offsets : HashMap<String, usize>,
    pub order : Vec<String>,
    pub index : TCFPyIndex
}

#[pymethods]
impl TCFPyCorpus {
    #[new]
    /// Create a new corpus
    ///
    /// # Arguments
    /// * `path` - The path to the database
    ///
    /// # Returns
    /// A new corpus object
    ///
    pub fn new<'p>(py : Python<'p>) -> PyResult<TCFPyCorpus> {
        Ok(TCFPyCorpus {
            meta: HashMap::new(),
            meta_keys: Vec::new(),
            order: Vec::new(),
            data: PyByteArray::new_bound(py, &[0u8; 0]).into(),
            offsets: HashMap::new(),
            index : TCFPyIndex::new()
        })
    }

    #[pyo3(name="add_layer_meta")]
    fn add_layer_meta(&mut self, name: String, layer_type: PyLayerType,
        meta: HashMap<String, PyValue>,
        base: Option<String>, data: Option<PyDataType>, link_types: Option<Vec<String>>, 
        target: Option<String>, default: Option<PyRawLayer>) -> PyResult<()> {
        let layer_desc = LayerDesc::new(&name, 
            layer_type.0, base, 
            data.map(|x| x.0), 
            link_types, target, 
            default.map(|x| x.0), 
            meta.into_iter().map(|(k, v)| (k, v.val())).collect()
            ).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        self.meta.insert(name, layer_desc);
        self.meta_keys = self.meta.keys().cloned().collect();
        self.meta_keys.sort();
            Ok(())
    }

    pub fn add_doc<'p>(&mut self, py : Python<'p>, 
        doc: HashMap<String, PyRawLayer>) -> PyResult<()> {
        let document = Document::new(doc.clone(), &self.meta).
            map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        let id = teanga_id(&self.order, &document);
        let mut index = self.index.to_index();
        let data = doc_content_to_bytes(doc,
            &self.meta_keys,
            &self.meta, &mut index, &SmazCompression)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        self.order.push(id.clone());
        let d = self.data.bind(py);
        let n = d.len();
        d.resize(n + data.len())?;
        unsafe {
            d.as_bytes_mut()[n..].copy_from_slice(&data);
        }
        self.index = TCFPyIndex::from_index(index);
        Ok(())
    }

    pub fn add_docs<'p>(&mut self, py : Python<'p>, docs: Vec<HashMap<String, PyRawLayer>>) -> PyResult<()> {
        for doc in docs {
            self.add_doc(py, doc)?;
        }
        Ok(())
    }   

    pub fn get_doc_by_id<'p>(&mut self, py : Python<'p>, id : &str) -> PyResult<HashMap<String, PyRawLayer>> {
        let mut index = self.index.to_index();
        if let Some(i) = self.offsets.get(id) {
            let data = self.data.bind(py);
            // TODO: Index should be initialized already!
            let doc = unsafe {
                bytes_to_doc(data.as_bytes(), *i,
                    &self.meta_keys, &self.meta, &mut index,
                    &SmazCompression)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?
            };
            self.index = TCFPyIndex::from_index(index);
            Ok(doc.content.iter().map(|(k, v)| (k.clone(), PyRawLayer(v.clone()))).collect())
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("No document with ID {}", id)))
        }
    }

    #[getter]
    fn meta(&self) -> PyResult<HashMap<String, PyLayerDesc>> {
        Ok(self.meta.iter().map(|(k, v)| (k.clone(), PyLayerDesc(v.clone()))).collect())
    }

    #[setter]
    fn set_meta(&mut self, meta: HashMap<String, PyLayerDesc>) -> PyResult<()> {
        self.meta = meta.into_iter().map(|(k, v)| (k, v.0)).collect();
        Ok(())
    }

    #[getter]
    fn order(&self) -> PyResult<Vec<String>> {
        Ok(self.order.clone())
    }

    #[setter]
    fn set_order(&mut self, _order: Vec<String>) -> PyResult<()> {
        Err(PyErr::new::<pyo3::exceptions::PyAttributeError, _>("Order is read-only"))
    }

    fn update_doc<'p>(&mut self, _py : Python<'p>, _id : &str, _content: HashMap<String, PyRawLayer>) -> PyResult<String> {
        panic!("Updating documents not yet supported in TCF")
    }

}

#[pyclass]
pub struct TCFPyIndex {
    pub keys : HashMap<String, u32>,
    pub key_strs : Vec<String>,
    pub lru : Vec<String>
}

impl TCFPyIndex {
    pub fn new() -> TCFPyIndex {
        TCFPyIndex {
            keys: HashMap::new(),
            key_strs: Vec::new(),
            lru : Vec::new()
        }
    }

    pub fn to_index(&self) -> Index {
        Index::from_values(self.keys.clone(), self.key_strs.clone(),
            self.lru.clone())
    }

    pub fn from_index(index : Index) -> TCFPyIndex {
        let (keys, key_strs, lru) = index.into_values();
        TCFPyIndex {
            keys,
            key_strs,
            lru
        }
    }
}
