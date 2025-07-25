// Purpose: Rust impl of the TeangaDB Python module.
// Author: John P. McCrae
// License: Apache 2.0
use pyo3::prelude::*;
use ::teanga::disk_corpus::{DiskCorpus, PathAsDB};
use ::teanga::{LayerDesc, LayerType, DataType, Value, Layer, Corpus, ReadableCorpus, SimpleCorpus, DocumentContent, Document};
use std::collections::HashMap;

mod cuac_py;
mod query;

use cuac_py::CuacPyCorpus;
use ::teanga::{TeangaResult, IntoLayer, WriteableCorpus, TeangaError};

pub enum PyCorpus {
    Disk(DiskCorpus<PathAsDB>),
    Mem(SimpleCorpus)
}

impl ReadableCorpus for PyCorpus {
    fn iter_docs<'a>(&'a self) -> Box<dyn Iterator<Item = TeangaResult<Document>> + 'a> {
        match self {
            PyCorpus::Disk(corpus) => corpus.iter_docs(),
            PyCorpus::Mem(corpus) => corpus.iter_docs()
        }
    }

    fn iter_doc_ids<'a>(&'a self) -> Box<dyn Iterator<Item = TeangaResult<(String, Document)>> + 'a> {
        match self {
            PyCorpus::Disk(corpus) => corpus.iter_doc_ids(),
            PyCorpus::Mem(corpus) => corpus.iter_doc_ids()
        }
    }

    fn get_meta(&self) -> &HashMap<String, LayerDesc> {
        match self {
            PyCorpus::Disk(corpus) => corpus.get_meta(),
            PyCorpus::Mem(corpus) => corpus.get_meta()
        }
    }
}

impl WriteableCorpus for PyCorpus {
    fn set_meta(&mut self, meta: HashMap<String, LayerDesc>) -> TeangaResult<()> {
        match self {
            PyCorpus::Disk(corpus) => corpus.set_meta(meta),
            PyCorpus::Mem(corpus) => corpus.set_meta(meta)
        }
    }

    fn set_order(&mut self, order: Vec<String>) -> TeangaResult<()> {
        match self {
            PyCorpus::Disk(corpus) => corpus.set_order(order),
            PyCorpus::Mem(corpus) => corpus.set_order(order)
        }
    }

    fn add_doc<D: IntoLayer, DC: DocumentContent<D>>(&mut self, doc: DC) -> TeangaResult<String> {
        match self {
            PyCorpus::Disk(corpus) => corpus.add_doc(doc),
            PyCorpus::Mem(corpus) => corpus.add_doc(doc)
        }
    }
}

impl Corpus for PyCorpus {
    fn add_layer_meta(&mut self, name: String, layer_type: LayerType, 
        base: Option<String>, data: Option<DataType>, link_types: Option<Vec<String>>, 
        target: Option<String>, default: Option<Layer>,
        meta: HashMap<String, Value>) -> TeangaResult<()> {
        match self {
            PyCorpus::Disk(corpus) => corpus.add_layer_meta(name, layer_type, base, data, link_types, target, default, meta),
            PyCorpus::Mem(corpus) => corpus.add_layer_meta(name, layer_type, base, data, link_types, target, default, meta)
        }
    }

    fn update_doc<D : IntoLayer, DC: DocumentContent<D>>(&mut self, id : &str, content : DC) -> TeangaResult<String> {
        match self {
            PyCorpus::Disk(corpus) => corpus.update_doc(id, content),
            PyCorpus::Mem(corpus) => corpus.update_doc(id, content)
        }
    }

    fn remove_doc(&mut self, id : &str) -> TeangaResult<()> {
        match self {
            PyCorpus::Disk(corpus) => corpus.remove_doc(id),
            PyCorpus::Mem(corpus) => corpus.remove_doc(id)
        }
    }

    fn get_doc_by_id(&self, id : &str) -> TeangaResult<Document> {
        match self {
            PyCorpus::Disk(corpus) => corpus.get_doc_by_id(id),
            PyCorpus::Mem(corpus) => corpus.get_doc_by_id(id)
        }
    }

    fn get_docs(&self) -> Vec<String> {
        match self {
            PyCorpus::Disk(corpus) => corpus.get_docs(),
            PyCorpus::Mem(corpus) => corpus.get_docs()
        }
    }

    fn get_order(&self) -> &Vec<String> {
        match self {
            PyCorpus::Disk(corpus) => corpus.get_order(),
            PyCorpus::Mem(corpus) => corpus.get_order()
        }
    }
}

#[pyclass(name="Corpus")]
/// A corpus object
pub struct PyDiskCorpus(PyCorpus);

#[pymethods]
impl PyDiskCorpus {

    #[new]
    /// Create a new corpus
    ///
    /// # Arguments
    /// * `path` - The path to the database
    ///
    /// # Returns
    /// A new corpus object
    ///
    pub fn new(path : &str) -> PyResult<PyDiskCorpus> {
        if path == "<memory>" {
            return Ok(PyDiskCorpus(PyCorpus::Mem(SimpleCorpus::new())));
        } else {
            Ok(PyDiskCorpus(
                    PyCorpus::Disk(DiskCorpus::new_path_db(path))))
        }
    }

    #[pyo3(name="add_layer_meta")]
    fn add_layer_meta(&mut self, name: String, layer_type: PyLayerType,
        meta: HashMap<String, PyValue>,
        base: Option<String>, data: Option<PyDataType>, link_types: Option<Vec<String>>, 
        target: Option<String>, default: Option<PyRawLayer>) -> PyResult<()> {
        Ok(self.0.add_layer_meta(name, layer_type.0, base, 
                data.map(|x| x.0), link_types, target, 
                default.map(|x| x.0),
                meta.into_iter().map(|(k,v)| (k, v.val())).collect::<HashMap<String, Value>>())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?)
    }

    pub fn add_doc(&mut self, doc: HashMap<String, PyRawLayer>) -> PyResult<String> {
        let id = self.0.add_doc(doc.iter().map(|(k,v)| (k.clone(), v.0.clone())).collect::<HashMap<String, Layer>>())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        Ok(id)
    }

    pub fn add_docs(&mut self, docs: Vec<HashMap<String, PyRawLayer>>) -> PyResult<()> {
        self.0.add_docs(docs.into_iter().map(|doc| doc.iter().map(|(k,v)| (k.clone(), v.0.clone())).collect::<HashMap<String, Layer>>())
            .collect::<Vec<HashMap<String, Layer>>>())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        Ok(())
    }   

    pub fn get_doc_by_id(&self, id : &str) -> PyResult<HashMap<String, PyRawLayer>> {
        Ok(self.0.get_doc_by_id(id)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?
            .into_iter().map(
                |(k, v)| (k.clone(), PyRawLayer(v.clone()))).collect())
    }

    #[getter]
    fn meta(&self) -> PyResult<HashMap<String, PyLayerDesc>> {
        Ok(self.0.get_meta().iter().map(|(k,v)| (k.clone(), PyLayerDesc(v.clone()))).collect())
    }

    #[setter]
    fn set_meta(&mut self, meta: HashMap<String, PyLayerDesc>) -> PyResult<()> {
        self.0.set_meta(meta.iter().map(|(k,v)| (k.clone(), v.0.clone())).collect())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        Ok(())
    }

    #[getter]
    fn order(&self) -> PyResult<Vec<String>> {
        Ok(self.0.get_order().clone())
    }

    #[setter]
    fn set_order(&mut self, order: Vec<String>) -> PyResult<()> {
        self.0.set_order(order)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        Ok(())
    }

    fn update_doc(&mut self, id : &str, content: HashMap<String, PyRawLayer>) -> PyResult<String> {
        self.0.update_doc(id, content.iter().map(|(k,v)| (k.clone(), v.0.clone())).collect::<HashMap<String, Layer>>())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))
    }

    fn search(&self, query : query::PyQuery) -> PyResult<Vec<String>> {
        let mut vec = Vec::new();
        for result in self.0.search(query.0) {
            vec.push(result.
                map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?.0);
        }
        Ok(vec)
    }
}

#[pyclass]
#[derive(Debug,Clone)]
pub struct PyLayerDesc(LayerDesc);

#[pymethods]
impl PyLayerDesc {
    #[getter]
    fn layer_type(&self) -> PyResult<PyLayerType> {
        Ok(PyLayerType(self.0.layer_type.clone()))
    }

    #[getter]
    fn base(&self) -> PyResult<Option<String>> {
        Ok(self.0.base.clone())
    }

    #[getter]
    fn data(&self) -> PyResult<Option<PyDataType>> {
        Ok(self.0.data.clone().map(|x| PyDataType(x)))
    }

    #[getter]
    fn link_types(&self) -> PyResult<Option<Vec<String>>> {
        Ok(self.0.link_types.clone())
    }

    #[getter]
    fn target(&self) -> PyResult<Option<String>> {
        Ok(self.0.target.clone())
    }

    #[getter]
    fn default(&self) -> PyResult<Option<PyRawLayer>> {
        Ok(self.0.default.clone().map(|x| PyRawLayer(x)))
    }

    #[getter]
    fn meta(&self) -> PyResult<HashMap<String, PyValue>> {
        Ok(self.0.meta.iter().map(|(k,v)| (k.clone(), val_to_pyval(v.clone()))).collect())
    }

    fn __repr__(&self) -> String {
        let data = match &self.0.data {
            Some(DataType::Enum(v)) => format!("{:?}", v),
            Some(DataType::String) => "string".to_string(),
            Some(DataType::Link) => "link".to_string(),
            None => "None".to_string()
        };
        let base = match &self.0.base {
            Some(v) => format!("'{:?}'", v),
            None => "None".to_string()
        };
        let target = match &self.0.target {
            Some(v) => format!("'{:?}'", v),
            None => "None".to_string()
        };
        let link_types = match &self.0.link_types {
            Some(v) => format!("{:?}", v),
            None => "None".to_string()
        };
        let default = match &self.0.default {
            Some(v) => format!("{:?}", v),
            None => "None".to_string()
        };
        let meta = format!("{{{}}}", self.0.meta.iter().map(|(k,v)| format!("'{}': {:?}", k, v)).collect::<Vec<String>>().join(", "));
        format!("LayerDesc(layer_type='{}', base={}, data={}, link_types={}, target={}, default={}, meta={})",
            self.0.layer_type, 
            base,
            data,
            link_types,
            target,
            default,
            meta)
    }
}

#[pyfunction]
fn layerdesc_from_dict(dict: HashMap<String, Bound<PyAny>>) -> PyResult<PyLayerDesc> {
    let layer_type = match dict.get("layer_type") {
        Some(pyval) => pyval.extract::<PyLayerType>()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?,
        None => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'layer_type' key".to_string()))
    };
    let base = dict.get("base")
        .and_then(|v| v.extract::<PyValue>().ok())
        .and_then(|v| match v {
            PyValue::String(s) => Some(s),
            _ => None,
        });
    let data = dict.get("data")
        .and_then(|v| v.extract::<PyValue>().ok())
        .and_then(|v| match v {
            PyValue::String(_) => Some(DataType::String),
            PyValue::Array(arr) => Some(DataType::Enum(arr.into_iter().filter_map(|x| match x {
                PyValue::String(s) => Some(s),
                _ => None,
            }).collect())),
            _ => None,
        });
    let link_types = dict.get("link_types")
        .and_then(|v| v.extract::<PyValue>().ok())
        .and_then(|v| match v {
            PyValue::Array(arr) => Some(arr.into_iter().filter_map(|x| match x {
                PyValue::String(s) => Some(s),
                _ => None,
            }).collect()),
            _ => None,
        });
    let target = dict.get("target")
        .and_then(|v| v.extract::<PyValue>().ok())
        .and_then(|v| match v {
            PyValue::String(s) => Some(s),
            _ => None,
        });
    let default = dict.get("default")
        .and_then(|v| v.extract::<PyValue>().ok())
        .and_then(|v| match v {
            PyValue::Object(_) | PyValue::String(_) | PyValue::Array(_) => {
                Some(PyRawLayer(Layer::MetaLayer(Some(v.clone().val()))).0)
            }
            _ => None,
        });
    let meta = dict.into_iter().filter_map(|(k, v)| {
        if k == "layer_type" || k == "base" || k == "data" || k == "link_types" || k == "target" || k == "default" || k == "meta" {
            None
        } else {
            v.extract::<PyValue>().ok().map(|pyv| (k, pyv.val()))
        }
    }).collect();
    Ok(PyLayerDesc(LayerDesc {
        layer_type: layer_type.0,
        base,
        data,
        link_types,
        target,
        default,
        meta
    }))
}


#[derive(Debug,Clone,PartialEq, FromPyObject)]
/// Any valid JSON/YAML value
pub enum PyValue {
    Bool(bool),
    Int(i32),
    Float(f64),
    String(String),
    Array(Vec<PyValue>),
    Object(HashMap<String, PyValue>)
}

impl PyValue {
    fn val(self) -> Value {
        match self {
            PyValue::Bool(val) => Value::Bool(val),
            PyValue::Int(val) => Value::Int(val),
            PyValue::Float(val) => Value::Float(val),
            PyValue::String(val) => Value::String(val),
            PyValue::Array(val) => Value::Array(val.into_iter().map(PyValue::val).collect()),
            PyValue::Object(val) => Value::Object(val.into_iter().map(|(k,v)| (k, v.val())).collect())
        }
    }
}

fn val_to_pyval(val: Value) -> PyValue {
    match val {
        Value::Bool(val) => PyValue::Bool(val),
        Value::Int(val) => PyValue::Int(val),
        Value::Float(val) => PyValue::Float(val),
        Value::String(val) => PyValue::String(val),
        Value::Array(val) => PyValue::Array(val.into_iter().map(val_to_pyval).collect()),
        Value::Object(val) => PyValue::Object(val.into_iter().map(|(k,v)| (k, val_to_pyval(v))).collect())
    }
}

impl<'py> IntoPyObject<'py> for PyValue {
    type Target = PyAny; 
    type Output = Bound<'py, Self::Target>; 
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            PyValue::Bool(val) => val.into_bound_py_any(py),
            PyValue::Int(val) => val.into_bound_py_any(py),
            PyValue::Float(val) => val.into_bound_py_any(py),
            PyValue::String(val) => val.into_bound_py_any(py),
            PyValue::Array(val) => val.into_bound_py_any(py),
            PyValue::Object(val) => val.into_bound_py_any(py)
        }
    }
}

#[derive(Debug,Clone,PartialEq)]
pub struct PyRawLayer(Layer);

impl<'py> IntoPyObject<'py> for PyRawLayer {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            Layer::Characters(val) => val.into_bound_py_any(py),
            Layer::L1(val) => val.into_bound_py_any(py),
            Layer::L2(val) => val.into_bound_py_any(py),
            Layer::L3(val) => val.into_bound_py_any(py),
            Layer::LS(val) => val.into_bound_py_any(py),
            Layer::L1S(val) => val.into_bound_py_any(py),
            Layer::L2S(val) => val.into_bound_py_any(py),
            Layer::L3S(val) => val.into_bound_py_any(py),
            Layer::MetaLayer(val) => val.map(|v| val_to_pyval(v)).into_bound_py_any(py),
        }
    }
}

impl IntoLayer for PyRawLayer {
    fn into_layer(self, _meta: &LayerDesc) -> TeangaResult<Layer> {
        Ok(self.0)
    }

    fn into_meta_layer(self) -> TeangaResult<Layer> {
        match self {
            PyRawLayer(Layer::MetaLayer(val)) => Ok(Layer::MetaLayer(val)),
            _ => Err(TeangaError::ModelError("Not a meta layer".to_string()))
        }
    }
}

impl <'py> FromPyObject<'py> for PyRawLayer {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<PyRawLayer> {
        if let Ok(layer) = ob.extract::<String>() {
            Ok(PyRawLayer(Layer::Characters(layer)))
        } else if let Ok(layer) = ob.extract::<Vec<u32>>() {
            Ok(PyRawLayer(Layer::L1(layer)))
        } else if let Ok(layer) = ob.extract::<Vec<(u32, u32)>>() {
            Ok(PyRawLayer(Layer::L2(layer)))
        } else if let Ok(layer) = ob.extract::<Vec<(u32, u32, u32)>>() {
            Ok(PyRawLayer(Layer::L3(layer)))
        } else if let Ok(layer) = ob.extract::<Vec<String>>() {
            Ok(PyRawLayer(Layer::LS(layer)))
        } else if let Ok(layer) = ob.extract::<Vec<(u32, String)>>() {
            Ok(PyRawLayer(Layer::L1S(layer)))
        } else if let Ok(layer) = ob.extract::<Vec<(u32, u32, String)>>() {
            Ok(PyRawLayer(Layer::L2S(layer)))
        } else if let Ok(layer) = ob.extract::<Vec<Vec<U32OrString>>>() {
            Ok(PyRawLayer(vecus2rawlayer(layer).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?))
        } else if let Ok(layer) = ob.extract::<PyValue>() {
            Ok(PyRawLayer(Layer::MetaLayer(Some(layer.val()))))
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown layer type {}", ob.extract::<String>()?)))
        }
    }
}
//impl FromPyObject<'_> for PyRawLayer {
//    fn extract(v: &PyAny) -> PyResult<Self> {
//        if let Ok(layer) = v.extract::<String>() {
//            Ok(PyRawLayer(Layer::Characters(layer)))
//        } else if let Ok(layer) = v.extract::<PyCell<Vec<u32>>>() {
//            Ok(PyRawLayer(Layer::L1(layer)))
//        } else if let Ok(layer) = v.extract::<Vec<(u32, u32)>>() {
//            Ok(PyRawLayer(Layer::L2(layer)))
//        } else if let Ok(layer) = v.extract::<Vec<(u32, u32, u32)>>() {
//            Ok(PyRawLayer(Layer::L3(layer)))
//        } else if let Ok(layer) = v.extract::<Vec<String>>() {
//            Ok(PyRawLayer(Layer::LS(layer)))
//        } else if let Ok(layer) = v.extract::<Vec<(u32, String)>>() {
//            Ok(PyRawLayer(Layer::L1S(layer)))
//        } else if let Ok(layer) = v.extract::<Vec<(u32, u32, String)>>() {
//            Ok(PyRawLayer(Layer::L2S(layer)))
//        } else if let Ok(layer) = v.extract::<Vec<Vec<U32OrString>>>() {
//            Ok(PyRawLayer(vecus2rawlayer(layer).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?))
//        } else if let Ok(layer) = v.extract::<PyValue>() {
//            Ok(PyRawLayer(Layer::MetaLayer(Some(layer.val()))))
//        } else {
//            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
//                format!("Unknown layer type {}", v.extract::<String>()?)))
//        }
//    }
//}

#[derive(Debug,Clone,PartialEq, FromPyObject)]
pub enum U32OrString {
    U32(u32),
    String(String)
}

fn vecus2rawlayer(v : Vec<Vec<U32OrString>>) -> Result<Layer, String> {
    if v.len() == 0 {
        return Err("Empty layer".to_string());
    }
    if v[0].len() == 1 {
        match v[0][0] {
            U32OrString::U32(_) => 
                Ok(Layer::L1(vecus2vecu32(v)?)),
            U32OrString::String(_) =>
                Ok(Layer::LS(vecus2vecstr(v)?))
        }
    } else if v[0].len() == 2 {
        match v[0][0] {
            U32OrString::U32(_) =>
                match v[0][1] {
                    U32OrString::U32(_) => 
                        Ok(Layer::L2(vecus2vecu32u32(v)?)),
                    U32OrString::String(_) => 
                        Ok(Layer::L1S(vecus2vecu32str(v)?))
                },
            U32OrString::String(_) =>
                Err(format!("str in first position of layer"))
        }
    } else if v[0].len() == 3 {
        match v[0][0] {
            U32OrString::U32(_) =>
                match v[0][1] {
                    U32OrString::U32(_) => 
                        match v[0][2] {
                            U32OrString::U32(_) => 
                                Ok(Layer::L3(vecus2vecu32u32u32(v)?)),
                            U32OrString::String(_) => 
                                Ok(Layer::L2S(vecus2vecu32u32str(v)?)
                                )
                        },
                    U32OrString::String(_) => 
                        Err(format!("str in second position of layer"))
                },
            U32OrString::String(_) =>
                Err(format!("str in first position of layer"))
        }
    } else if v[0].len() == 4 {
        match v[0][0] {
            U32OrString::U32(_) =>
                match v[0][1] {
                    U32OrString::U32(_) => 
                        match v[0][2] {
                            U32OrString::U32(_) => 
                                match v[0][3] {
                                    U32OrString::U32(_) => 
                                        Err(format!("u32 in fourth position of layer")),
                                    U32OrString::String(_) => 
                                        Ok(Layer::L3S(vecus2vecu32u32u32str(v)?))
                                },
                            U32OrString::String(_) => 
                                Err(format!("str in third position of layer"))
                        },
                    U32OrString::String(_) => 
                        Err(format!("str in second position of layer"))
                },
            U32OrString::String(_) =>
                Err(format!("str in first position of layer"))
        }
    } else {
        Err("Unsupported length of layer".to_string())
    }
}

fn vecus2vecu32(vs: Vec<Vec<U32OrString>>) -> Result<Vec<u32>, String> {
    let mut v2 = Vec::new();
    for v in vs {
        if v.len() != 1 {
            return Err("Mixed length of annotations".to_string());
        }
        match v[0] {
            U32OrString::U32(x) => v2.push(x),
            U32OrString::String(_) => 
                return Err("Mixture of int and str".to_string())
        }
    }
    Ok(v2)
}

fn vecus2vecu32u32(vs: Vec<Vec<U32OrString>>) -> Result<Vec<(u32, u32)>, String> {
    let mut v2 = Vec::new();
    for v in vs {
        if v.len() != 2 {
            return Err("Mixed length of annotations".to_string());
        }
        match &v[0] {
            U32OrString::U32(x) => {
                match &v[1] {
                    U32OrString::U32(y) => v2.push((*x, *y)),
                    U32OrString::String(_) => 
                        return Err("Mixture of int and str".to_string())
                }
            }
            U32OrString::String(_) => 
                return Err("Mixture of int and str".to_string())
        }
    }
    Ok(v2)
}

fn vecus2vecu32u32u32(vs: Vec<Vec<U32OrString>>) -> Result<Vec<(u32, u32, u32)>, String> {
    let mut v2 = Vec::new();
    for v in vs {
        if v.len() != 3 {
            return Err("Mixed length of annotations".to_string());
        }
        match &v[0] {
            U32OrString::U32(x) => 
                match &v[1] {
                    U32OrString::U32(y) => 
                        match &v[2] {
                            U32OrString::U32(z) => v2.push((*x, *y, *z)),
                            U32OrString::String(_) => 
                                return Err("Mixture of int and str".to_string())
                        },
                    U32OrString::String(_) => 
                        return Err("Mixture of int and str".to_string())
                },
            U32OrString::String(_) => 
                return Err("Mixture of int and str".to_string())
        }
    }
    Ok(v2)
}

fn vecus2vecstr(vs: Vec<Vec<U32OrString>>) -> Result<Vec<String>, String> {
    let mut v2 = Vec::new();
    for v in vs {
        let mut i = v.into_iter();
        match i.next() {
            Some(U32OrString::U32(_)) => 
                return Err("Mixture of int and str".to_string()),
            Some(U32OrString::String(x)) => v2.push(x),
            None => return Err("Mixed length of annotations".to_string())
        }
    }
    Ok(v2)
}

fn vecus2vecu32str(vs: Vec<Vec<U32OrString>>) -> Result<Vec<(u32, String)>, String> {
    let mut v2 = Vec::new();
    for v in vs {
        let mut i = v.into_iter();
        match i.next() {
            Some(U32OrString::U32(x)) => 
                match i.next() {
                    Some(U32OrString::U32(_)) => 
                        return Err("Mixture of int and str".to_string()),
                    Some(U32OrString::String(y)) => v2.push((x, y)),
                    None => return Err("Mixed length of annotations".to_string()
                    )
                },
            Some(U32OrString::String(_)) => 
                return Err("Mixture of int and str".to_string()),
                None => return Err("Mixed length of annotations".to_string())
        }
    }
    Ok(v2)
}

fn vecus2vecu32u32str(vs: Vec<Vec<U32OrString>>) -> Result<Vec<(u32, u32, String)>, String> {
    let mut v2 = Vec::new();
    for v in vs {
        let mut i = v.into_iter();
        match i.next() {
            Some(U32OrString::U32(x)) => {
                match i.next() {
                    Some(U32OrString::U32(y)) => 
                        match i.next() {
                            Some(U32OrString::U32(_)) => 
                                return Err("Mixture of int and str".to_string()),
                            Some(U32OrString::String(z)) => v2.push((x, y, z)),
                    None => return Err("Mixed length of annotations".to_string())

                        },
                    Some(U32OrString::String(_)) => 
                        return Err("Mixture of int and str".to_string()),
                    None => return Err("Mixed length of annotations".to_string())

                }
            },
            Some(U32OrString::String(_)) => 
                return Err("Mixture of int and str".to_string()),
                    None => return Err("Mixed length of annotations".to_string())

        }
    }
    Ok(v2)
}

fn vecus2vecu32u32u32str(vs: Vec<Vec<U32OrString>>) -> Result<Vec<(u32, u32, u32, String)>, String> {
    let mut v2 = Vec::new();
    for v in vs {
        let mut i = v.into_iter();
        match i.next() {
            Some(U32OrString::U32(x)) => {
                match i.next() {
                    Some(U32OrString::U32(y)) => 
                        match i.next() {
                            Some(U32OrString::U32(z)) => 
                                match i.next() {
                                    Some(U32OrString::U32(_)) => 
                                        return Err("Mixture of int and str".to_string()),
                                    Some(U32OrString::String(w)) => v2.push((x, y, z, w)),
                    None => return Err("Mixed length of annotations".to_string())

                                },
                            Some(U32OrString::String(_)) => 
                                return Err("Mixture of int and str".to_string()),
                    None => return Err("Mixed length of annotations".to_string())

                        },
                    Some(U32OrString::String(_)) => 
                        return Err("Mixture of int and str".to_string()),
                    None => return Err("Mixed length of annotations".to_string())

                }
            },
            Some(U32OrString::String(_)) => 
                return Err("Mixture of int and str".to_string()),
                    None => return Err("Mixed length of annotations".to_string())

        }
    }
    Ok(v2)
}

#[derive(Debug,Clone,PartialEq)]
pub struct PyLayerType(LayerType);

impl <'py> FromPyObject<'py> for PyLayerType {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<PyLayerType> {
        match ob.extract::<String>()?.to_lowercase().as_str() {
            "characters" => Ok(PyLayerType(LayerType::characters)),
            "seq" => Ok(PyLayerType(LayerType::seq)),
            "div" => Ok(PyLayerType(LayerType::div)),
            "element" => Ok(PyLayerType(LayerType::element)),
            "span" => Ok(PyLayerType(LayerType::span)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown layer type {}", ob.extract::<String>()?)))
        }
    }
}

use pyo3::IntoPyObjectExt;

impl<'py> IntoPyObject<'py> for PyLayerType {
    type Target = PyAny; // the Python type
    type Output = Bound<'py, Self::Target>; // in most cases this will be `Bound`
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let name = match self.0 {
            LayerType::characters => "characters",
            LayerType::seq => "seq",
            LayerType::div => "div",
            LayerType::element => "element",
            LayerType::span => "span"
        };
        name.into_bound_py_any(py)
    }
}


#[derive(Debug,Clone,PartialEq)]
pub struct PyDataType(DataType);

impl <'py> FromPyObject<'py> for PyDataType {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<PyDataType> {
        match ob.extract::<Vec<String>>() {
            Ok(vals) => return Ok(PyDataType(DataType::Enum(vals))),
            Err(_) => ()
        };
        match ob.extract::<String>()?.to_lowercase().as_str() {
            "string" => Ok(PyDataType(DataType::String)),
            "link" => Ok(PyDataType(DataType::Link)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown data type {}", ob.extract::<String>()?)))
        }
    }
}

impl<'py> IntoPyObject<'py> for PyDataType {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            DataType::String => "string".into_bound_py_any(py),
            DataType::Enum(v) => v.into_bound_py_any(py),
            DataType::Link => "link".into_bound_py_any(py),
        }
    }
}

#[pyfunction]
fn read_corpus_from_json_string(s : &str, path : &str) -> PyResult<PyDiskCorpus> {
    if path == "<memory>" {
        let mut corpus = SimpleCorpus::new();
        ::teanga::read_json(s.as_bytes(), &mut corpus).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        Ok(PyDiskCorpus(PyCorpus::Mem(corpus)))
    } else {
        let mut corpus = DiskCorpus::new_path_db(path);
        ::teanga::read_json(s.as_bytes(), &mut corpus).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        Ok(PyDiskCorpus(PyCorpus::Disk(corpus)))
    }
}

#[pyfunction]
fn read_corpus_from_json_file(json : &str, path: &str) -> PyResult<PyDiskCorpus> {
    if path == "<memory>" {
        let mut corpus = SimpleCorpus::new();
        let file = std::fs::File::open(json).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        ::teanga::read_json(file, &mut corpus).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        return Ok(PyDiskCorpus(PyCorpus::Mem(corpus)));
    } else {
        let mut corpus = DiskCorpus::new_path_db(path);
        let file = std::fs::File::open(json).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        ::teanga::read_json(file, &mut corpus).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        Ok(PyDiskCorpus(PyCorpus::Disk(corpus)))
    }
}

#[pyfunction]
fn read_corpus_from_cuac_file(cuac : &str, path : &str) -> PyResult<PyDiskCorpus> {
    if path == "<memory>" {
        let mut corpus = SimpleCorpus::new();
        let file = std::fs::File::open(cuac).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        ::teanga::read_cuac(file, &mut corpus).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        return Ok(PyDiskCorpus(PyCorpus::Mem(corpus)));
    } else {
        let mut corpus = DiskCorpus::new_path_db(path);
        let file = std::fs::File::open(cuac).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        ::teanga::read_cuac(file, &mut corpus).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        Ok(PyDiskCorpus(PyCorpus::Disk(corpus)))
    }
}

#[pyfunction]
fn read_corpus_from_yaml_string(s : &str, path: &str) -> PyResult<PyDiskCorpus> {
    if path == "<memory>" {
        let mut corpus = SimpleCorpus::new();
        ::teanga::read_yaml(s.as_bytes(), &mut corpus).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        return Ok(PyDiskCorpus(PyCorpus::Mem(corpus)));
    } else {
        let mut corpus = DiskCorpus::new_path_db(path);
        ::teanga::read_yaml(s.as_bytes(), &mut corpus).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        Ok(PyDiskCorpus(PyCorpus::Disk(corpus)))
    }
}

#[pyfunction]
fn read_corpus_from_yaml_file(yaml : &str, path: &str) -> PyResult<PyDiskCorpus> {
    if path == "<memory>" {
        let mut corpus = SimpleCorpus::new();
        let file = std::fs::File::open(yaml).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        ::teanga::read_yaml(file, &mut corpus).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        return Ok(PyDiskCorpus(PyCorpus::Mem(corpus)));
    } else {
        let mut corpus = DiskCorpus::new_path_db(path);
        let file = std::fs::File::open(yaml).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        ::teanga::read_yaml(file, &mut corpus).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        Ok(PyDiskCorpus(PyCorpus::Disk(corpus)))
    }
}

#[pyfunction]
fn read_corpus_from_yaml_url(url : &str, path : &str) -> PyResult<PyDiskCorpus> {
    if path == "<memory>" {
        let mut corpus = SimpleCorpus::new();
        let url = match reqwest::Url::parse(url) {
            Ok(url) => url,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)));
            }
        };
        if url.scheme() == "file" {
            read_corpus_from_yaml_file(&url.path(), path)
        } else {
            let url = reqwest::blocking::get(url).map_err(|e|
                PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
            ::teanga::read_yaml(url, &mut corpus).map_err(|e|
                PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
            return Ok(PyDiskCorpus(PyCorpus::Mem(corpus)));
        }
    } else {
        let mut corpus = DiskCorpus::new_path_db(path);
        let url = reqwest::blocking::get(url).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        ::teanga::read_yaml(url, &mut corpus).map_err(|e|
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        Ok(PyDiskCorpus(PyCorpus::Disk(corpus)))
    }
}

#[pyfunction]
fn write_corpus_to_yaml(corpus : &PyDiskCorpus, path : &str) -> PyResult<()> {
    let mut file = std::fs::File::create(path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    ::teanga::write_yaml(&mut file, &corpus.0).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    Ok(())
}

#[pyfunction]
fn write_corpus_to_cuac(corpus : &PyDiskCorpus, path : &str) -> PyResult<()> {
    let mut file = std::fs::File::create(path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    ::teanga::write_cuac(&mut file, &corpus.0).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    Ok(())
}

#[pyfunction]
fn write_corpus_to_json(corpus : &PyDiskCorpus, path : &str) -> PyResult<()> {
    let mut file = std::fs::File::create(path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    ::teanga::write_json(&mut file, &corpus.0).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pyfunction]
fn write_corpus_to_json_string(corpus : &PyDiskCorpus) -> PyResult<String> {
    let mut result = Vec::new();
    ::teanga::write_json(&mut result, &corpus.0).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    Ok(String::from_utf8(result).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?)
}

#[pyfunction]
fn write_corpus_to_yaml_string(corpus : &PyDiskCorpus) -> PyResult<String> {
    let mut result = Vec::new();
    ::teanga::write_yaml(&mut result, &corpus.0).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    Ok(String::from_utf8(result).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?)
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name="teanga")]
fn teanga(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDiskCorpus>()?;
    m.add_class::<CuacPyCorpus>()?;
    m.add_function(wrap_pyfunction!(read_corpus_from_json_string, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_yaml_string, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_json_file, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_yaml_file, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_cuac_file, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_yaml_url, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_yaml_string, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_json, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_json_string, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_cuac, m)?)?;
    m.add_function(wrap_pyfunction!(layerdesc_from_dict, m)?)?;
    Ok(())
}
