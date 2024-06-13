// Purpose: Rust impl of the TeangaDB Python module.
// Author: John P. McCrae
// License: Apache 2.0
use pyo3::prelude::*;
use ::teanga::{DiskCorpus, LayerDesc, LayerType, DataType, Value, Layer, Corpus};
use std::collections::HashMap;

mod tcf_py;

use tcf_py::TCFPyCorpus;
use ::teanga::{TeangaResult, IntoLayer};

#[pyclass(name="Corpus")]
#[derive(Debug,Clone)]
/// A corpus object
pub struct PyDiskCorpus(DiskCorpus);

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
        Ok(PyDiskCorpus(DiskCorpus::new(path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?))
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

    pub fn add_doc(&mut self, doc: HashMap<String, PyRawLayer>) -> PyResult<()> {
        self.0.add_doc(doc.iter().map(|(k,v)| (k.clone(), v.0.clone())).collect::<HashMap<String, Layer>>())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        Ok(())
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
        self.0.meta = meta.iter().map(|(k,v)| (k.clone(), v.0.clone())).collect();
        Ok(())
    }

    #[getter]
    fn order(&self) -> PyResult<Vec<String>> {
        Ok(self.0.get_order().clone())
    }

    #[setter]
    fn set_order(&mut self, order: Vec<String>) -> PyResult<()> {
        self.0.order = order;
        Ok(())
    }

    fn update_doc(&mut self, id : &str, content: HashMap<String, PyRawLayer>) -> PyResult<String> {
        self.0.update_doc(id, content.iter().map(|(k,v)| (k.clone(), v.0.clone())).collect::<HashMap<String, Layer>>())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))
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

impl IntoPy<PyObject> for PyValue {
    fn into_py(self, py: Python) -> PyObject {
        match self {
            PyValue::Bool(val) => val.into_py(py),
            PyValue::Int(val) => val.into_py(py),
            PyValue::Float(val) => val.into_py(py),
            PyValue::String(val) => val.into_py(py),
            PyValue::Array(val) => val.into_py(py),
            PyValue::Object(val) => val.into_py(py)
        }
    }
}

#[derive(Debug,Clone,PartialEq)]
pub struct PyRawLayer(Layer);

impl IntoPy<PyObject> for PyRawLayer {
    fn into_py(self, py: Python) -> PyObject {
        match self.0 {
            Layer::Characters(val) => val.into_py(py),
            Layer::L1(val) => val.into_py(py),
            Layer::L2(val) => val.into_py(py),
            Layer::L3(val) => val.into_py(py),
            Layer::LS(val) => val.into_py(py),
            Layer::L1S(val) => val.into_py(py),
            Layer::L2S(val) => val.into_py(py),
            Layer::L3S(val) => val.into_py(py),
            Layer::MetaLayer(val) => val.into_iter()
                .map(|v| 
                    v.into_iter().map(|(k,v)| (k, val_to_pyval(v)))
                    .collect::<HashMap<String, PyValue>>())
                    .collect::<Vec<HashMap<String, PyValue>>>()
                    .into_py(py)
        }
    }
}

impl IntoLayer for PyRawLayer {
    fn into_layer(self, _meta: &LayerDesc) -> TeangaResult<Layer> {
        Ok(self.0)
    }
}

impl FromPyObject<'_> for PyRawLayer {
    fn extract(v: &PyAny) -> PyResult<Self> {
        if let Ok(layer) = v.extract::<String>() {
            Ok(PyRawLayer(Layer::Characters(layer)))
        } else if let Ok(layer) = v.extract::<Vec<u32>>() {
            Ok(PyRawLayer(Layer::L1(layer)))
        } else if let Ok(layer) = v.extract::<Vec<(u32, u32)>>() {
            Ok(PyRawLayer(Layer::L2(layer)))
        } else if let Ok(layer) = v.extract::<Vec<(u32, u32, u32)>>() {
            Ok(PyRawLayer(Layer::L3(layer)))
        } else if let Ok(layer) = v.extract::<Vec<String>>() {
            Ok(PyRawLayer(Layer::LS(layer)))
        } else if let Ok(layer) = v.extract::<Vec<(u32, String)>>() {
            Ok(PyRawLayer(Layer::L1S(layer)))
        } else if let Ok(layer) = v.extract::<Vec<(u32, u32, String)>>() {
            Ok(PyRawLayer(Layer::L2S(layer)))
        } else if let Ok(layer) = v.extract::<Vec<Vec<U32OrString>>>() {
            Ok(PyRawLayer(vecus2rawlayer(layer).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?))
        } else if let Ok(layer) = v.extract::<Vec<HashMap<String, &PyAny>>>() {
            let mut layer2 = Vec::new();
            for l in layer {
                let mut layer3 = HashMap::new();
                for (k,v) in l {
                    layer3.insert(k, 
                        v.extract::<PyValue>()?.val());
                }
                layer2.push(layer3);
            }
            Ok(PyRawLayer(Layer::MetaLayer(layer2)))
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown layer type {}", v.extract::<String>()?)))
        }
    }
}

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

impl FromPyObject<'_> for PyLayerType {
    fn extract(ob: &PyAny) -> PyResult<Self> {
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

impl IntoPy<PyObject> for PyLayerType {
    fn into_py(self, py: Python) -> PyObject {
        match self.0 {
            LayerType::characters => "characters".into_py(py),
            LayerType::seq => "seq".into_py(py),
            LayerType::div => "div".into_py(py),
            LayerType::element => "element".into_py(py),
            LayerType::span => "span".into_py(py)
        }
    }
}


#[derive(Debug,Clone,PartialEq)]
pub struct PyDataType(DataType);

impl FromPyObject<'_> for PyDataType {
    fn extract(ob: &PyAny) -> PyResult<Self> {
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

impl IntoPy<PyObject> for PyDataType {
    fn into_py(self, py: Python) -> PyObject {
        match self.0 {
            DataType::String => "string".into_py(py),
            DataType::Enum(v) => v.into_py(py),
            DataType::Link => "link".into_py(py),
        }
    }
}

#[pyfunction]
fn read_corpus_from_json_string(s : &str, path : &str) -> PyResult<PyDiskCorpus> {
    ::teanga::read_corpus_from_json_string(s, path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
        .map(|x| PyDiskCorpus(x))
}

#[pyfunction]
fn read_corpus_from_yaml_string(s : &str, path: &str) -> PyResult<PyDiskCorpus> {
    ::teanga::read_corpus_from_yaml_string(s, path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
        .map(|x| PyDiskCorpus(x))
}

#[pyfunction]
fn read_corpus_from_yaml_file(yaml : &str, path: &str) -> PyResult<PyDiskCorpus> {
    ::teanga::read_corpus_from_yaml_file(yaml, path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
        .map(|x| PyDiskCorpus(x))
}

#[pyfunction]
fn read_corpus_from_yaml_url(url : &str, path : &str) -> PyResult<PyDiskCorpus> {
    ::teanga::read_corpus_from_yaml_url(url, path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
        .map(|x| PyDiskCorpus(x))
}

#[pyfunction]
fn write_corpus_to_yaml(corpus : &PyDiskCorpus, path : &str) -> PyResult<()> {
    ::teanga::write_corpus_to_yaml(&corpus.0, path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pyfunction]
fn write_corpus_to_json(corpus : &PyDiskCorpus, path : &str) -> PyResult<()> {
    ::teanga::write_corpus_to_json(&corpus.0, path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pyfunction]
fn write_corpus_to_json_string(corpus : &PyDiskCorpus) -> PyResult<String> {
    ::teanga::write_corpus_to_json_string(&corpus.0).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pyfunction]
fn write_corpus_to_yaml_string(corpus : &PyDiskCorpus) -> PyResult<String> {
    ::teanga::write_corpus_to_yaml_string(&corpus.0).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
}

#[pyfunction]
fn read_corpus_from_json_file(json : &str, path: &str) -> PyResult<PyDiskCorpus> {
    ::teanga::read_corpus_from_json_file(json, path).map_err(|e|
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))
        .map(|x| PyDiskCorpus(x))
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name="teanga")]
fn teanga(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDiskCorpus>()?;
    m.add_class::<TCFPyCorpus>()?;
    m.add_function(wrap_pyfunction!(read_corpus_from_json_string, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_yaml_string, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_json_file, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_yaml_file, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_yaml_url, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_yaml_string, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_json, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_json_string, m)?)?;
    Ok(())
}
