// Purpose: Rust impl of the TeangaDB Python module.
// Author: John P. McCrae
// License: Apache 2.0
use pyo3::prelude::*;
use ::teanga::{DiskCorpus, LayerDesc, LayerType, DataType, Value, RawLayer, Corpus};
use std::collections::HashMap;

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
        base: Option<String>, data: Option<PyDataType>, values: Option<Vec<String>>, 
        target: Option<String>, default: Option<Vec<String>>,
        uri : Option<String>) -> PyResult<()> {
        Ok(self.0.add_layer_meta(name, layer_type.0, base, data.map(|x| x.0), values, target, default, uri)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?)
    }

    pub fn add_doc(&mut self, doc: HashMap<String, PyRawLayer>) -> PyResult<()> {
        self.0.add_doc(doc.iter().map(|(k,v)| (k.clone(), v.0.clone())).collect::<HashMap<String, RawLayer>>())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
        Ok(())
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
    fn values(&self) -> PyResult<Option<Vec<String>>> {
        Ok(self.0.values.clone())
    }

    #[getter]
    fn target(&self) -> PyResult<Option<String>> {
        Ok(self.0.target.clone())
    }

    #[getter]
    fn default(&self) -> PyResult<Option<Vec<String>>> {
        Ok(self.0.default.clone())
    }

    #[getter]
    fn _uri(&self) -> PyResult<Option<String>> {
        Ok(self.0._uri.clone())
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
pub struct PyRawLayer(RawLayer);

impl IntoPy<PyObject> for PyRawLayer {
    fn into_py(self, py: Python) -> PyObject {
        match self.0 {
            RawLayer::CharacterLayer(val) => val.into_py(py),
            RawLayer::L1(val) => val.into_py(py),
            RawLayer::L2(val) => val.into_py(py),
            RawLayer::L3(val) => val.into_py(py),
            RawLayer::LS(val) => val.into_py(py),
            RawLayer::L1S(val) => val.into_py(py),
            RawLayer::L2S(val) => val.into_py(py),
            RawLayer::L3S(val) => val.into_py(py),
            RawLayer::MetaLayer(val) => val.into_iter()
                .map(|v| 
                    v.into_iter().map(|(k,v)| (k, val_to_pyval(v)))
                    .collect::<HashMap<String, PyValue>>())
                    .collect::<Vec<HashMap<String, PyValue>>>()
                    .into_py(py)
        }
    }
}

impl FromPyObject<'_> for PyRawLayer {
    fn extract(v: &PyAny) -> PyResult<Self> {
        if let Ok(layer) = v.extract::<String>() {
            Ok(PyRawLayer(RawLayer::CharacterLayer(layer)))
        } else if let Ok(layer) = v.extract::<Vec<u32>>() {
            Ok(PyRawLayer(RawLayer::L1(layer)))
        } else if let Ok(layer) = v.extract::<Vec<(u32, u32)>>() {
            Ok(PyRawLayer(RawLayer::L2(layer)))
        } else if let Ok(layer) = v.extract::<Vec<(u32, u32, u32)>>() {
            Ok(PyRawLayer(RawLayer::L3(layer)))
        } else if let Ok(layer) = v.extract::<Vec<String>>() {
            Ok(PyRawLayer(RawLayer::LS(layer)))
        } else if let Ok(layer) = v.extract::<Vec<(u32, String)>>() {
            Ok(PyRawLayer(RawLayer::L1S(layer)))
        } else if let Ok(layer) = v.extract::<Vec<(u32, u32, String)>>() {
            Ok(PyRawLayer(RawLayer::L2S(layer)))
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
            Ok(PyRawLayer(RawLayer::MetaLayer(layer2)))
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown layer type {}", v.extract::<String>()?)))
        }
    }
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
            DataType::Enum(_) => "string".into_py(py),
            DataType::Link => "link".into_py(py),
            DataType::TypedLink(_) => "link".into_py(py)
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
fn teanga(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyDiskCorpus>()?;
    m.add_function(wrap_pyfunction!(read_corpus_from_json_string, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_yaml_string, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_json_file, m)?)?;
    m.add_function(wrap_pyfunction!(read_corpus_from_yaml_file, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_yaml_string, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_json, m)?)?;
    m.add_function(wrap_pyfunction!(write_corpus_to_json_string, m)?)?;
    Ok(())
}
