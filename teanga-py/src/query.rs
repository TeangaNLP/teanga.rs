use teanga::{Query, TeangaData};
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use regex::Regex;

#[derive(Debug)]
pub struct PyQuery(pub Query);

#[derive(Debug,Clone,FromPyObject)]
enum QueryValue {
    Vec(Vec<QueryValue>),
    Map(HashMap<String, QueryValue>),
    String(String),
    Int(u32),
    IntString((u32, String)),
}

impl QueryValue {
    fn to_query(self) -> PyResult<Query> {
        match self {
            QueryValue::Vec(v) => {
                let mut vec = Vec::new();
                for x in v {
                    vec.push(x.to_query()?);
                }
                Ok(Query::And(vec))
            }
            QueryValue::Map(m) => convert_query(m),
            _ => Err(pyo3::exceptions::PyTypeError::new_err("Expected a list or map")),
        }
    }

    fn to_set(self) -> PyResult<HashSet<TeangaData>> {
        match self {
            QueryValue::Vec(v) => {
                let mut set = HashSet::new();
                for item in v {
                    set.insert(item.to_data()?);
                }
                Ok(set)
            },
            _ => Err(pyo3::exceptions::PyTypeError::new_err("Expected a list")),
        }
    }

    fn to_data(self) -> PyResult<TeangaData> {
        match self {
            QueryValue::String(s) => Ok(TeangaData::String(s)),
            QueryValue::Int(i) => Ok(TeangaData::Link(i)),
            QueryValue::IntString((i, s)) => Ok(TeangaData::TypedLink(i, s)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err("Expected a string or int")),
        }
    }

    fn to_string(self) -> PyResult<String> {
        match self {
            QueryValue::String(s) => Ok(s),
            _ => Err(pyo3::exceptions::PyTypeError::new_err("Expected a string")),
        }
    }

}

fn convert_query(query : HashMap<String, QueryValue>) -> PyResult<Query> {
    let mut queries = Vec::new();
    for (layer, value) in query {
        if layer == "$exists" {
            queries.push(Query::Exists(value.to_string()?));
        } else {
            match value {
                QueryValue::Vec(v) => {
                    let mut vec = HashSet::new();
                    for x in v {
                        vec.insert(x.to_data()?);
                    }
                    queries.push(Query::In(layer, vec));
                },
                QueryValue::Map(m) => {
                    if m.len() == 1 {
                        let (key, value) = m.iter().next()
                            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!("Dict should have a single key: {:?}", m)))?;
                        let value = value.clone();
                        if key == "$text" {
                            queries.push(Query::Text(layer, value.to_string()?))
                        } else if key == "$text_ne" {
                            queries.push(Query::TextNot(layer, value.to_string()?))
                        } else if key == "$eq" {
                            queries.push(Query::Value(layer, value.to_data()?))
                        } else if key == "$ne" {
                            queries.push(Query::ValueNot(layer, value.to_data()?))
                        } else if key == "$lt" {
                            queries.push(Query::LessThan(layer, value.to_data()?))
                        } else if key == "$lte" {
                            queries.push(Query::LessThanEqual(layer, value.to_data()?))
                        } else if key == "$gt" {
                            queries.push(Query::GreaterThan(layer, value.to_data()?))
                        } else if key == "$gte" {
                            queries.push(Query::GreaterThanEqual(layer, value.to_data()?))
                        } else if key == "$in" {
                            queries.push(Query::In(layer, value.to_set()?))
                        } else if key == "$nin" {
                            queries.push(Query::NotIn(layer, value.to_set()?))
                        } else if key == "$regex" {
                            queries.push(Query::Regex(layer, Regex::new(&value.to_string()?)
                                    .map_err(|_| pyo3::exceptions::PyValueError::new_err("Invalid Regex"))?))
                        } else if key == "$text_regex" {
                            queries.push(Query::TextRegex(layer, Regex::new(&value.to_string()?)
                                    .map_err(|_| pyo3::exceptions::PyValueError::new_err("Invalid Regex"))?))
                        } else {
                            return Err(pyo3::exceptions::PyValueError::new_err(format!("Unknown key: {}", key)))
                        }
                    } else {
                        return Err(pyo3::exceptions::PyValueError::new_err("Dict should have a single key"))
                    }
                },
                QueryValue::String(s) => {
                    queries.push(Query::Value(layer, TeangaData::String(s)))
                },
                QueryValue::Int(i) => {
                    queries.push(Query::Value(layer, TeangaData::Link(i)))
                },
                QueryValue::IntString((i, s)) => {
                    queries.push(Query::Value(layer, TeangaData::TypedLink(i, s)))
                },
            }
        }
    }
    if queries.len() == 1 {
        Ok(queries.remove(0))
    } else {
        Ok(Query::And(queries))
    }
}

impl <'py> FromPyObject<'py> for PyQuery {
    fn extract_bound(v: &Bound<'py, PyAny>) -> PyResult<Self> {
        let mut query = v.extract::<HashMap<String, QueryValue>>()?;
        if query.len() == 1 {
            let key = query.keys().next().unwrap();
            if key == "$and" {
                let value = query.remove("$and").unwrap();
                if let QueryValue::Vec(value) = value {
                    let mut queries = Vec::new();
                    for x in value.into_iter() {
                        queries.push(x.to_query()?);
                    }
                    return Ok(PyQuery(Query::And(queries)));
                } else if let QueryValue::Map(value) = value {
                    return convert_query(value).map(PyQuery);
                } else {
                    return Err(pyo3::exceptions::PyTypeError::new_err("Expected a list as value for $and"));
                }
            } else if key == "$or" {
                let value = query.remove("$or").unwrap();
                if let QueryValue::Vec(value) = value {
                    let mut queries = Vec::new();
                    for x in value.into_iter() {
                        queries.push(x.to_query()?);
                    }
                    return Ok(PyQuery(Query::Or(queries)));
                } else if let QueryValue::Map(value) = value {
                    let queries = convert_query(value)?;
                    if let Query::And(queries) = queries {
                        return Ok(PyQuery(Query::Or(queries)));
                    } else {
                        return Ok(PyQuery(Query::Or(vec![queries])));
                    }
                } else {
                    return Err(pyo3::exceptions::PyTypeError::new_err("Expected a list as value for $or"));
                }
            } else if key == "$not" {
                let value = query.remove("$not").unwrap();
                if let QueryValue::Vec(mut value) = value {
                    return Ok(PyQuery(Query::Not(Box::new(value.remove(0).to_query()?))));
                } else {
                    return Err(pyo3::exceptions::PyTypeError::new_err("Expected a list as value for $not"));
                }
            } 
        }
        convert_query(query).map(PyQuery)
    }
}
 
