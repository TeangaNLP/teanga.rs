//! Searching a corpus
//!
//! The `query` module provides a way to search a corpus for documents that match
//! a set of conditions.
//!
//! # Examples
//!
//! ```
//! use teanga::query::{QueryBuilder, Query};
//! let query = QueryBuilder::new()
//!     .text("words", "fox")
//!     .build();
//! ```
use std::collections::{HashMap, HashSet};
use crate::{Document, LayerDesc, TeangaData};
use regex::Regex;

/// A query for searching a corpus
#[derive(Debug)]
pub enum Query {
    /// A text value in a layer matches
    Text(String, String),
    /// A text value in a layer does not match
    TextNot(String, String),
    /// A data value in a layer matches
    Value(String, TeangaData),
    /// A data value in a layer does not match
    ValueNot(String, TeangaData),
    /// A data value in a layer is less than a value
    LessThan(String, TeangaData),
    /// A data value in a layer is less than or equal to a value
    LessThanEqual(String, TeangaData),
    /// A data value in a layer is greater than a value
    GreaterThan(String, TeangaData),
    /// A data value in a layer is greater than or equal to a value
    GreaterThanEqual(String, TeangaData),
    /// A data value in a layer is in a set of values
    In(String, HashSet<TeangaData>),
    /// A data value in a layer is not in a set of values
    NotIn(String, HashSet<TeangaData>),
    /// A data value in a layer matches a regex
    Regex(String, Regex),
    /// A text value in a layer matches a regex
    TextRegex(String, Regex),
    /// All of a set of queries match
    And(Vec<Query>),
    /// Any of a set of queries match
    Or(Vec<Query>),
    /// A query does not match
    Not(Box<Query>),
    /// A layer is present in a document
    Exists(String)
}

impl Query {
    pub fn matches(&self, document : &Document,
        meta : &HashMap<String, LayerDesc>) -> bool {
        match self {
            Query::Text(layer, text) => {
                document.text(layer, meta).map_or(false,
                    |t| t.iter().any(|t| t == text))
            },
            Query::TextNot(layer, text) => {
                document.text(layer, meta).map_or(false,
                    |t| t.iter().any(|t| t != text))
            },
            Query::Value(layer, value) => {
                document.data(layer, meta).map_or(false,
                    |v| v.iter().any(|v| v == value))
            },
            Query::ValueNot(layer, value) => {
                document.data(layer, meta).map_or(false,
                    |v| v.iter().any(|v| v != value))
            },
            Query::LessThan(layer, value) => {
                document.data(layer, meta).map_or(false,
                    |v| v.iter().any(|v| v < value))
            },
            Query::LessThanEqual(layer, value) => {
                document.data(layer, meta).map_or(false,
                    |v| v.iter().any(|v| v <= value))
            },
            Query::GreaterThan(layer, value) => {
                document.data(layer, meta).map_or(false,
                    |v| v.iter().any(|v| v > value))
            },
            Query::GreaterThanEqual(layer, value) => {
                document.data(layer, meta).map_or(false,
                    |v| v.iter().any(|v| v >= value))
            },
            Query::In(layer, values) => {
                document.data(layer, meta).map_or(false,
                    |v| v.iter().any(|v| values.contains(v)))
            },
            Query::NotIn(layer, values) => {
                document.data(layer, meta).map_or(false,
                    |v| v.iter().any(|v| !values.contains(v)))
            },
            Query::Regex(layer, regex) => {
                document.data(layer, meta).map_or(false,
                    |t| t.iter().any(|t| match t {
                        TeangaData::String(t) => regex.is_match(t),
                        _ => false
                    }))
            },
            Query::TextRegex(layer, regex) => {
                document.text(layer, meta).map_or(false,
                    |t| t.iter().any(|t| regex.is_match(t)))
            },
            Query::And(and) => {
                and.iter().all(|q| q.matches(document, meta))
            },
            Query::Or(or) => {
                or.iter().any(|q| q.matches(document, meta))
            },
            Query::Not(q) => {
                !q.matches(document, meta)
            },
            Query::Exists(field) => {
                document.get(field).is_some()
            }
        }
    }
}

/// Utility for building queries
pub struct QueryBuilder(Query);

impl QueryBuilder {
    /// Start building a new query
    pub fn new() -> QueryBuilder {
        QueryBuilder(Query::And(Vec::new()))
    }

    /// Finish building the query
    pub fn build(self) -> Query {
        self.0
    }

    /// Add a text match condition to the query
    pub fn text(self, layer : &str, text: &str) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::Text(layer.to_string(), text.to_string()));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::Text(layer.to_string(), text.to_string()), self.0]))
        }
    }

    /// Add a text not match condition to the query
    pub fn text_not(self, layer : &str, text: &str) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::TextNot(layer.to_string(), text.to_string()));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::TextNot(layer.to_string(), text.to_string()), self.0]))
        }
    }

    /// Add a data match condition to the query
    pub fn value<T : Into<TeangaData>>(self, layer : &str, value: T) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::Value(layer.to_string(), value.into()));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::Value(layer.to_string(), value.into()), self.0]))
        }
    }

    /// Add a data not match condition to the query
    pub fn value_not<T : Into<TeangaData>>(self, layer : &str, value: T) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::ValueNot(layer.to_string(), value.into()));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::ValueNot(layer.to_string(), value.into()), self.0]))
        }
    }

    /// Add a less than condition to the query
    pub fn less_than<T : Into<TeangaData>>(self, layer : &str, value: T) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::LessThan(layer.to_string(), value.into()));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::LessThan(layer.to_string(), value.into()), self.0]))
        }
    }

    /// Add a less than or equal condition to the query
    pub fn less_than_equal<T : Into<TeangaData>>(self, layer : &str, value: T) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::LessThanEqual(layer.to_string(), value.into()));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::LessThanEqual(layer.to_string(), value.into()), self.0]))
        }
    }

    /// Add a greater than condition to the query
    pub fn greater_than<T : Into<TeangaData>>(self, layer : &str, value: T) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::GreaterThan(layer.to_string(), value.into()));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::GreaterThan(layer.to_string(), value.into()), self.0]))
        }
    }

    /// Add a greater than or equal condition to the query
    pub fn greater_than_equal<T : Into<TeangaData>>(self, layer : &str, value: T) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::GreaterThanEqual(layer.to_string(), value.into()));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::GreaterThanEqual(layer.to_string(), value.into()), self.0]))
        }
    }

    /// Add an in condition to the query
    pub fn in_<T : Into<TeangaData>>(self, layer : &str, values: HashSet<T>) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::In(layer.to_string(), values.into_iter().map(|x| x.into()).collect()));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::In(layer.to_string(), 
                        values.into_iter().map(|x| x.into()).collect()),
                    self.0]))
        }
    }

    /// Add a not in condition to the query
    pub fn not_in<T : Into<TeangaData>>(self, layer : &str, values: HashSet<T>) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::NotIn(layer.to_string(), values.into_iter().map(|x| x.into()).collect()));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::NotIn(layer.to_string(), 
                        values.into_iter().map(|x| x.into()).collect()),
                    self.0]))
        }
    }

    /// Add a data regex condition to the query
    pub fn regex(self, layer : &str, regex: Regex) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::Regex(layer.to_string(), regex));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::Regex(layer.to_string(), regex), self.0]))
        }
    }

    /// Add a text regex condition to the query
    pub fn text_regex(self, layer : &str, regex: Regex) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::TextRegex(layer.to_string(), regex));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::TextRegex(layer.to_string(), regex), self.0]))
        }
    }

    /// Combine queries with an and
    pub fn and(self, queries: Vec<Query>) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.extend(queries);
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::And(queries), self.0]))
        }
    }

    /// Combine queries with an or
    pub fn or(self, queries: Vec<Query>) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::Or(queries));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::Or(queries), self.0]))
        }
    }

    /// Negate a query
    pub fn not(self, query: Query) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::Not(Box::new(query)));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::Not(Box::new(query)), self.0]))
        }
    }

    /// Add an exists condition to the query
    pub fn exists(self, field: &str) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::Exists(field.to_string()));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::Exists(field.to_string()), self.0]))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Corpus, SimpleCorpus, LayerType, DataType};

    #[test]
    fn test_query() {
        let mut corpus = SimpleCorpus::new();
        corpus.build_layer("text").add().unwrap();
        corpus.build_layer("words")
            .layer_type(LayerType::span)
            .base("text").add().unwrap();
        corpus.build_layer("pos")
            .layer_type(LayerType::seq)
            .base("words")
            .data(DataType::Enum(vec![
                    "noun".to_string(),
                    "verb".to_string(),
                    "adjective".to_string()])).add().unwrap();
        corpus.build_layer("lemma")
            .layer_type(LayerType::seq)
            .base("words")
            .data(DataType::String).add().unwrap();
        let _doc = corpus.build_doc()
            .layer("text", "The quick brown fox jumps over the lazy dog").unwrap()
            .layer("words", vec![(0, 3), (4, 9), (10, 15), (16, 19), (20, 25), (26, 30), (31, 34), (35, 39), (40, 43)]).unwrap()
            .layer("pos", vec!["det", "adj", "adj", "noun", "verb", "adp", "det", "adj", "noun", "punct"]).unwrap()
            .layer("lemma", vec!["the", "quick", "brown", "fox", "jump", "over", "the", "lazy", "dog", "."]).unwrap()
            .add().unwrap();
        let mut iter = corpus.search(QueryBuilder::new()
            .text("words", "fox")
            .build());
        assert!(iter.next().is_some());
    }

    #[test]
    fn test_query2() {
        let mut corpus = SimpleCorpus::new();
        corpus.build_layer("text").add().unwrap();
        corpus.build_layer("words")
            .layer_type(LayerType::span)
            .base("text").add().unwrap();
        corpus.build_layer("oewn")
            .layer_type(LayerType::element)
            .base("words")
            .data(DataType::String).add().unwrap();
        let _doc = corpus.build_doc()
            .layer("text", "The Fulton_County_Grand_Jury said Friday").unwrap()
            .layer("words", vec![(0, 3), (4, 28), (29, 33)]).unwrap()
            .layer("oewn", vec![(1, "oewn-00031563-n"), (2, "oewn-01011267-v")]).unwrap()
            .add().unwrap();
        let query = QueryBuilder::new()
            .value("oewn", "oewn-00031563-n".to_string())
            .build();
        let mut iter = corpus.search(query);
        assert!(iter.next().is_some());
    }
}

