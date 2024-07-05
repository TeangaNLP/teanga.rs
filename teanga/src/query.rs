use std::collections::{HashMap, HashSet};
use crate::{Document, LayerDesc, TeangaData};
use regex::Regex;

#[derive(Debug)]
pub enum Query {
    Text(String, String),
    TextNot(String, String),
    Value(String, TeangaData),
    ValueNot(String, TeangaData),
    LessThan(String, TeangaData),
    LessThanEqual(String, TeangaData),
    GreaterThan(String, TeangaData),
    GreaterThanEqual(String, TeangaData),
    In(String, HashSet<TeangaData>),
    NotIn(String, HashSet<TeangaData>),
    Regex(String, Regex),
    TextRegex(String, Regex),
    And(Vec<Query>),
    Or(Vec<Query>),
    Not(Box<Query>),
    Exists(String)
}

impl Query {
    pub fn matches(&self, document : &Document,
        meta : &HashMap<String, LayerDesc>) -> bool {
        eprintln!("Query: {:?}", self);
        match self {
            Query::Text(layer, text) => {
                eprintln!("Text: {:?}", document.text(layer, meta));
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

pub struct QueryBuilder(Query);

impl QueryBuilder {
    pub fn new() -> QueryBuilder {
        QueryBuilder(Query::And(Vec::new()))
    }

    pub fn build(self) -> Query {
        self.0
    }

    pub fn text(self, layer : String, text: String) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::Text(layer, text));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::Text(layer, text), self.0]))
        }
    }

    pub fn text_not(self, layer : String, text: String) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::TextNot(layer, text));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::TextNot(layer, text), self.0]))
        }
    }

    pub fn value(self, layer : String, value: TeangaData) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::Value(layer, value));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::Value(layer, value), self.0]))
        }
    }

    pub fn value_not(self, layer : String, value: TeangaData) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::ValueNot(layer, value));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::ValueNot(layer, value), self.0]))
        }
    }

    pub fn less_than(self, layer : String, value: TeangaData) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::LessThan(layer, value));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::LessThan(layer, value), self.0]))
        }
    }

    pub fn less_than_equal(self, layer : String, value: TeangaData) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::LessThanEqual(layer, value));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::LessThanEqual(layer, value), self.0]))
        }
    }

    pub fn greater_than(self, layer : String, value: TeangaData) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::GreaterThan(layer, value));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::GreaterThan(layer, value), self.0]))
        }
    }

    pub fn greater_than_equal(self, layer : String, value: TeangaData) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::GreaterThanEqual(layer, value));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::GreaterThanEqual(layer, value), self.0]))
        }
    }

    pub fn in_(self, layer : String, values: HashSet<TeangaData>) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::In(layer, values));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::In(layer, values), self.0]))
        }
    }

    pub fn not_in(self, layer : String, values: HashSet<TeangaData>) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::NotIn(layer, values));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::NotIn(layer, values), self.0]))
        }
    }

    pub fn regex(self, layer : String, regex: Regex) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::Regex(layer, regex));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::Regex(layer, regex), self.0]))
        }
    }

    pub fn text_regex(self, layer : String, regex: Regex) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::TextRegex(layer, regex));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::TextRegex(layer, regex), self.0]))
        }
    }

    pub fn and(self, queries: Vec<Query>) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.extend(queries);
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::And(queries), self.0]))
        }
    }

    pub fn or(self, queries: Vec<Query>) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::Or(queries));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::Or(queries), self.0]))
        }
    }

    pub fn not(self, query: Query) -> QueryBuilder {
        if let Query::And(and) = self.0 {
            let mut q = and;
            q.push(Query::Not(Box::new(query)));
            QueryBuilder(Query::And(q))
        } else {
            QueryBuilder(Query::And(vec![Query::Not(Box::new(query)), self.0]))
        }
    }

    pub fn exists(self, field: String) -> QueryBuilder {
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
            .text("words".to_string(), "fox".to_string())
            .build());
        assert!(iter.next().is_some());
    }
}

