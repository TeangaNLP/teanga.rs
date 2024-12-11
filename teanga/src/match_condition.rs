//! Text match condition
//!
//! This module provides a trait for whether a section
//! of text matches a condition.
use crate::layer::TeangaData;

/// Matching condition for text
pub trait TextMatchCondition {
    /// Check if the text matches the condition
    fn matches(&self, text: &str) -> bool;
}

impl TextMatchCondition for String {
    fn matches(&self, text: &str) -> bool {
        self == text
    }
}

impl TextMatchCondition for Vec<String> {
    fn matches(&self, text: &str) -> bool {
        self.iter().any(|item| item == text)
    }
}

pub struct AnyText;

impl TextMatchCondition for AnyText {
    fn matches(&self, _text: &str) -> bool {
        true
    }
}

/// Data match condition
pub trait DataMatchCondition {
    /// Check if the data matches the condition
    fn matches(&self, data: &TeangaData) -> bool;
}

impl DataMatchCondition for String {
    fn matches(&self, data: &TeangaData) -> bool {
        if let TeangaData::String(s) = data {
            self == s
        } else {
            false
        }
    }
}

impl DataMatchCondition for TeangaData {
    fn matches(&self, data: &TeangaData) -> bool {
        self == data
    }
}

impl DataMatchCondition for Vec<String> {
    fn matches(&self, data: &TeangaData) -> bool {
        if let TeangaData::String(s) = data {
            self.iter().any(|item| item == s)
        } else {
            false
        }
    }
}

impl DataMatchCondition for Vec<TeangaData> {
    fn matches(&self, data: &TeangaData) -> bool {
        self.iter().any(|item| item == data)
    }
}

pub struct AnyData;

impl DataMatchCondition for AnyData {
    fn matches(&self, _data: &TeangaData) -> bool {
        true
    }
}
