#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Condition(String);

impl Condition {
    pub fn new(condition: String) -> Self {
        Condition(condition)
    }

    pub fn query(&self) -> &str {
        &self.0
    }
}

impl From<&nostr_sdk::Filter> for Condition {
    fn from(filter: &nostr_sdk::Filter) -> Self {
        Condition::new(filter.search.clone().unwrap_or_default())
    }
}
