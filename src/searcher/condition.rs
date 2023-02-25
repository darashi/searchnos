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
