#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Object {
    name: String,
}

impl Object {
    /// Create a new `Object`.
    pub fn new<S: Into<String>>(name: S) -> Self {
        Object { name: name.into() }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Command {
    objects: Vec<Object>,
}

impl Command {
    /// Create a new `Command`.
    pub fn new(objects: Vec<Object>) -> Self {
        Command { objects }
    }

    /// Returns references to list of objects modified by this command.
    pub fn objects(&self) -> &Vec<Object> {
        &self.objects
    }

    /// Returns list of objects modified by this command.
    pub fn objects_clone(&self) -> Vec<Object> {
        self.objects.clone()
    }
}
