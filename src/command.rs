pub type Key = String;
pub type Value = String;

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Get(Key),
    Put(Key, Value),
}

impl Command {
    /// Retrieves the `Key` accessed by this command.
    pub fn key(&self) -> &Key{
        match self {
            Command::Get(key) => key,
            Command::Put(key, _) => key,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MultiCommand {
    commands: Vec<Command>,
}

impl MultiCommand {
    /// Create a new `MultiCommand`.
    pub fn new(commands: Vec<Command>) -> Self {
        MultiCommand { commands }
    }

    /// Returns references to list of keys modified by this command.
    pub fn keys(&self) -> Vec<&Key> {
        self.commands
            .iter()
            .map(|command| command.key())
            .collect()
    }
}
