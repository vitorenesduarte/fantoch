use crate::id::ClientId;

pub fn ids_repr(client_ids: &Vec<ClientId>) -> String {
    client_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join("-")
}
