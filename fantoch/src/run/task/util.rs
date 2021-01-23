use crate::id::ClientId;
use color_eyre::Report;
use serde::Serialize;

pub fn ids_repr(client_ids: &Vec<ClientId>) -> String {
    client_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join("-")
}

pub fn serialize_and_compress<T: Serialize>(
    data: &T,
    file: &str,
) -> Result<(), Report> {
    // if the file does not exist it will be created, otherwise truncated
    std::fs::File::create(file)
        .ok()
        // create a buf writer
        .map(std::io::BufWriter::new)
        // compress using gzip
        .map(|buffer| {
            flate2::write::GzEncoder::new(buffer, flate2::Compression::best())
        })
        // and try to serialize
        .map(|writer| {
            bincode::serialize_into(writer, data)
                .expect("error serializing data")
        })
        .unwrap_or_else(|| {
            panic!("couldn't save serialized data in file {:?}", file)
        });

    Ok(())
}
