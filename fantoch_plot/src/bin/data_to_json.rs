use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch_plot::ResultsDB;

fn main() -> Result<(), Report> {
    processed_data_to_json()?;
    Ok(())
}

#[allow(dead_code)]
fn processed_data_to_json() -> Result<(), Report> {
    let results_dir = "../results_fairness_and_tail_latency";
    let output_dir = "../results_fairness_and_tail_latency_processed";
    let db = ResultsDB::load(results_dir).wrap_err("load results")?;
    db.data_to_json(output_dir)?;
    Ok(())
}
