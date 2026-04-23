#[tokio::main]
async fn main() {
    let db_path  = std::env::args().nth(1);
    let bind     = std::env::args().nth(2).unwrap_or_else(|| "0.0.0.0:8000".into());
    viscacha_api::run(db_path.as_deref(), &bind).await;
}
