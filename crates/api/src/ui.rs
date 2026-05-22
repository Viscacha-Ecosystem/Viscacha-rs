use axum::response::Html;

static HTML: &str = include_str!("../assets/index.html");

pub async fn ui_page() -> Html<&'static str> {
    Html(HTML)
}
