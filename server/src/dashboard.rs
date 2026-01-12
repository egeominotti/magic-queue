use axum::{
    http::header,
    response::{Html, IntoResponse},
    routing::get,
    Router,
};

// Static file contents embedded at compile time for single-binary distribution
const INDEX_HTML: &str = include_str!("../static/index.html");
const DASHBOARD_CSS: &str = include_str!("../static/css/dashboard.css");
const DASHBOARD_JS: &str = include_str!("../static/js/dashboard.js");
const UTILS_JS: &str = include_str!("../static/js/utils.js");
const API_JS: &str = include_str!("../static/js/api.js");
const CHARTS_JS: &str = include_str!("../static/js/charts.js");
const SETTINGS_JS: &str = include_str!("../static/js/settings.js");

pub fn dashboard_routes() -> Router {
    Router::new()
        .route("/", get(serve_index))
        .route("/dashboard", get(serve_index))
        .route("/static/css/dashboard.css", get(serve_css))
        .route("/static/js/dashboard.js", get(serve_dashboard_js))
        .route("/static/js/utils.js", get(serve_utils_js))
        .route("/static/js/api.js", get(serve_api_js))
        .route("/static/js/charts.js", get(serve_charts_js))
        .route("/static/js/settings.js", get(serve_settings_js))
}

async fn serve_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn serve_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        DASHBOARD_CSS,
    )
}

async fn serve_dashboard_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        DASHBOARD_JS,
    )
}

async fn serve_utils_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        UTILS_JS,
    )
}

async fn serve_api_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        API_JS,
    )
}

async fn serve_charts_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        CHARTS_JS,
    )
}

async fn serve_settings_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        SETTINGS_JS,
    )
}
