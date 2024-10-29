use axum::{
	http::StatusCode,
	response::{IntoResponse, Response},
};
use databend_driver::Error as DBError;
use logql::parser::LogQLParseError;
use thiserror::Error;
use traceql::TraceQLError;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum AppError {
	#[error("Invalid logql: {0}")]
	InvalidLogQL(#[from] LogQLParseError),
	#[error("Invalid traceql: {0}")]
	InvalidTraceQL(TraceQLError),
	#[error("Invalid time format: {0}")]
	InvalidTimeFormat(String),
	#[error("db error: {0}")]
	DBError(#[from] DBError),
	#[error("Serde error: {0}")]
	SerdeError(#[from] serde_json::Error),
	#[error("Unsupported data type: {0}")]
	UnsupportedDataType(String),
	#[error("Storage error: {0}")]
	StorageError(#[from] anyhow::Error),
	#[error("series only support one match, but got: {0}")]
	MultiMatch(usize),
	#[error("Invalid query string: {0}")]
	InvalidQueryString(String),
	#[error("Trace not found")]
	TraceNotFound,
	#[error("IO error: {0}")]
	IOError(#[from] std::io::Error),
	#[error("Rmp error: {0}")]
	RmpDecodeError(#[from] rmp_serde::decode::Error),
	#[error("Rmp encode error: {0}")]
	RmpEncodeError(#[from] rmp_serde::encode::Error),
}

impl IntoResponse for AppError {
	fn into_response(self) -> Response {
		match self {
			AppError::StorageError(e) => (
				StatusCode::INTERNAL_SERVER_ERROR,
				format!("Storage error: {}", e),
			)
				.into_response(),
			AppError::InvalidTraceQL(e) => (
				StatusCode::BAD_REQUEST,
				format!("Invalid trace query: {}", e),
			)
				.into_response(),
			AppError::UnsupportedDataType(e) => (
				StatusCode::INTERNAL_SERVER_ERROR,
				format!("Unsupported data type: {}", e),
			)
				.into_response(),
			AppError::SerdeError(e) => (
				StatusCode::INTERNAL_SERVER_ERROR,
				format!("Serde error: {}", e),
			)
				.into_response(),
			AppError::InvalidLogQL(e) => {
				(StatusCode::BAD_REQUEST, format!("Invalid query: {}", e))
					.into_response()
			}
			AppError::InvalidTimeFormat(e) => (
				StatusCode::BAD_REQUEST,
				format!("Invalid time format: {}", e),
			)
				.into_response(),
			AppError::DBError(e) => (
				StatusCode::INTERNAL_SERVER_ERROR,
				format!("DB error: {}", e),
			)
				.into_response(),
			AppError::MultiMatch(n) => (
				StatusCode::BAD_REQUEST,
				format!("series only support one match, but got: {}", n),
			)
				.into_response(),
			AppError::InvalidQueryString(e) => (
				StatusCode::BAD_REQUEST,
				format!("Invalid query string: {}", e),
			)
				.into_response(),
			AppError::TraceNotFound => {
				(StatusCode::NOT_FOUND, "Trace not found".to_string())
					.into_response()
			}
			AppError::IOError(e) => (
				StatusCode::INTERNAL_SERVER_ERROR,
				format!("IO error: {}", e),
			)
				.into_response(),
			AppError::RmpDecodeError(e) => (
				StatusCode::INTERNAL_SERVER_ERROR,
				format!("Rmp decode error: {}", e),
			)
				.into_response(),
			AppError::RmpEncodeError(e) => (
				StatusCode::INTERNAL_SERVER_ERROR,
				format!("Rmp encode error: {}", e),
			)
				.into_response(),
		}
	}
}
