use anyhow::Result;
use tracing::error;

pub trait ResultLogger {
	fn log_e(self) -> Self;
}

impl<T> ResultLogger for Result<T> {
	fn log_e(self) -> Self {
		self.map_err(|e| {
			error!("{:?}", e);
			e
		})
	}
}
