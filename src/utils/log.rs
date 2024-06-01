use anyhow::Result;

pub trait ResultLogger {
	fn log_e(self) -> Self;
}

impl<T> ResultLogger for Result<T> {
	fn log_e(self) -> Self {
		match self {
			Ok(v) => Ok(v),
			Err(e) => {
				eprintln!("{:?}", e);
				Err(e)
			}
		}
	}
}
