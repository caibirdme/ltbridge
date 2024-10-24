use anyhow::Result;
use ltbridge::app;

#[tokio::main]
async fn main() -> Result<()> {
	app::start().await
}
