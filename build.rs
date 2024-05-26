use anyhow::Result;

fn main() -> Result<()> {
	println!("cargo:rerun-if-changed=protocol/tempo/tempo.proto");
	let mut cfg = prost_build::Config::new();
	let mut builder = cfg
		.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
		.type_attribute(".", "#[serde(rename_all = \"camelCase\")]")
		.extern_path(
			".opentelemetry.proto.trace.v1",
			"opentelemetry_proto::tonic::trace::v1",
		)
		.extern_path(
			".opentelemetry.proto.common.v1",
			"opentelemetry_proto::tonic::common::v1",
		)
		.format(true)
		.out_dir("src/proto");

	let i64_fields = [
		"startTimeUnixNano",
		"durationNanos",
		"inspectedBytes",
		"totalBlockBytes",
	];
	for field in i64_fields {
		builder = builder.field_attribute(
			field,
			"#[serde(with = \"crate::utils::serde::jsonstr\")]",
		);
	}

	let id_fields = ["traceID", "spanID"];
	for field in id_fields {
		builder = builder.field_attribute(
			field,
			format!("#[serde(rename = \"{}\")]", field),
		);
	}

	builder.compile_protos(&["protocol/tempo/tempo.proto"], &["protocol"])?;
	Ok(())
}
