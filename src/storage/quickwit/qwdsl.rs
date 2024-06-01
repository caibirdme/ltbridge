use core::fmt;

use serde_json::Value as JSONValue;

pub struct TermCtx {
	pub field: String,
	pub value: JSONValue,
}

pub struct PhraseCtx {
	pub field: String,
	pub value: String,
}

pub enum Clause {
	Term(TermCtx),
	Phrase(PhraseCtx),
	Defaultable(String),
}

impl fmt::Display for Clause {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Clause::Term(term) => {
				let v = term.value.to_string();
				let val = v.trim_matches('"');
				if is_valid_string(val) {
					write!(f, "{}:{}", term.field, val)
				} else {
					write!(f, "{}:\"{}\"", term.field, val)
				}
			}
			Clause::Phrase(phrase) => {
				write!(f, "{}:\"{}\"", phrase.field, phrase.value)
			}
			Clause::Defaultable(d) => write!(f, "{}", d.clone()),
		}
	}
}

// https://quickwit.io/docs/configuration/index-config#field-name-validation-rules
fn is_valid_string(input: &str) -> bool {
	input
		.chars()
		.all(|c| c.is_ascii_alphanumeric() || ".-_/@$".contains(c))
}

pub enum Unary {
	Pos(Clause),
	Neg(Clause),
}

pub enum Query {
	C(Unary),
	And(Box<Query>, Box<Query>),
	Or(Box<Query>, Box<Query>),
}

impl Default for Query {
	fn default() -> Self {
		Query::C(Unary::Pos(Clause::Defaultable("*".to_string())))
	}
}

impl fmt::Display for Query {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Query::And(l, r) => {
				write!(f, "({} AND {})", l, r)
			}
			Query::Or(l, r) => {
				write!(f, "({} OR {})", l, r)
			}
			Query::C(u) => match u {
				Unary::Pos(pos) => write!(f, "{}", pos),
				Unary::Neg(neg) => {
					write!(f, "-{}", neg)
				}
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use pretty_assertions::assert_eq;

	#[test]
	fn test_to_query() {
		let test_cases = [
			(
				Query::And(
					Box::new(Query::C(Unary::Pos(Clause::Term(TermCtx {
						field: "foo".to_string(),
						value: serde_json::json!("bar"),
					})))),
					Box::new(Query::Or(
						Box::new(Query::C(Unary::Pos(Clause::Term(TermCtx {
							field: "baz".to_string(),
							value: serde_json::json!("fuzz"),
						})))),
						Box::new(Query::C(Unary::Neg(Clause::Term(TermCtx {
							field: "tt".to_string(),
							value: serde_json::json!(15),
						})))),
					)),
				),
				r#"(foo:bar AND (baz:fuzz OR -tt:15))"#,
			),
			(
				Query::C(Unary::Pos(Clause::Defaultable("bar".to_string()))),
				r#"bar"#,
			),
			(
				Query::C(Unary::Pos(Clause::Defaultable(
					"I love football".to_string(),
				))),
				r#"I love football"#,
			),
			(
				Query::Or(
					Box::new(Query::C(Unary::Pos(Clause::Term(TermCtx {
						field: "foo".to_string(),
						value: serde_json::json!("bar"),
					})))),
					Box::new(Query::And(
						Box::new(Query::C(Unary::Pos(Clause::Phrase(
							PhraseCtx {
								field: "baz".to_string(),
								value: "fuzz asd".to_string(),
							},
						)))),
						Box::new(Query::C(Unary::Neg(Clause::Term(TermCtx {
							field: "tt".to_string(),
							value: serde_json::json!(15),
						})))),
					)),
				),
				r#"(foo:bar OR (baz:"fuzz asd" AND -tt:15))"#,
			),
			(Query::default(), "*"),
		];
		for (q, expected) in test_cases {
			let actual = q.to_string();
			assert_eq!(expected, actual);
		}
	}
}
