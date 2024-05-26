use std::{fmt::Display, time::Duration};

use nom::{
	branch::alt,
	bytes::complete::{is_not, tag, tag_no_case, take_while1, take_while_m_n},
	character::{
		complete::{
			alpha1, alphanumeric1, char, i64 as ni64, multispace0, multispace1,
		},
		is_alphanumeric,
	},
	combinator::{
		all_consuming, map, map_opt, map_res, recognize, value, verify,
	},
	error::{FromExternalError, ParseError},
	multi::{fold_many0, many0_count},
	number::complete::double,
	sequence::{delimited, pair, preceded, tuple},
	IResult, Parser,
};
use ordered_float::OrderedFloat;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ComparisonOperator {
	Equal,
	NotEqual,
	GreaterThan,
	GreaterThanOrEqual,
	LessThan,
	LessThanOrEqual,
	RegularExpression,
	NegatedRegularExpression,
}

impl Display for ComparisonOperator {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		use ComparisonOperator::*;
		match self {
			Equal => write!(f, "="),
			NotEqual => write!(f, "!="),
			GreaterThan => write!(f, ">"),
			GreaterThanOrEqual => write!(f, ">="),
			LessThan => write!(f, "<"),
			LessThanOrEqual => write!(f, "<="),
			RegularExpression => write!(f, "REGEXP"),
			NegatedRegularExpression => write!(f, "NOT REGEXP"),
		}
	}
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum LogicalOperator {
	And,
	Or,
}

impl Display for LogicalOperator {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		use LogicalOperator::*;
		match self {
			And => write!(f, "AND"),
			Or => write!(f, "OR"),
		}
	}
}

// parser combinators are constructed from the bottom up:
// first we write parsers for the smallest elements (escaped characters),
// then combine them into larger parsers.

/// Parse a unicode sequence, of the form u{XXXX}, where XXXX is 1 to 6
/// hexadecimal numerals. We will combine this later with parse_escaped_char
/// to parse sequences like \u{00AC}.
fn parse_unicode<'a, E>(input: &'a str) -> IResult<&'a str, char, E>
where
	E: ParseError<&'a str>
		+ FromExternalError<&'a str, std::num::ParseIntError>,
{
	// `take_while_m_n` parses between `m` and `n` bytes (inclusive) that match
	// a predicate. `parse_hex` here parses between 1 and 6 hexadecimal numerals.
	let parse_hex = take_while_m_n(1, 6, |c: char| c.is_ascii_hexdigit());

	// `preceded` takes a prefix parser, and if it succeeds, returns the result
	// of the body parser. In this case, it parses u{XXXX}.
	let parse_delimited_hex = preceded(
		char('u'),
		// `delimited` is like `preceded`, but it parses both a prefix and a suffix.
		// It returns the result of the middle parser. In this case, it parses
		// {XXXX}, where XXXX is 1 to 6 hex numerals, and returns XXXX
		delimited(char('{'), parse_hex, char('}')),
	);

	// `map_res` takes the result of a parser and applies a function that returns
	// a Result. In this case we take the hex bytes from parse_hex and attempt to
	// convert them to a u32.
	let parse_u32 =
		map_res(parse_delimited_hex, move |hex| u32::from_str_radix(hex, 16));

	// map_opt is like map_res, but it takes an Option instead of a Result. If
	// the function returns None, map_opt returns an error. In this case, because
	// not all u32 values are valid unicode code points, we have to fallibly
	// convert to char with from_u32.
	map_opt(parse_u32, std::char::from_u32).parse(input)
}

/// Parse an escaped character: \n, \t, \r, \u{00AC}, etc.
fn parse_escaped_char<'a, E>(input: &'a str) -> IResult<&'a str, char, E>
where
	E: ParseError<&'a str>
		+ FromExternalError<&'a str, std::num::ParseIntError>,
{
	preceded(
		char('\\'),
		// `alt` tries each parser in sequence, returning the result of
		// the first successful match
		alt((
			parse_unicode,
			// The `value` parser returns a fixed value (the first argument) if its
			// parser (the second argument) succeeds. In these cases, it looks for
			// the marker characters (n, r, t, etc) and returns the matching
			// character (\n, \r, \t, etc).
			value('\n', char('n')),
			value('\r', char('r')),
			value('\t', char('t')),
			value('\u{08}', char('b')),
			value('\u{0C}', char('f')),
			value('\\', char('\\')),
			value('/', char('/')),
			value('"', char('"')),
		)),
	)
	.parse(input)
}

/// Parse a backslash, followed by any amount of whitespace. This is used later
/// to discard any escaped whitespace.
fn parse_escaped_whitespace<'a, E: ParseError<&'a str>>(
	input: &'a str,
) -> IResult<&'a str, &'a str, E> {
	preceded(char('\\'), multispace1).parse(input)
}

/// Parse a non-empty block of text that doesn't include \ or "
fn parse_literal<'a, E: ParseError<&'a str>>(
	input: &'a str,
) -> IResult<&'a str, &'a str, E> {
	// `is_not` parses a string of 0 or more characters that aren't one of the
	// given characters.
	let not_quote_slash = is_not("\"\\");

	// `verify` runs a parser, then runs a verification function on the output of
	// the parser. The verification function accepts out output only if it
	// returns true. In this case, we want to ensure that the output of is_not
	// is non-empty.
	verify(not_quote_slash, |s: &str| !s.is_empty()).parse(input)
}

/// A string fragment contains a fragment of a string being parsed: either
/// a non-empty Literal (a series of non-escaped characters), a single
/// parsed escaped character, or a block of escaped whitespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringFragment<'a> {
	Literal(&'a str),
	EscapedChar(char),
	EscapedWS,
}

/// Combine parse_literal, parse_escaped_whitespace, and parse_escaped_char
/// into a StringFragment.
fn parse_fragment<'a, E>(
	input: &'a str,
) -> IResult<&'a str, StringFragment<'a>, E>
where
	E: ParseError<&'a str>
		+ FromExternalError<&'a str, std::num::ParseIntError>,
{
	alt((
		// The `map` combinator runs a parser, then applies a function to the output
		// of that parser.
		map(parse_literal, StringFragment::Literal),
		map(parse_escaped_char, StringFragment::EscapedChar),
		value(StringFragment::EscapedWS, parse_escaped_whitespace),
	))
	.parse(input)
}

/// Parse a string. Use a loop of parse_fragment and push all of the fragments
/// into an output string.
fn parse_string<'a, E>(input: &'a str) -> IResult<&'a str, String, E>
where
	E: ParseError<&'a str>
		+ FromExternalError<&'a str, std::num::ParseIntError>,
{
	// fold is the equivalent of iterator::fold. It runs a parser in a loop,
	// and for each output value, calls a folding function on each output value.
	let build_string = fold_many0(
		// Our parser function– parses a single string fragment
		parse_fragment,
		// Our init value, an empty string
		String::new,
		// Our folding function. For each fragment, append the fragment to the
		// string.
		|mut string, fragment| {
			match fragment {
				StringFragment::Literal(s) => string.push_str(s),
				StringFragment::EscapedChar(c) => string.push(c),
				StringFragment::EscapedWS => {}
			}
			string
		},
	);

	// Finally, parse the string. Note that, if `build_string` could accept a raw
	// " character, the closing delimiter " would never match. When using
	// `delimited` with a looping parser (like fold), be sure that the
	// loop won't accidentally match your closing delimiter!
	delimited(char('"'), build_string, char('"')).parse(input)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FieldValue {
	Integer(i64),
	Float(ordered_float::OrderedFloat<f64>),
	String(String),
	Status(StatusCode),
	Duration(Duration),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum StatusCode {
	Unset,
	Ok,
	Err,
}

impl From<StatusCode> for i64 {
	fn from(val: StatusCode) -> i64 {
		use StatusCode::*;
		match val {
			Unset => 0,
			Ok => 1,
			Err => 2,
		}
	}
}

impl Display for StatusCode {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		use StatusCode::*;
		// according to pb definition
		match self {
			Ok => write!(f, "1"),
			Err => write!(f, "2"),
			Unset => write!(f, "0"),
		}
	}
}

impl Display for FieldValue {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			FieldValue::Integer(i) => write!(f, "{}", i),
			FieldValue::Float(v) => write!(f, "{}", v),
			FieldValue::String(s) => write!(f, "'{}'", s),
			FieldValue::Status(s) => write!(f, "{}", s),
			FieldValue::Duration(d) => write!(f, "{}", d.as_nanos()),
		}
	}
}

fn humantime_duration(input: &str) -> IResult<&str, Duration> {
	map_res(
		recognize(pair(ni64, take_while1(|c| is_alphanumeric(c as u8)))),
		|s: &str| s.parse::<humantime::Duration>().map(|hd| hd.into()),
	)(input)
}

fn field_value(input: &str) -> IResult<&str, FieldValue> {
	alt((
		map(ws(humantime_duration), FieldValue::Duration),
		map(ws(ni64), FieldValue::Integer),
		map(ws(double), |v| FieldValue::Float(OrderedFloat(v))),
		map(ws(parse_string), FieldValue::String),
		alt((
			value(FieldValue::Status(StatusCode::Ok), ws(tag("ok"))),
			value(FieldValue::Status(StatusCode::Err), ws(tag("error"))),
			value(FieldValue::Status(StatusCode::Unset), ws(tag("unset"))),
		)),
	))(input)
}

fn parse_comparison_operator(input: &str) -> IResult<&str, ComparisonOperator> {
	use ComparisonOperator::*;
	alt((
		value(NotEqual, tag("!=")),
		value(GreaterThanOrEqual, tag(">=")),
		value(GreaterThan, tag(">")),
		value(LessThanOrEqual, tag("<=")),
		value(LessThan, tag("<")),
		value(RegularExpression, tag("=~")),
		value(NegatedRegularExpression, tag("!~")),
		value(Equal, tag("=")),
	))(input)
}

fn ws<'a, F, O, E: ParseError<&'a str>>(inner: F) -> impl Parser<&'a str, O, E>
where
	F: Parser<&'a str, O, E>,
{
	delimited(multispace0, inner, multispace0)
}

fn identifier(input: &str) -> IResult<&str, &str> {
	recognize(pair(
		alt((alpha1, tag("_"))),
		many0_count(alt((alphanumeric1, tag("_"), tag(".")))),
	))(input)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FieldExpr {
	pub kv: FieldType,
	pub operator: ComparisonOperator,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FieldType {
	Intrinsic(IntrisincField),
	Span(String, FieldValue),
	Resource(String, FieldValue),
	Unscoped(String, FieldValue),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SpanKind {
	Unspecified,
	Internal,
	Server,
	Client,
	Producer,
	Consumer,
}

impl From<SpanKind> for i64 {
	fn from(val: SpanKind) -> i64 {
		use SpanKind::*;
		match val {
			Unspecified => 0,
			Internal => 1,
			Server => 2,
			Client => 3,
			Producer => 4,
			Consumer => 5,
		}
	}
}

fn parse_non_intrisinc_field(input: &str) -> IResult<&str, FieldExpr> {
	map(
		tuple((
			ws(identifier),
			ws(parse_comparison_operator),
			ws(field_value),
		)),
		|(a, b, c)| {
			let t = if a.starts_with("span.") {
				FieldType::Span(a.trim_start_matches("span.").to_string(), c)
			} else if a.starts_with("resource.") {
				FieldType::Resource(
					a.trim_start_matches("resource.").to_string(),
					c,
				)
			} else {
				FieldType::Unscoped(a.to_string(), c)
			};
			FieldExpr { kv: t, operator: b }
		},
	)(input)
}

fn parse_intrinsic_status(input: &str) -> IResult<&str, FieldExpr> {
	use IntrisincField::*;
	map(
		tuple((
			ws(tag("status")),
			ws(parse_comparison_operator),
			ws(alt((
				value(StatusCode::Ok, ws(tag("ok"))),
				value(StatusCode::Err, ws(tag("error"))),
				value(StatusCode::Unset, ws(tag("unset"))),
			))),
		)),
		|(_, b, c)| FieldExpr {
			kv: FieldType::Intrinsic(Status(c)),
			operator: b,
		},
	)(input)
}

fn parse_intrisinc_duration(input: &str) -> IResult<&str, FieldExpr> {
	map(
		tuple((
			ws(alt((tag("duration"), tag("traceDuration")))),
			ws(parse_comparison_operator),
			ws(humantime_duration),
		)),
		|(a, b, c)| {
			use IntrisincField::*;
			let t = match a {
				"duration" => Duraion(c),
				"traceDuration" => TraceDuration(c),
				_ => unreachable!(),
			};
			FieldExpr {
				kv: FieldType::Intrinsic(t),
				operator: b,
			}
		},
	)(input)
}

fn parse_intrinsic_kind(input: &str) -> IResult<&str, FieldExpr> {
	map(
		tuple((
			ws(tag("kind")),
			ws(parse_comparison_operator),
			ws(parse_span_kind),
		)),
		|(_, b, c)| FieldExpr {
			kv: FieldType::Intrinsic(IntrisincField::Kind(c)),
			operator: b,
		},
	)(input)
}

fn parse_intrisinc_common(input: &str) -> IResult<&str, FieldExpr> {
	map(
		tuple((
			ws(alt((
				tag("statusMessage"),
				tag("name"),
				tag("rootName"),
				tag("rootServiceName"),
				tag("serviceName"),
			))),
			ws(parse_comparison_operator),
			ws(parse_string),
		)),
		|(a, b, c)| {
			use IntrisincField::*;
			let t = match a {
				"statusMessage" => StatusMessage(c),
				"name" => Name(c),
				"rootName" => RootName(c),
				"rootServiceName" => RootServiceName(c),
				"serviceName" => ServiceName(c),
				_ => unreachable!(),
			};
			FieldExpr {
				kv: FieldType::Intrinsic(t),
				operator: b,
			}
		},
	)(input)
}

fn parse_span_kind(input: &str) -> IResult<&str, SpanKind> {
	use SpanKind::*;
	alt((
		value(Unspecified, tag_no_case("unspecified")),
		value(Client, tag_no_case("client")),
		value(Server, tag_no_case("server")),
		value(Producer, tag_no_case("producer")),
		value(Consumer, tag_no_case("consumer")),
		value(Internal, tag_no_case("internal")),
	))(input)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum IntrisincField {
	Status(StatusCode),
	StatusMessage(String),
	Duraion(Duration),
	Name(String),
	Kind(SpanKind),
	TraceDuration(Duration),
	RootName(String),
	RootServiceName(String),
	// custom added intrinsic fields
	ServiceName(String),
}

fn field_expr(input: &str) -> IResult<&str, FieldExpr> {
	alt((
		parse_intrinsic_status,
		parse_intrinsic_kind,
		parse_intrisinc_common,
		parse_intrisinc_duration,
		parse_non_intrisinc_field,
	))(input)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SpanSet {
	Expr(FieldExpr),
	Logical(Box<SpanSet>, LogicalOperator, Box<SpanSet>),
}

fn cmp_field_expr(input: &str) -> IResult<&str, SpanSet> {
	alt((
		map(
			tuple((ws(and_field_expr), ws(tag("||")), ws(cmp_field_expr))),
			|(a, _, c)| {
				SpanSet::Logical(Box::new(a), LogicalOperator::Or, Box::new(c))
			},
		),
		ws(and_field_expr),
	))(input)
}

fn and_field_expr(input: &str) -> IResult<&str, SpanSet> {
	alt((
		map(
			tuple((ws(field_expr_spanset), ws(tag("&&")), ws(and_field_expr))),
			|(a, _, c)| {
				SpanSet::Logical(Box::new(a), LogicalOperator::And, Box::new(c))
			},
		),
		ws(field_expr_spanset),
	))(input)
}

fn field_expr_spanset(input: &str) -> IResult<&str, SpanSet> {
	map(ws(field_expr), SpanSet::Expr)(input)
}

fn spanset(input: &str) -> IResult<&str, SpanSet> {
	delimited(ws(char('{')), ws(cmp_field_expr), ws(char('}')))(input)
}

fn spanset_expression(input: &str) -> IResult<&str, Expression> {
	alt((
		map(ws(spanset), Expression::SpanSet),
		delimited(ws(char('(')), ws(expression), ws(char(')'))),
	))(input)
}

fn and_expression(input: &str) -> IResult<&str, Expression> {
	alt((
		map(
			tuple((ws(spanset_expression), ws(tag("&&")), ws(and_expression))),
			|(a, _, c)| {
				Expression::Logical(
					Box::new(a),
					LogicalOperator::And,
					Box::new(c),
				)
			},
		),
		ws(spanset_expression),
	))(input)
}

fn expression(input: &str) -> IResult<&str, Expression> {
	alt((
		map(
			tuple((ws(and_expression), ws(tag("||")), ws(expression))),
			|(a, _, c)| {
				Expression::Logical(
					Box::new(a),
					LogicalOperator::Or,
					Box::new(c),
				)
			},
		),
		ws(and_expression),
	))(input)
}

pub type TraceQLError = nom::Err<nom::error::Error<String>>;

pub fn parse_traceql(input: &str) -> Result<Expression, TraceQLError> {
	all_consuming(expression)(input)
		.map(|(_, v)| v)
		.map_err(|e| e.to_owned())
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Expression {
	SpanSet(SpanSet),
	Logical(Box<Expression>, LogicalOperator, Box<Expression>),
}

#[cfg(test)]
mod tests {
	use super::*;
	use pretty_assertions::assert_eq;
	use ComparisonOperator::*;
	use LogicalOperator::*;

	#[test]
	fn test_status_enum() {
		let input = [
			("ok", StatusCode::Ok),
			("error", StatusCode::Err),
			("unset", StatusCode::Unset),
		];
		for (i, e) in input {
			let input = format!(r#"{{status = {} }}"#, i);
			let (res, actual) = expression(&input).unwrap();
			assert_eq!(res, "");
			assert_eq!(
				actual,
				Expression::SpanSet(SpanSet::Expr(FieldExpr {
					operator: Equal,
					kv: FieldType::Intrinsic(IntrisincField::Status(e)),
				}))
			);
		}
		let input = "{status!=ok}";
		let (res, actual) = expression(input).unwrap();
		assert_eq!(res, "");
		assert_eq!(
			actual,
			Expression::SpanSet(SpanSet::Expr(FieldExpr {
				operator: NotEqual,
				kv: FieldType::Intrinsic(IntrisincField::Status(
					StatusCode::Ok
				)),
			}))
		);
	}

	#[test]
	fn test_human_duration() {
		use std::str::FromStr;
		let input = ["1s", "5m30s", "2h32m4s", "1h30m", "1us"];
		for i in input {
			let (res, actual) = field_value(i).unwrap();
			assert_eq!(res, "", "failed to parse '{}'", i);
			let expected = humantime::Duration::from_str(i).unwrap().into();
			assert_eq!(actual, FieldValue::Duration(expected));
		}
	}

	#[test]
	fn traceql_with_human_time() {
		use std::str::FromStr;
		let input = r#"{foo="abc" && baz > 1h30m  }"#;
		let (res, actual) = expression(input).unwrap();
		assert_eq!(res, "");
		let expected = Expression::SpanSet(SpanSet::Logical(
			Box::new(SpanSet::Expr(FieldExpr {
				kv: FieldType::Unscoped(
					"foo".to_string(),
					FieldValue::String("abc".to_string()),
				),
				operator: Equal,
			})),
			And,
			Box::new(SpanSet::Expr(FieldExpr {
				kv: FieldType::Unscoped(
					"baz".to_string(),
					FieldValue::Duration(
						humantime::Duration::from_str("1h30m").unwrap().into(),
					),
				),
				operator: GreaterThan,
			})),
		));
		assert_eq!(actual, expected);
	}

	#[test]
	fn very_simple_traceql() {
		let input = r#"{foo="bar"}"#;
		let (res, actual) = expression(input).unwrap();
		assert_eq!(res, "");
		let expected = Expression::SpanSet(SpanSet::Expr(FieldExpr {
			kv: FieldType::Unscoped(
				"foo".to_string(),
				FieldValue::String("bar".to_string()),
			),
			operator: Equal,
		}));
		assert_eq!(actual, expected);
	}

	#[test]
	fn logical_order_in_spanset() {
		let input = r#"{a="a" && b>123 || a="aa" && b<456}"#;
		let (res, actual) = expression(input).unwrap();
		assert_eq!(res, "");
		let expected = Expression::SpanSet(SpanSet::Logical(
			Box::new(SpanSet::Logical(
				Box::new(SpanSet::Expr(FieldExpr {
					kv: FieldType::Unscoped(
						"a".to_string(),
						FieldValue::String("a".to_string()),
					),
					operator: Equal,
				})),
				And,
				Box::new(SpanSet::Expr(FieldExpr {
					kv: FieldType::Unscoped(
						"b".to_string(),
						FieldValue::Integer(123),
					),
					operator: GreaterThan,
				})),
			)),
			Or,
			Box::new(SpanSet::Logical(
				Box::new(SpanSet::Expr(FieldExpr {
					kv: FieldType::Unscoped(
						"a".to_string(),
						FieldValue::String("aa".to_string()),
					),
					operator: Equal,
				})),
				And,
				Box::new(SpanSet::Expr(FieldExpr {
					kv: FieldType::Unscoped(
						"b".to_string(),
						FieldValue::Integer(456),
					),
					operator: LessThan,
				})),
			)),
		));
		assert_eq!(actual, expected);
	}

	#[test]
	fn simple_traceql() {
		let input = r#"{foo="bar" && bar!=123} && ({baz=10 && buzz>20} || {qwe=~"ab.*c\\d+"})"#;
		let (res, actual) = expression(input).unwrap();
		assert_eq!(res, "");
		let expected = Expression::Logical(
			Box::new(Expression::SpanSet(SpanSet::Logical(
				Box::new(SpanSet::Expr(FieldExpr {
					kv: FieldType::Unscoped(
						"foo".to_string(),
						FieldValue::String("bar".to_string()),
					),
					operator: Equal,
				})),
				And,
				Box::new(SpanSet::Expr(FieldExpr {
					kv: FieldType::Unscoped(
						"bar".to_string(),
						FieldValue::Integer(123),
					),
					operator: NotEqual,
				})),
			))),
			And,
			Box::new(Expression::Logical(
				Box::new(Expression::SpanSet(SpanSet::Logical(
					Box::new(SpanSet::Expr(FieldExpr {
						kv: FieldType::Unscoped(
							"baz".to_string(),
							FieldValue::Integer(10),
						),
						operator: Equal,
					})),
					And,
					Box::new(SpanSet::Expr(FieldExpr {
						kv: FieldType::Unscoped(
							"buzz".to_string(),
							FieldValue::Integer(20),
						),
						operator: GreaterThan,
					})),
				))),
				Or,
				Box::new(Expression::SpanSet(SpanSet::Expr(FieldExpr {
					kv: FieldType::Unscoped(
						"qwe".to_string(),
						FieldValue::String("ab.*c\\d+".to_string()),
					),
					operator: RegularExpression,
				}))),
			)),
		);
		assert_eq!(actual, expected);
	}

	#[test]
	fn logical_order() {
		let input = [
			r#"{baz=10 && buzz>20} || {qwe=~"ab.*"} && {foo="bar" && bar!=123}"#,
			r#"({baz=10 && buzz>20}) || ({qwe=~"ab.*"}) && {foo="bar" && bar!=123}"#,
			r#"({baz=10 && buzz>20}) || ((({qwe=~"ab.*"}))) && ({foo="bar" && bar!=123})"#,
		];
		let expected = Expression::Logical(
			Box::new(Expression::SpanSet(SpanSet::Logical(
				Box::new(SpanSet::Expr(FieldExpr {
					kv: FieldType::Unscoped(
						"baz".to_string(),
						FieldValue::Integer(10),
					),
					operator: Equal,
				})),
				And,
				Box::new(SpanSet::Expr(FieldExpr {
					kv: FieldType::Unscoped(
						"buzz".to_string(),
						FieldValue::Integer(20),
					),
					operator: GreaterThan,
				})),
			))),
			Or,
			Box::new(Expression::Logical(
				Box::new(Expression::SpanSet(SpanSet::Expr(FieldExpr {
					kv: FieldType::Unscoped(
						"qwe".to_string(),
						FieldValue::String("ab.*".to_string()),
					),
					operator: RegularExpression,
				}))),
				And,
				Box::new(Expression::SpanSet(SpanSet::Logical(
					Box::new(SpanSet::Expr(FieldExpr {
						kv: FieldType::Unscoped(
							"foo".to_string(),
							FieldValue::String("bar".to_string()),
						),
						operator: Equal,
					})),
					And,
					Box::new(SpanSet::Expr(FieldExpr {
						kv: FieldType::Unscoped(
							"bar".to_string(),
							FieldValue::Integer(123),
						),
						operator: NotEqual,
					})),
				))),
			)),
		);
		for s in input {
			let (res, actual) = expression(s).unwrap();
			assert_eq!(res, "");
			assert_eq!(actual, expected);
		}
	}

	#[test]
	fn test_serde_logic_op() {
		use ComparisonOperator::*;
		let test_cases = [(Equal, "="), (NotEqual, "!=")];
		for (op, expect) in test_cases {
			assert_eq!(format!("{}", op), expect.to_string());
		}
	}

	#[test]
	fn test_failed_case_1() {
		let input =
			r#"{resource.app="camp" && duration > 1m30s && status!=ok}"#;
		let expr = parse_traceql(input).unwrap();
		let expect = Expression::SpanSet(SpanSet::Logical(
			Box::new(SpanSet::Expr(FieldExpr {
				kv: FieldType::Resource(
					"app".to_string(),
					FieldValue::String("camp".to_string()),
				),
				operator: Equal,
			})),
			LogicalOperator::And,
			Box::new(SpanSet::Logical(
				Box::new(SpanSet::Expr(FieldExpr {
					kv: FieldType::Intrinsic(IntrisincField::Duraion(
						Duration::from_secs(90),
					)),
					operator: ComparisonOperator::GreaterThan,
				})),
				LogicalOperator::And,
				Box::new(SpanSet::Expr(FieldExpr {
					kv: FieldType::Intrinsic(IntrisincField::Status(
						StatusCode::Ok,
					)),
					operator: ComparisonOperator::NotEqual,
				})),
			)),
		));
		assert_eq!(expect, expr);
	}
}
