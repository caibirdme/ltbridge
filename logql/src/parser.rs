use humantime_serde::re::humantime::parse_duration;
use itertools::Itertools;
use nom::{
	branch::alt,
	bytes::complete::{tag, take_until, take_until1},
	character::complete::{alpha1, alphanumeric1, char, multispace0},
	combinator::{all_consuming, map, map_res, opt, recognize},
	error::ParseError,
	multi::{many0_count, many1, separated_list1},
	sequence::{delimited, pair, preceded, tuple},
	IResult, Parser,
};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub struct LabelPair {
	pub label: String,
	pub op: Operator,
	pub value: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Operator {
	Equal,
	NotEqual,
	RegexMatch,
	RegexNotMatch,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Selector {
	pub label_paris: Vec<LabelPair>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Filter {
	LogLine(LogLineFilter),
	Drop,
}
#[derive(Debug, PartialEq, Eq)]
pub enum FilterType {
	Contain,
	NotContain,
	RegexMatch,
	RegexNotMatch,
}

#[derive(Debug, PartialEq, Eq)]
pub struct LogLineFilter {
	pub op: FilterType,
	pub expression: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct LogQuery {
	pub selector: Selector,
	pub filters: Option<Vec<Filter>>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Aggregator {
	Sum,
	Avg,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum RangeFunction {
	Rate,
	CountOverTime,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Query {
	LogQuery(LogQuery),
	MetricQuery(MetricQuery),
}

#[derive(Debug, PartialEq, Eq)]
pub struct MetricQuery {
	pub aggregator: Aggregator,
	pub agg_func: RangeFunction,
	pub agg_by: Vec<String>,
	pub range: Duration,
	pub log_query: LogQuery,
}

fn parse_agg_func(s: &str) -> IResult<&str, RangeFunction> {
	alt((tag("rate"), tag("count_over_time")))(s).map(|(s, v)| {
		(
			s,
			match v {
				"rate" => RangeFunction::Rate,
				"count_over_time" => RangeFunction::CountOverTime,
				_ => unreachable!(),
			},
		)
	})
}

// sum by (label) xxx
fn parse_metric_query_front_by(s: &str) -> IResult<&str, MetricQuery> {
	tuple((
		ws(aggregator),
		ws(by_label_list),
		delimited(
			ws(tag("(")),
			tuple((
				ws(parse_agg_func),
				delimited(
					ws(tag("(")),
					tuple((logql, time_range)),
					ws(tag(")")),
				),
			)),
			ws(tag(")")),
		),
	))(s)
	.map(|(s, (agg, agg_by, (agg_func, (lq, range))))| {
		(
			s,
			MetricQuery {
				aggregator: agg,
				agg_func,
				agg_by,
				log_query: lq,
				range,
			},
		)
	})
}

// sum xxx by (label)
fn parse_metric_query_tail_by(s: &str) -> IResult<&str, MetricQuery> {
	tuple((
		ws(aggregator),
		delimited(
			ws(tag("(")),
			tuple((
				parse_agg_func,
				delimited(tag("("), tuple((logql, time_range)), tag(")")),
			)),
			ws(tag(")")),
		),
		by_label_list,
	))(s)
	.map(|(s, (agg, (agg_func, (lq, range)), agg_by))| {
		(
			s,
			MetricQuery {
				aggregator: agg,
				agg_func,
				agg_by,
				log_query: lq,
				range,
			},
		)
	})
}

fn parse_metric_query(s: &str) -> IResult<&str, MetricQuery> {
	alt((parse_metric_query_front_by, parse_metric_query_tail_by))(s)
}

fn ws<'a, F, O, E: ParseError<&'a str>>(inner: F) -> impl Parser<&'a str, O, E>
where
	F: Parser<&'a str, O, E>,
{
	delimited(multispace0, inner, multispace0)
}

fn aggregator(s: &str) -> IResult<&str, Aggregator> {
	alt((tag("sum"), tag("avg")))(s).map(|(s, v)| {
		(
			s,
			match v {
				"sum" => Aggregator::Sum,
				"avg" => Aggregator::Avg,
				_ => unreachable!(),
			},
		)
	})
}

fn by_label_list(s: &str) -> IResult<&str, Vec<String>> {
	preceded(
		ws(tag("by")),
		delimited(
			ws(tag("(")),
			separated_list1(ws(tag(",")), identifier),
			ws(tag(")")),
		),
	)(s)
	.map(|(s, arr)| (s, arr.into_iter().map(|s| s.to_string()).collect()))
}

fn identifier(input: &str) -> IResult<&str, &str> {
	recognize(pair(
		alt((alpha1, tag("_"))),
		many0_count(alt((alphanumeric1, tag("_"), tag(".")))),
	))(input)
}

fn time_range(s: &str) -> IResult<&str, Duration> {
	map_res(
		delimited(tag("["), ws(alphanumeric1), tag("]")),
		parse_duration,
	)(s)
}

fn op_eq(s: &str) -> IResult<&str, Operator> {
	let (r, _) = tag("=")(s)?;
	Ok((r, Operator::Equal))
}

fn op_ne(s: &str) -> IResult<&str, Operator> {
	let (r, _) = tag("!=")(s)?;
	Ok((r, Operator::NotEqual))
}

fn op_regexp(s: &str) -> IResult<&str, Operator> {
	let (r, _) = tag("=~")(s)?;
	Ok((r, Operator::RegexMatch))
}

fn op_regexp_not(s: &str) -> IResult<&str, Operator> {
	let (r, _) = tag("!~")(s)?;
	Ok((r, Operator::RegexNotMatch))
}

fn operator(s: &str) -> IResult<&str, Operator> {
	alt((op_regexp, op_regexp_not, op_ne, op_eq))(s)
}

fn label_pair(s: &str) -> IResult<&str, LabelPair> {
	let (r, (ident, op, val)) = tuple((
		identifier,
		ws(operator),
		ws(delimited(char('"'), take_until1("\""), char('"'))),
	))(s)?;
	Ok((
		r,
		LabelPair {
			label: ident.to_string(),
			op,
			value: val.to_string(),
		},
	))
}

fn label_pair_list(s: &str) -> IResult<&str, Vec<LabelPair>> {
	separated_list1(ws(tag(",")), label_pair)(s)
}

fn selector(s: &str) -> IResult<&str, Selector> {
	let (s, list) = delimited(char('{'), ws(label_pair_list), char('}'))(s)?;
	Ok((s, Selector { label_paris: list }))
}

fn filter_type_contain(s: &str) -> IResult<&str, FilterType> {
	let (s, _) = tag("|=")(s)?;
	Ok((s, FilterType::Contain))
}

fn filter_type_not_contain(s: &str) -> IResult<&str, FilterType> {
	let (s, _) = tag("!=")(s)?;
	Ok((s, FilterType::NotContain))
}
fn filter_type_regex_match(s: &str) -> IResult<&str, FilterType> {
	let (s, _) = tag("|~")(s)?;
	Ok((s, FilterType::RegexMatch))
}

fn filter_type_not_regex_match(s: &str) -> IResult<&str, FilterType> {
	let (s, _) = tag("!~")(s)?;
	Ok((s, FilterType::RegexNotMatch))
}

fn filter_type(s: &str) -> IResult<&str, FilterType> {
	alt((
		filter_type_contain,
		filter_type_not_contain,
		filter_type_regex_match,
		filter_type_not_regex_match,
	))(s)
}

fn string_val(s: &str) -> IResult<&str, &str> {
	alt((
		delimited(char('`'), take_until("`"), char('`')),
		delimited(char('"'), take_until("\""), char('"')),
	))(s)
}

fn line_filter(s: &str) -> IResult<&str, Filter> {
	let (s, (t, e)) = tuple((ws(filter_type), ws(string_val)))(s)?;
	Ok((
		s,
		Filter::LogLine(LogLineFilter {
			op: t,
			expression: e.to_string(),
		}),
	))
}

fn drop_filter(s: &str) -> IResult<&str, Filter> {
	map(
		preceded(ws(char('|')), preceded(ws(tag("drop")), ws(identifier))),
		|_| Filter::Drop,
	)(s)
}

fn filter_chain(s: &str) -> IResult<&str, Vec<Filter>> {
	many1(alt((ws(line_filter), ws(drop_filter))))(s)
}

fn logql(s: &str) -> IResult<&str, LogQuery> {
	let (s, (se, chain)) = pair(selector, opt(filter_chain))(s)?;
	Ok((
		s,
		LogQuery {
			selector: se,
			filters: check_opt_vec(chain.map(|c| {
				c.into_iter()
					.filter_map(|v| match &v {
						Filter::LogLine(line) => {
							if line.expression.is_empty() {
								None
							} else {
								Some(v)
							}
						}
						_ => Some(v),
					})
					.collect_vec()
			})),
		},
	))
}

fn check_opt_vec<T>(opt: Option<Vec<T>>) -> Option<Vec<T>> {
	opt.filter(|v| !v.is_empty())
}

pub type LogQLParseError = nom::Err<nom::error::Error<String>>;

fn parse_logql_log_query(s: &str) -> IResult<&str, Query> {
	logql(s).map(|(s, lq)| (s, Query::LogQuery(lq)))
}

fn parse_logql_metric_query(s: &str) -> IResult<&str, Query> {
	parse_metric_query(s).map(|(s, mq)| (s, Query::MetricQuery(mq)))
}

pub fn parse_logql_query(s: &str) -> Result<Query, LogQLParseError> {
	all_consuming(alt((parse_logql_log_query, parse_logql_metric_query)))(s)
		.map(|(_, v)| v)
		.map_err(|e| e.to_owned())
}

#[cfg(test)]
mod tests {
	use super::*;
	use pretty_assertions::assert_eq;

	#[test]
	fn test_drop_filter() {
		let input = "| drop __error__";
		let (s, v) = drop_filter(input).unwrap();
		assert!(s.is_empty());
		assert_eq!(Filter::Drop, v);
		let input = r#"{app="t"} |= `giao` | drop __error__"#;
		let actual = parse_logql_query(input).unwrap();
		let expect = LogQuery {
			selector: Selector {
				label_paris: vec![LabelPair {
					label: "app".to_string(),
					op: Operator::Equal,
					value: "t".to_string(),
				}],
			},
			filters: Some(vec![
				Filter::LogLine(LogLineFilter {
					op: FilterType::Contain,
					expression: "giao".to_string(),
				}),
				Filter::Drop,
			]),
		};
		assert_eq!(Query::LogQuery(expect), actual);
	}
	#[test]
	fn test_drop_filter_metric() {
		let input = r#"sum by (level) (count_over_time({app="t"} |= `giao` | drop __error__[1m]))"#;
		let actual = parse_logql_query(input).unwrap();
		let expect = MetricQuery {
			aggregator: Aggregator::Sum,
			agg_func: RangeFunction::CountOverTime,
			agg_by: vec!["level".to_string()],
			log_query: LogQuery {
				selector: Selector {
					label_paris: vec![LabelPair {
						label: "app".to_string(),
						op: Operator::Equal,
						value: "t".to_string(),
					}],
				},
				filters: Some(vec![
					Filter::LogLine(LogLineFilter {
						op: FilterType::Contain,
						expression: "giao".to_string(),
					}),
					Filter::Drop,
				]),
			},
			range: Duration::from_secs(60),
		};
		assert_eq!(Query::MetricQuery(expect), actual);
	}

	#[test]
	fn test_query_parse_metric_query() {
		let test_cases = vec![
			r#"sum by (name) (rate({tags.foo="baz"} |=`qwe`[5m]))"#,
			r#"sum(rate({tags.foo="baz"} |=`qwe`[5m])) by (name) "#,
		];
		for input in test_cases {
			let actual = parse_logql_query(input).unwrap();
			let expect = MetricQuery {
				aggregator: Aggregator::Sum,
				agg_func: RangeFunction::Rate,
				agg_by: vec!["name".to_string()],
				log_query: LogQuery {
					selector: Selector {
						label_paris: vec![LabelPair {
							label: "tags.foo".to_string(),
							op: Operator::Equal,
							value: "baz".to_string(),
						}],
					},
					filters: Some(vec![Filter::LogLine(LogLineFilter {
						op: FilterType::Contain,
						expression: "qwe".to_string(),
					})]),
				},
				range: Duration::from_secs(300),
			};
			assert_eq!(Query::MetricQuery(expect), actual);
		}
	}

	#[test]
	fn test_query_parse_logquery() {
		let input = r#"{name="foo", level != "info" , qq=~"qq.*\d+", ww!~"\d+qwe" }  |= `hello world` |~ `a.*[^"]q?`  !~`b.*q`!=`foo`  "#;
		let actual = parse_logql_query(input).unwrap();
		let expect = LogQuery {
			selector: Selector {
				label_paris: vec![
					LabelPair {
						label: "name".to_string(),
						op: Operator::Equal,
						value: "foo".to_string(),
					},
					LabelPair {
						label: "level".to_string(),
						op: Operator::NotEqual,
						value: "info".to_string(),
					},
					LabelPair {
						label: "qq".to_string(),
						op: Operator::RegexMatch,
						value: "qq.*\\d+".to_string(),
					},
					LabelPair {
						label: "ww".to_string(),
						op: Operator::RegexNotMatch,
						value: "\\d+qwe".to_string(),
					},
				],
			},
			filters: Some(vec![
				Filter::LogLine(LogLineFilter {
					op: FilterType::Contain,
					expression: "hello world".to_string(),
				}),
				Filter::LogLine(LogLineFilter {
					op: FilterType::RegexMatch,
					expression: "a.*[^\"]q?".to_string(),
				}),
				Filter::LogLine(LogLineFilter {
					op: FilterType::RegexNotMatch,
					expression: "b.*q".to_string(),
				}),
				Filter::LogLine(LogLineFilter {
					op: FilterType::NotContain,
					expression: "foo".to_string(),
				}),
			]),
		};
		assert_eq!(Query::LogQuery(expect), actual);
	}

	#[test]
	fn test_complicate_case() {
		let input = r#"{name="foo", level != "info" , qq=~"qq.*\d+", ww!~"\d+qwe" }  |= `hello world` |~ `a.*[^"]q?`  !~`b.*q`!=`foo`  "#;
		let (remain, actual) = logql(input).unwrap();
		assert!(remain.is_empty());
		let expect = LogQuery {
			selector: Selector {
				label_paris: vec![
					LabelPair {
						label: "name".to_string(),
						op: Operator::Equal,
						value: "foo".to_string(),
					},
					LabelPair {
						label: "level".to_string(),
						op: Operator::NotEqual,
						value: "info".to_string(),
					},
					LabelPair {
						label: "qq".to_string(),
						op: Operator::RegexMatch,
						value: "qq.*\\d+".to_string(),
					},
					LabelPair {
						label: "ww".to_string(),
						op: Operator::RegexNotMatch,
						value: "\\d+qwe".to_string(),
					},
				],
			},
			filters: Some(vec![
				Filter::LogLine(LogLineFilter {
					op: FilterType::Contain,
					expression: "hello world".to_string(),
				}),
				Filter::LogLine(LogLineFilter {
					op: FilterType::RegexMatch,
					expression: "a.*[^\"]q?".to_string(),
				}),
				Filter::LogLine(LogLineFilter {
					op: FilterType::RegexNotMatch,
					expression: "b.*q".to_string(),
				}),
				Filter::LogLine(LogLineFilter {
					op: FilterType::NotContain,
					expression: "foo".to_string(),
				}),
			]),
		};
		assert_eq!(expect, actual);
	}
	#[test]
	fn test_only_selector() {
		let input = r#"{name="foo"}"#;
		let (remain, actual) = logql(input).unwrap();
		assert!(remain.is_empty());
		let expect = LogQuery {
			selector: Selector {
				label_paris: vec![LabelPair {
					label: "name".to_string(),
					op: Operator::Equal,
					value: "foo".to_string(),
				}],
			},
			filters: None,
		};
		assert_eq!(expect, actual);
	}
	#[test]
	fn test_filter_use_quote() {
		let input = r#"{name="foo"} |="qwe" |= `"hello"` |="def""#;
		let (remain, actual) = logql(input).unwrap();
		assert!(remain.is_empty());
		let expect = LogQuery {
			selector: Selector {
				label_paris: vec![LabelPair {
					label: "name".to_string(),
					op: Operator::Equal,
					value: "foo".to_string(),
				}],
			},
			filters: Some(vec![
				Filter::LogLine(LogLineFilter {
					op: FilterType::Contain,
					expression: "qwe".to_string(),
				}),
				Filter::LogLine(LogLineFilter {
					op: FilterType::Contain,
					expression: "\"hello\"".to_string(),
				}),
				Filter::LogLine(LogLineFilter {
					op: FilterType::Contain,
					expression: "def".to_string(),
				}),
			]),
		};
		assert_eq!(expect, actual);
	}

	#[test]
	fn test_empty_filter() {
		let inputs = [r#"{name="foo"}|=``"#, r#"{name="foo"}|="""#];
		for input in inputs.iter() {
			let (remain, actual) = logql(input).unwrap();
			assert!(remain.is_empty());
			let expect = LogQuery {
				selector: Selector {
					label_paris: vec![LabelPair {
						label: "name".to_string(),
						op: Operator::Equal,
						value: "foo".to_string(),
					}],
				},
				filters: None,
			};
			assert_eq!(expect, actual);
		}
	}
	#[test]
	fn test_empty_filter_and_drop() {
		let input = r#"{name="foo"}|=``| drop __error__ |="" |= "hello""#;
		let (remain, actual) = logql(input).unwrap();
		assert!(remain.is_empty());
		let expect = LogQuery {
			selector: Selector {
				label_paris: vec![LabelPair {
					label: "name".to_string(),
					op: Operator::Equal,
					value: "foo".to_string(),
				}],
			},
			filters: Some(vec![
				Filter::Drop,
				Filter::LogLine(LogLineFilter {
					op: FilterType::Contain,
					expression: "hello".to_string(),
				}),
			]),
		};
		assert_eq!(expect, actual);
	}
}
