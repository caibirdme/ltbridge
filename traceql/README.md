# TraceQL Overview

this crate parse [grafana traceql](https://grafana.com/blog/2023/02/07/get-to-know-traceql-a-powerful-new-query-language-for-distributed-tracing/) into AST, which can then be used for generating queries in sql or any other DSL.

For now, this crate only supports a subset(most of the grammar except aggregation) of the standard traceql, you can see the unit tests to know more.
