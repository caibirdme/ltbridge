# LogQL Overview

this crate parse [grafana logql](https://grafana.com/docs/loki/latest/query/) into AST, which can then be used for generating queries in sql or any other DSL.

For now, this crate only supports a subset(most of the grammar except aggregation) of the standard logql, you can see the unit tests to know more.
