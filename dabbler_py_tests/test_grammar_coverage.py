"""Statements the grammar should accept, from across DuckDB's syntax.

Completions work best where the grammar parses; elsewhere the completer falls back to
regexes. Known gaps are strict xfails: when a construct is added to duckdb.lark, its
case starts passing and must be taken off KNOWN_GAPS.
"""
import re

import duckdb
import pytest

from dabbler.lsp.parser import sql_parser

CASES = {
    'delete': 'delete from t where a = 1',
    'delete_using': 'delete from t using u where t.a = u.a',
    'delete_returning': 'delete from t where a = 1 returning *',
    'except': 'select a from t except select a from u',
    'intersect_all': 'select a from t intersect all select a from u',
    'union_by_name': 'select a from t union by name select b from u',
    'union_all_by_name': 'select a from t union all by name select b from u',
    'window_clause': 'select sum(a) over w from t window w as (partition by b order by c)',
    'semi_join': 'select * from t semi join u on t.a = u.a',
    'anti_join': 'select * from t anti join u using (a)',
    'grouping_sets': 'select a, b, sum(c) from t group by grouping sets ((a, b), (a), ())',
    'cube': 'select a, b, sum(c) from t group by cube (a, b)',
    'rollup': 'select a, b, sum(c) from t group by rollup (a, b)',
    'lambda_arrow': 'select list_transform(l, x -> x + 1) from t',
    'lambda_keyword': 'select list_transform(l, lambda x: x + 1) from t',
    'lambda_two': 'select list_reduce(l, (x, y) -> x + y) from t',
    'named_arg': "select * from read_csv('x.csv', header := true)",
    'named_arg_eq': "select * from read_csv('x.csv', header = true)",
    'trim_both': "select trim(both 'x' from a) from t",
    'substring_from': 'select substring(a from 2 for 3) from t',
    'position_in': "select position('a' in b) from t",
    'explain': 'explain select 1',
    'explain_analyze': 'explain analyze select 1',
    'describe_table': 'describe t',
    'describe_select': 'describe select * from t',
    'show_tables': 'show tables',
    'show_all_tables': 'show all tables',
    'summarize': 'summarize t',
    'summarize_select': 'summarize select * from t',
    'begin': 'begin transaction',
    'begin_bare': 'begin',
    'commit': 'commit',
    'rollback': 'rollback',
    'create_index': 'create index idx on t (a, b)',
    'create_unique_index': 'create unique index if not exists idx on t (a)',
    'create_secret': "create secret s (type s3, key_id 'x', secret 'y', region 'us-east-1')",
    'create_persistent_secret': 'create persistent secret s (type s3, provider credential_chain)',
    'export_db': "export database 'dir' (format parquet)",
    'vacuum': 'vacuum',
    'analyze': 'analyze',
    'array_type': 'select a::int[] from t',
    'array_type2': 'select cast(a as varchar[]) from t',
    'e_string': "select e'a\\nb'",
    'dollar_string': "select $$a'b$$",
    'alter_add_column': 'alter table t add column b int',
    'macro_default': 'create macro m(a, b := 2) as a + b',
    'attach_options': "attach 'x.db' as x (read_only)",
    'limit_percent': 'select * from t limit 10%',
    'not_like': "select * from t where a not like 'x%'",
    'is_not_distinct': 'select * from t where a is not distinct from b',
    'case_between': 'select (case when a then 1 else 2 end) between 2.000/3.000 and 3.000/2.000',
    'struct_access': 'select s.a.b from t',
    'struct_literal': "select {'a': 1, 'b': 2} as s",
    'list_comprehension': 'select [x + 1 for x in [1, 2, 3]]',
    'count_star_filter': 'select count(*) filter (where a > 1) from t',
    'string_agg_order': "select string_agg(a, ',' order by b) from t",
    'pivot_full': 'pivot t on a using sum(b) group by c',
    'from_first': 'from t select a',
    'from_only': 'from t',
    'insert_by_name': 'insert into t by name select 1 as a',
    'create_table_as': 'create or replace table t as select 1',
    'create_temp_table': 'create temp table t (a int, b varchar)',
    'cte_materialized': 'with x as materialized (select 1) select * from x',
    'recursive_cte': 'with recursive x(n) as (select 1 union all select n + 1 from x where n < 3) select * from x',
    'values_alias': 'select * from (values (1, 2)) v(a, b)',
    'values_stmt': 'values (1, 2), (3, 4)',
    'interval_literal': "select interval '1 day'",
    'interval_expr': 'select interval 1 day',
    'timestamp_literal': "select timestamp '2020-01-01'",
    'negation': 'select -a, not b from t',
    'any_subquery': 'select * from t where a = any (select b from u)',
    'in_list': 'select * from t where a in (1, 2, 3)',
    'nested_func_qualified': 'select main.f(a) from t',
    'set_variable': 'set variable x = 1',
    'getvariable': "select getvariable('x')",
    'order_nulls': 'select * from t order by a desc nulls last, b',
    'offset_limit': 'select * from t limit 10 offset 5',
    'distinct_on': 'select distinct on (a) a, b from t',
    'exclude_star': 'select * exclude (a) from t',
    'replace_star': 'select * replace (a + 1 as a) from t',
    'columns_regex': "select columns('a.*') from t",
    'asof_join': 'select * from t asof join u on t.ts >= u.ts',
    'positional_join': 'select * from t positional join u',
    'copy_to': "copy t to 'x.parquet' (format parquet)",
    'create_view': 'create or replace view v as select 1',
    'drop_table': 'drop table if exists t',
    'comment_on': "comment on table t is 'x'",
    'unnest': 'select unnest([1, 2]) as x',
    'glob_from': "select * from 'data/*.parquet'",
    'try_cast': 'select try_cast(a as int) from t',
    'similar_to': "select * from t where a similar to 'x'",
    'escape_quote_ident': 'select "a b" from t',
    'unary_minus_func': 'select -abs(a) from t',
    'parameter_q': 'select * from t where a = ?',
    'parameter_dollar': 'select * from t where a = $1',
    'select_star_like': "select * like 'a%' from t",
    'group_by_all': 'select a, sum(b) from t group by all',
    'order_by_all': 'select * from t order by all',
    'qualify': 'select * from t qualify row_number() over () = 1',
    'having': 'select a from t group by a having count(*) > 1',
    'nested_subquery': 'select * from (select * from (select 1) a) b',
    'map_literal': "select map {'a': 1}",
    'array_slice': 'select l[1:2] from t',
    'case_simple': "select case a when 1 then 'x' end from t",
    'exists': 'select exists (select 1)',
    'coalesce': 'select coalesce(a, b, 0) from t',
    'is_null': 'select * from t where a is null and b is not null',
    'date_literal': "select date '2020-01-01'",
    'between': 'select * from t where a between 1 and 2',
    'update_from': 'update t set a = u.a from u where t.id = u.id',
    'insert_on_conflict': 'insert into t values (1) on conflict do nothing',
    'insert_returning': 'insert into t values (1) returning a',
    'column_named_date': 'select date, time, timestamp from t',
    'doubled_quote_in_string': "select 'it''s'",
    'int_cast': 'select a::int, a::varchar(10), a::decimal(10, 2) from t',
    'between_arithmetic': 'select * from t where a between 2.000/3.000 and 3.000/2.000',
    'and_or_arithmetic': 'select * from t where a = 1 + 2 and b = 3 * 4 or c',
    'merge_basic': 'merge into t using u on t.id = u.id when matched then update set a = u.a when not matched then insert values (u.id, u.a)',
    'merge_delete': 'merge into t as tt using (select * from u) s on tt.id = s.id when matched and s.flag then delete when matched then update set a = s.a',
    'merge_by_source': 'merge into t using u on t.id = u.id when not matched by source then delete when not matched by target then insert (id, a) values (u.id, u.a)',
    'merge_star': 'merge into t using u on t.id = u.id when matched then update set * when not matched then insert *',
    'merge_using_cols': 'merge into t using u using (id) when matched then update when not matched then insert',
    'merge_do_nothing': 'merge into t using u on t.id = u.id when matched then do nothing',
    'merge_returning': 'merge into t using u on t.id = u.id when matched then delete returning *',
    'merge_with_cte': 'with s as (select 1 as id) merge into t using s on t.id = s.id when matched then delete',
    'pivot_std': "select * from t pivot (sum(b) for a in ('x', 'y'))",
    'pivot_std_group': "select * from t pivot (sum(b) as total, count(*) for a in ('x' as ex, 'y') group by c)",
    'pivot_std_multi': "select * from t pivot (sum(b) for a in ('x', 'y') c in (1, 2))",
    'unpivot_std': 'select * from t unpivot (val for name in (c1, c2))',
    'unpivot_std_nulls': 'select * from t unpivot include nulls (val for name in (c1, c2))',
    'unpivot_std_multi': 'select * from t unpivot ((v1, v2) for name in ((a1, a2) as one, (b1, b2) as two))',
    'pivot_on_in': "pivot t on a in ('x', 'y') using sum(b)",
    'pivot_using_alias': 'pivot t on a using sum(b) as total, max(c) as top group by d',
    'pivot_no_using': 'pivot t on a',
    'pivot_in_cte': 'with p as (pivot t on a using sum(b)) select * from p',
    'pivot_subquery_from': 'select * from (pivot t on a using sum(b))',
    'pivot_on_concat': 'pivot t on a || b using sum(c)',
    'unpivot_columns_star': 'unpivot t on columns(* exclude (id)) into name n value v',
    'unpivot_on_list': 'unpivot t on (a, b) as ab, (c, d) as cd into name n value v1, v2',
    'create_table_constraints': 'create table t (id int primary key, b int not null default 0 check (b > 0), c varchar unique)',
    'create_table_named_constraint': 'create table t (id int, b int, constraint pk primary key (id, b), constraint ck check (b > 0))',
    'create_table_fk': 'create table t (id int references u (id), c int, foreign key (c) references u (id))',
    'create_table_fk_no_cols': 'create table t (id int references u)',
    'create_table_collate': 'create table t (a varchar collate nocase not null)',
    'create_table_default_func': "create table t (id int default nextval('seq') primary key, ts timestamp default current_timestamp)",
    'create_table_generated_typed': 'create table t (a int, b int generated always as (a + 1) virtual not null)',
    'create_table_as_no_data': 'create table t as select * from u with no data',
    'create_table_as_cols': 'create table t (a, b) as select 1, 2',
    'create_table_struct': 'create table t (s struct(a int, b varchar), m map(varchar, int), l int[3])',
    'create_table_qualified_ine': 'create table if not exists db.s.t (a int)',
    'create_table_constraint_first': 'create table t (primary key (a), a int)',
    'rename_star': 'select * rename (a as b) from t',
    'rename_star_qualified': 'select t.* rename (t.a as b) from t',
    'exclude_no_parens': 'select * exclude a from t',
    'exclude_qualified': 'select * exclude (t.a) from t',
    'replace_and_rename': 'select * exclude (c) replace (a + 1 as a) rename (b as bb) from t',
    'all_subquery': 'select * from t where a > all (select b from u)',
    'some_subquery': 'select * from t where a <> some (select b from u)',
    'any_list': 'select 1 = any ([1, 2])',
    'all_list': 'select 1 < all ([2, 3])',
    'columns_lambda': "select columns(c -> c like 'a%') from t",
    'star_columns_unpacked': 'select coalesce(*columns(*)) from t',
}

KNOWN_GAPS: set[str] = set()


def params():
    for name, sql in CASES.items():
        marks = pytest.mark.xfail(strict=True, reason="not in the grammar yet") if name in KNOWN_GAPS else ()
        yield pytest.param(sql, id=name, marks=marks)


@pytest.mark.parametrize("sql", list(params()))
def test_grammar_accepts(sql):
    sql_parser.parse(sql)


@pytest.mark.parametrize("sql", CASES.values(), ids=CASES.keys())
def test_cases_are_valid_duckdb(sql):
    duckdb.connect().extract_statements(sql)


def function_examples():
    """DuckDB's examples for its functions, as SELECT statements DuckDB accepts."""
    con = duckdb.connect()
    rows = con.execute("select examples from duckdb_functions() where len(examples) > 0").fetchall()
    examples = sorted({e for (found,) in rows for e in found})
    statements = []
    for example in examples:
        sql = example if re.match(r"(?i)\s*(select|from|with|pivot|describe)\b", example) else f"select {example}"
        try:
            con.extract_statements(sql)
        except duckdb.Error:
            continue  # some examples are fragments, not statements
        statements.append(sql)
    return statements


def test_grammar_parses_duckdb_function_examples():
    failed = []
    for sql in function_examples():
        try:
            sql_parser.parse(sql)
        except Exception:
            failed.append(sql)
    assert failed == []
