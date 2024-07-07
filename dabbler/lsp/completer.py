from ctypes import Union
import logging
import os
from pathlib import Path
import re as regex
from dabbler.common import grammer_kw
from dabbler.lsp.parser import SqlParserNew
from dabbler.lsp.db_data import make_db, make_completion_map
from dabbler.lsp.sql_utils import strip_sql_whitespace, SelectNode, CmpItem
from typing import Union
from dabbler.lsp.completion_utils import (
    PathCompleter,
    duckdb_extensions,
    duckdb_settings,
    duckdb_pragmas,
    duckdb_types,
    file_path_completion_regex,
)
from lsprotocol.types import (
    CompletionItem,
    CompletionList,
    CompletionItemKind,
)

table_types = set(["table", "database", "schema", "cte", "table_macro"])
kw_replace = {
    'ORDER': 'ORDER BY',
    'GROUP': 'GROUP BY',
    'PARTITION': 'PARTITION BY',
}
kw_adds = {
    'CREATE' : 'CREATE OR REPLACE',
    'GROUP' : 'GROUP BY ALL',
    'ORDER': 'ORDER BY ALL',
}

class SqlCompleter:
    def __init__(self, db_data) -> None:
        self.db = make_db(db_data)
        self.completion_map = make_completion_map(self.db, db_data)
        self.db_data = db_data
        if Path(db_data["cwd"]).is_dir():
            os.chdir(db_data["cwd"])
        self.path_completer = PathCompleter(db_data["cwd"], db_data["file_search_path"])
        self.file_search_path = db_data["file_search_path"]
        self.log = logging.getLogger("completer")
        self.log_comp_map = self.log.getChild("comp_map")
        self.parser2 = SqlParserNew(self.db, self.file_search_path)

    def get_queries(self, pos, sql):
        queries, choices_pos = self.parser2.parse_sql(sql, pos)   # type: ignore
        if not queries:
            return None, None, choices_pos
        queries.queries_list.sort(key=lambda x: x.end_pos - x.start_pos)
        if len(queries.queries_list) == 0:
            return None, None, choices_pos
        filtered = [x for x in queries.queries_list if x.start_pos <= pos <= x.end_pos]
        if len(filtered) == 0:
            return queries.queries_list[-1], queries, choices_pos
        q = filtered[0]
        return q, queries, choices_pos

    def parse_sql2(self, pos, sql: str):
        comp_map = {}
        comp_map["root_namespace"] = []

        q, queries, choices_pos = self.get_queries(pos, sql)
        
        if choices_pos:
            kw_comps = []
            for c in choices_pos:
                if c in grammer_kw:
                    if c in kw_adds:
                        kw_comps.append(
                            CmpItem(kw_adds[c], CompletionItemKind.Keyword, None, "keyword", "3", "keyword")
                        )

                    c = kw_replace.get(c, c)
                    
                    kw_comps.append(
                        CmpItem(c, CompletionItemKind.Keyword, None, "keyword", "3", "keyword")
                    )
            
            comp_map["root_namespace"].extend(kw_comps)
        
        
        if q is None:
            # self.log_comp_map.debug(comp_map)
            return comp_map
        for k, v in q.from_refs.items():
            if v.kind.name == "subquery":
                if not queries or not v.start_pos:
                    continue
                projection = queries.queries[v.start_pos].projection
                if not projection:
                    continue
                comp_map[k] = [
                    CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                    for x in projection
                ]
                comp_map["root_namespace"].append(
                    CmpItem(k, CompletionItemKind.File, None, "sub query", "1", "table")
                )
                continue
            
            if v.kind.name == "table_function":
                projection = v.projection
                if not projection:
                    continue
                comp_map[k] = [
                    CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                    for x in projection
                ]
                comp_map["root_namespace"].append(
                    CmpItem(k, CompletionItemKind.File, None, "table function", "1", "table")
                )
                continue
            
            # self.show_message_log(f'{v}')
            if q.ctes and v.name in q.ctes.map:
                projection = q.ctes.map[v.name].projection
                if not projection:
                    continue
                comp_map[k] = [
                    CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                    for x in projection
                ]
                comp_map["root_namespace"].append(
                    CmpItem(k, CompletionItemKind.File, None, "cte", "1", "cte")
                )
                continue
            elif v.name in q.cte_sibblings:
                projection = q.cte_sibblings[v.name].projection
                if not projection:
                    continue
                comp_map[k] = [
                    CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                    for x in projection
                ]
                comp_map["root_namespace"].append(
                    CmpItem(k, CompletionItemKind.File, None, "cte", "1", "cte")
                )
                continue
            else:
                if v.name not in self.completion_map:
                    continue
                comp_map[k] = self.completion_map[v.name]
                comp_map["root_namespace"].append(
                    CmpItem(
                        k, CompletionItemKind.File, None, "table_alias", "1", "table"
                    )
                )

        if q.ctes:
            for k, v in q.ctes.map.items():
                projection = v.projection
                if not projection:
                    continue
                comp_map[k] = [
                    CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                    for x in projection
                ]
                comp_map["root_namespace"].append(
                    CmpItem(k, CompletionItemKind.File, None, "cte", "1", "cte")
                )

        for k, v in q.cte_sibblings.items():
            projection = v.projection
            if not projection:
                continue
            comp_map[k] = [
                CmpItem(x[0], CompletionItemKind.Field, None, x[1], "1", "column")
                for x in projection
            ]
            comp_map["root_namespace"].append(
                CmpItem(k, CompletionItemKind.File, None, "cte", "1", "cte")
            )

        # self.show_message_log(f'cte_sibblings {q.cte_sibblings}')

        col_to_add = []
        labels_added = []

        for k in comp_map["root_namespace"]:
            if k in q.from_refs and k in comp_map:
                col_to_add.extend(
                    [x for x in comp_map[k] if x.label not in labels_added]
                )
                labels_added.extend([x.label for x in comp_map[k]])

        comp_map["root_namespace"].extend(col_to_add)

        # self.log_comp_map.debug(comp_map)
        
        return comp_map



    def route_completion2(
        self,
        cursor_pos: int,
        txt: str,
        trigger: Union[str,None],
        # current_line: int,
        # current_line_txt: str,
    ):
        # self.show_message_log(f'route_completion pos:{cursor_pos}, trigger:{trigger}, line:{current_line}, line_txt{current_line_txt}')
        # self.parsed_times_cache.age += 1
        sql_left_of_cur = strip_sql_whitespace(txt[:cursor_pos])
        # self.show_message_log(f'comp_map_size: {sys.getsizeof(self.completion_map)} parser: {sys.getsizeof(self.parser)}, trigger: {trigger}')

        if sql_left_of_cur[-2:] == "::":
            return CompletionList(is_incomplete=False, items=duckdb_types)

        if regex.match(
            r"(^| )(load |install )$", sql_left_of_cur, flags=regex.IGNORECASE
        ):
            return CompletionList(is_incomplete=False, items=duckdb_extensions)

        if regex.match(r"(^| )(pragma )$", sql_left_of_cur, flags=regex.IGNORECASE):
            return CompletionList(is_incomplete=False, items=duckdb_pragmas)

        if regex.match(r"(^| )(set |reset )$", sql_left_of_cur, flags=regex.IGNORECASE):
            return CompletionList(
                is_incomplete=False,
                items=duckdb_settings
                + [CompletionItem(label=c) for c in ["LOCAL", "SESSION", "GLOBAL"]],
            )

        if regex.match(
            r"(^| )(set |reset )(local | session | global )$",
            sql_left_of_cur,
            flags=regex.IGNORECASE,
        ):
            return CompletionList(is_incomplete=False, items=duckdb_settings)

        file_comp = file_path_completion_regex.match(sql_left_of_cur)
        if file_comp:
            items = self.path_completer.get_items(file_comp.group(2))
            return CompletionList(is_incomplete=False, items=items)


        try:
            parsed_items: dict[str, list[CmpItem]] = self.parse_sql2(
                cursor_pos, txt
            )
        except Exception as e:
            # self.log.info(["parsed_items_error", e,sys.exception().__traceback__])
            # self.log.debug('parsed_items_error')

            parsed_items = {"root_namespace": []}
        # self.show_message_log(f'{parsed_items}')
        # comp_map:dict[str,list[CmpItem]] = {}
        # comp_map.update(parsed_items)
        # comp_map.update(self.completion_map.copy())
        # comp_map['root_namespace'].extend(parsed_items['root_namespace'])
        comp_map = self.completion_map

        if regex.match(
            r".*(^| )(join |from |pivot |unpivot |alter table |insert into )(\w+( \w+)?, )*\w?$",
            sql_left_of_cur,
            flags=regex.IGNORECASE,
        ):
            tbls = [
                x.comp for x in comp_map["root_namespace"] if x.obj_type in table_types
            ]
            tbls += [
                x.comp
                for x in parsed_items["root_namespace"]
                if x.obj_type in table_types
            ]
            # ls.show_message_log(f'join {tbls}')
            return CompletionList(is_incomplete=False, items=tbls)

        if trigger == " ":
            return

        m = regex.match(
            r".*(^| )(join |from |pivot |unpivot |alter table |insert into )(\w+( \w+)?, )*(?P<dotitems>(\w+\.)+)$",
            sql_left_of_cur,
            flags=regex.IGNORECASE,
        )
        if m and trigger == ".":
            key = m.group("dotitems").strip(".")
            items = []
            if key in comp_map:
                items = [x.comp for x in comp_map[key] if x.obj_type in table_types]
            if key in parsed_items:
                items += [
                    x.comp for x in parsed_items[key] if x.obj_type in table_types
                ]
            return CompletionList(is_incomplete=False, items=items)

        m = regex.match(
            r".*(^| |\()(?P<dotitems>(\w+\.)+)$", sql_left_of_cur, flags=regex.IGNORECASE
        )
        if m and trigger == ".":
            key = m.group("dotitems").strip(".")
            items = []
            if key in comp_map:
                items += [x.comp for x in comp_map[key]]
            if key in parsed_items:
                items += [x.comp for x in parsed_items[key]]
            if len(items) > 0:
                return CompletionList(is_incomplete=False, items=items)

        m = regex.match(
            r".*(^| |\()(?P<char>(\w+))$", sql_left_of_cur, flags=regex.IGNORECASE
        )
        if m:
            # comp_items = []
            # comp_items.extend(self.completion_map['root_namespace'])
            # comp_items.extend(parsed_items['root_namespace'])
            chars = m.group("char").lower()
            comp_items = [x for x in comp_map["root_namespace"]]
            comp_items += parsed_items["root_namespace"]
            return CompletionList(
                is_incomplete=True,
                items=[x.comp for x in comp_items if x.label.lower().startswith(chars)],
            )

        return None
