import sys
import asyncio
from typing import Optional
import sqlglot
import time

from lsprotocol import types as lsp
from lsprotocol.types import (
    CompletionList,
)

from dabbler.common import FromLangServer
from dabbler.lsp.parser import sql_parser
from lark import UnexpectedToken

from dabbler.lsp.server_classes import InlineSqlLangServer
from dabbler.lsp.sql_utils import (
    get_sql2,
    line_col,
    get_range,
    get_statement,
)
from dabbler.lsp.completion_utils import pathlib_completetions


if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
sql_server = InlineSqlLangServer("pygls-json-example", "v0.1")


# list("abcdefghijklmnopqrstuvwxyz")+


# trigger_characters=[":",'.',' '],
@sql_server.thread()
@sql_server.feature(
    lsp.TEXT_DOCUMENT_COMPLETION,
    lsp.CompletionOptions(
        trigger_characters=[":", ".",'/', ' ','"',"'"],
    ),  # all_commit_characters=[":"]
)
def completions(
    ls: InlineSqlLangServer, params: Optional[lsp.CompletionParams] = None
) -> CompletionList:
    """Returns completion items."""
    # ls.zmq_check_for_update()
    # ls.check_sockets()
    if not params:
        return CompletionList(is_incomplete=False, items=[])
    

    document = ls.workspace.get_document(params.text_document.uri)
    current_line_txt = document.lines[params.position.line]

    if not params.context:
        return CompletionList(is_incomplete=False, items=[])

    trigger = params.context.trigger_character

    # sql_rng = get_sql(document.source, params.position.line, params.position.character)
    sql_rng = get_sql2(document.source, params.position.line, params.position.character)

    # ls.show_message_log(f'{params}')
    # ls.show_message_log(f'\nsql_rng: {sql_rng.start,sql_rng.end}\nleft of idx{document.source[sql_rng.cur_idx-5:sql_rng.cur_idx]}\n{sql_rng.txt}\n{document.source[sql_rng.start:sql_rng.end]}')


    if not sql_rng:
        # ls.show_message_log(f"txt {current_line_txt[:params.position.character]}")
        try:
            path_completions = pathlib_completetions(current_line_txt[:params.position.character], ls.pathlibs_paths, ls.log)
            if path_completions:
                return CompletionList(is_incomplete=False, items=path_completions)
        except Exception as e:
            ls.log.debug(['pathlib completion error',e])
        return CompletionList(is_incomplete=False, items=[])

    if trigger in ['"',"'"]:
        return CompletionList(is_incomplete=False, items=[])

    pos_in_range = sql_rng.cur_idx - sql_rng.start

    if not ls.completer:
        return CompletionList(is_incomplete=False, items=[])

    # try:
    comps = ls.completer.route_completion2(
        pos_in_range, sql_rng.txt, trigger
    )
    # ls.show_message_log(f'completion time: {time.time()-start}')

    # except:
    # ls.show_message_log('problem with completion')
    # return None
    if comps:
        return comps
    return CompletionList(is_incomplete=False, items=[])




def publish_diagnostics(ls:InlineSqlLangServer, uri, line, char,  msg):
    d = lsp.Diagnostic(
            range=lsp.Range(
                start=lsp.Position(line=line, character=char),
                end=lsp.Position(line=line, character=char+1),
            ),
            message=msg,
            source='inline_sql',
        )
    ls.publish_diagnostics(uri,[d])


@sql_server.thread()
@sql_server.feature(lsp.TEXT_DOCUMENT_DID_CHANGE)
def did_change(ls: InlineSqlLangServer, params: lsp.DidChangeTextDocumentParams):
    """Text document did change notification."""
    if not params:
        return
    
    if not params.content_changes:
        return

    if not params.content_changes[0].range: # type: ignore
        return
    # ls.show_message_log(f'did change: {params}')
    # ls.show_message_log(f'did change: {params.content_changes[0].range.start}')
    start_line = params.content_changes[0].range.end.line # type: ignore
    char = params.content_changes[0].range.end.character # type: ignore
    document = ls.workspace.get_document(params.text_document.uri)
    # current_line_txt = document.lines[start_line]
    # ls.show_message_log(f"did change: {start_line,char}, line txt: {current_line_txt}")
    sql_range = get_sql2(document.source, start_line, char)
    if not sql_range:
        # ls.show_message_log(f"did not find sql range at line:{start_line + 1}")
        ls.publish_diagnostics(params.text_document.uri, [])
        return

    try:
        sql_range.txt = sql_range.txt.replace('\r\n','\n')
        # ls.show_message_log(f"sql: {sql_range}")
        start = time.time()
        p = sql_parser.parse(sql_range.txt)
        # ls.log.debug(['parse time',time.time()-start])


    except Exception as e:
        # ls.log.debug(['parse time',time.time()-start])
        rng_start_line, rng_start_col = line_col(
        ls.workspace.get_document(params.text_document.uri).source, sql_range.start
        )
        
        if isinstance(e, UnexpectedToken):
            msg = f'Unexpected Token "{e.token}"'
            # ls.show_message_log(f'line: {e.line}, col: {e.column} {msg}')
            publish_diagnostics(ls, params.text_document.uri, rng_start_line+e.line-2, e.column, msg)
            
    
    else:
        ls.publish_diagnostics(params.text_document.uri, [])



@sql_server.feature(lsp.TEXT_DOCUMENT_DID_OPEN)
async def did_open(ls: InlineSqlLangServer, params: lsp.DidOpenTextDocumentParams):
    ls.log_workspace_info()
    ls.save_key_file()
    ls.create_default_compelter()


@sql_server.command(InlineSqlLangServer.CMD_SEND_SQL_TO_GUI)
def send_sql_to_gui(ls: InlineSqlLangServer, *args):
    """Starts counting down and showing message asynchronously.
    It won't `block` the main thread, which can be tested by trying to show
    completion items.
    """
    q = None
    try:
        uri_data = args[0][0]
        pos = args[0][1]
    except:
        ls.show_message_log("send cmd gui: error getting uri and pos")
        return

    if "line" not in pos or "character" not in pos:
        ls.show_message_log("did not find line/charager in pos")
        return

    if "external" not in uri_data:
        ls.show_message_log("problem obtaining document uri")
        return

    uri = uri_data["external"]
    line = pos["line"]
    char = pos["character"]

    document = ls.workspace.get_document(uri)
    sql_rng = get_sql2(document.source, line, char)

    if not sql_rng:
        ls.show_message_log(
            f"did not find sql range at line:{line + 1} char: {char + 1}"
        )
        return

    pos_in_range = sql_rng.cur_idx - sql_rng.start

    try:
        if not ls.completer:
            return
        q, queries, choices_pos = ls.completer.get_queries(pos_in_range,sql_rng.txt)
    except Exception as e:
        ls.log.debug(['error getting queries',e])

    if not q:
        ls.show_message_log("did not find query")
        return
    
    cte_sql = ''
    if q.cte_sibblings:
        sibblings = ',\n'.join([f'{k} as ({v.sql})' for k,v in q.cte_sibblings.items()])
        cte_sql = f'with {sibblings}'
    sql = f'{cte_sql}\n{q.sql}'
    
    msg:FromLangServer = {"cmd": "run_sql", "data": sql, "con_id": ls.connection_info['server_id']}

    resp = ls.zmq_send(msg)

    # ls.show_message_log(f"args: {args}\n")
    return


# @sql_server.feature(lsp.TEXT_DOCUMENT_FORMATTING)
@sql_server.command(InlineSqlLangServer.CMD_FORMAT_CURRENT_STATEMENT)
def format_range(ls: InlineSqlLangServer, *args):
    """Starts counting down and showing message asynchronously.
    It won't `block` the main thread, which can be tested by trying to show
    completion items.
    """
    try:
        uri_data = args[0][0]
        pos = args[0][1]
    except:
        ls.show_message_log("send cmd gui: error getting uri and pos")
        return

    if "line" not in pos or "character" not in pos:
        ls.show_message_log("did not find line/charager in pos")
        return

    if "external" not in uri_data:
        ls.show_message_log("problem obtaining document uri")
        return

    uri = uri_data["external"]
    line = pos["line"]
    char = pos["character"]

    document = ls.workspace.get_document(uri)
    sql_rng = get_range(document.source, line, char)
    if not sql_rng:
        return

    sql_rng = get_statement(sql_rng, document.source)
    if not sql_rng:
        return

    try:
        p = sqlglot.parse_one(sql_rng.txt)
    except:
        ls.show_message_log("problem parsing sql")
        return

    sql = p.sql(pretty=True, dialect="duckdb", pad=4, indent=4)
    sql = "\n".join([f"    {x}" for i, x in enumerate(sql.split("\n"))]) + "\n"
    s_line, s_char = line_col(document.source, sql_rng.start)
    e_line, e_char = line_col(document.source, sql_rng.end)
    rng = lsp.Range(lsp.Position(s_line - 1, 0), lsp.Position(e_line - 1, 0))

    text_edit = lsp.TextEdit(rng, sql)

    # ls.send_notification('textDocument/didChange', {
    # 'textDocument': {
    #     'uri': document.uri,
    #     'version': document.version
    # },
    # 'contentChanges': [{
    #     'range': rng,
    #     'rangeLength': len(text_edit.range),
    #     'text': text_edit.new_text
    # }]
    # })
    ls.apply_edit(lsp.WorkspaceEdit(changes={document.uri: [text_edit]}))
    # ls.show_message_log(f"args: {args}\n")
    return [text_edit]


# validation_error = []

# def get_validations():
#     return validation_error

# @json_server.feature(
#     lsp.TEXT_DOCUMENT_DIAGNOSTIC,
#     lsp.DiagnosticOptions(
#         identifier="jsonServer",
#         inter_file_dependencies=False,
#         workspace_diagnostics=False,
#     ),
# )
# def text_document_diagnostic(ls:InlineSqlLangServer,
#     params: lsp.DocumentDiagnosticParams,
# ) -> lsp.DocumentDiagnosticReport:
#     """Returns diagnostic report."""
#     document = json_server.workspace.get_document(params.text_document.uri)
#     ls.show_message(f"val: {validation_error}")
#     return lsp.RelatedFullDocumentDiagnosticReport(
#         items=get_validations(),
#         kind=lsp.DocumentDiagnosticReportKind.Full,
#     )

# @json_server.feature(lsp.WORKSPACE_DIAGNOSTIC)
# def workspace_diagnostic(ls,
#     params: lsp.WorkspaceDiagnosticParams,
# ) -> lsp.WorkspaceDiagnosticReport:
#     """Returns diagnostic report."""
#     first = list(json_server.workspace._docs.keys())[0]
#     document = json_server.workspace.get_document(first)
#     ls.show_message(f"val: {validation_error}")
#     return lsp.WorkspaceDiagnosticReport(
#         items=[
#             lsp.WorkspaceFullDocumentDiagnosticReport(
#                 uri=document.uri,
#                 items=validation_error,
#                 kind=lsp.DocumentDiagnosticReportKind.Full,
#             )
#         ]
#     )


def main():
    import logging
    from pathlib import Path

    # logging.basicConfig(filename=Path(__file__).parent.joinpath("pygls.log"), filemode="w", level=logging.DEBUG)
    sql_server.start_io()
    # sql_server.check_sockets2()
    # sql_server.create_sockets2()


if __name__ == "__main__":
    main()
