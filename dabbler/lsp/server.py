from typing import Optional
import sqlglot
import time


from lsprotocol import types as lsp
from lsprotocol.types import (
    CompletionList,
)

from dabbler.lsp.parser import sql_parser, with_clause
from lark import UnexpectedToken

from dabbler.lsp.server_classes import InlineSqlLangServer
from dabbler.lsp.sql_utils import (
    get_sql2,
    line_col,
    get_range,
    get_statement,
)
from dabbler.lsp.completion_utils import pathlib_completetions


sql_server = InlineSqlLangServer("dabbler", "v0.2")


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
    # ls.check_sockets()
    if not params:
        return CompletionList(is_incomplete=False, items=[])
    

    document = ls.workspace.get_text_document(params.text_document.uri)
    current_line_txt = document.lines[params.position.line]

    if not params.context:
        return CompletionList(is_incomplete=False, items=[])

    trigger = params.context.trigger_character

    # sql_rng = get_sql(document.source, params.position.line, params.position.character)
    sql_rng = get_sql2(document.source, params.position.line, params.position.character)

    # ls.log_to_client(f'{params}')
    # ls.log_to_client(f'\nsql_rng: {sql_rng.start,sql_rng.end}\nleft of idx{document.source[sql_rng.cur_idx-5:sql_rng.cur_idx]}\n{sql_rng.txt}\n{document.source[sql_rng.start:sql_rng.end]}')


    completer = ls.get_completer(params.text_document.uri)

    if not sql_rng:
        # ls.log_to_client(f"txt {current_line_txt[:params.position.character]}")
        try:
            path_completions = pathlib_completetions(current_line_txt[:params.position.character], completer.pathlib_paths, ls.log)
            if path_completions:
                return CompletionList(is_incomplete=False, items=path_completions)
        except Exception as e:
            ls.log.debug(['pathlib completion error',e])
        return CompletionList(is_incomplete=False, items=[])

    if trigger in ['"',"'"]:
        return CompletionList(is_incomplete=False, items=[])

    pos_in_range = sql_rng.cur_idx - sql_rng.start

    # try:
    comps = completer.route_completion2(
        pos_in_range, sql_rng.txt, trigger
    )
    # ls.log_to_client(f'completion time: {time.time()-start}')

    # except:
    # ls.log_to_client('problem with completion')
    # return None
    if comps:
        return comps
    return CompletionList(is_incomplete=False, items=[])




def publish_syntax_error(ls:InlineSqlLangServer, uri, line, char,  msg):
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

    change = params.content_changes[0]
    if not isinstance(change, lsp.TextDocumentContentChangePartial):
        return
    start_line = change.range.end.line
    char = change.range.end.character
    document = ls.workspace.get_text_document(params.text_document.uri)
    # current_line_txt = document.lines[start_line]
    # ls.log_to_client(f"did change: {start_line,char}, line txt: {current_line_txt}")
    sql_range = get_sql2(document.source, start_line, char)
    if not sql_range:
        # ls.log_to_client(f"did not find sql range at line:{start_line + 1}")
        ls.publish_diagnostics(params.text_document.uri, [])
        return

    try:
        sql_range.txt = sql_range.txt.replace('\r\n','\n')
        # ls.log_to_client(f"sql: {sql_range}")
        start = time.time()
        p = sql_parser.parse(sql_range.txt)
        # ls.log.debug(['parse time',time.time()-start])


    except Exception as e:
        # ls.log.debug(['parse time',time.time()-start])
        rng_start_line, rng_start_col = line_col(
        ls.workspace.get_text_document(params.text_document.uri).source, sql_range.start
        )
        
        if isinstance(e, UnexpectedToken):
            msg = f'Unexpected Token "{e.token}"'
            # ls.log_to_client(f'line: {e.line}, col: {e.column} {msg}')
            publish_syntax_error(ls, params.text_document.uri, rng_start_line+e.line-2, e.column, msg)
            
    
    else:
        ls.publish_diagnostics(params.text_document.uri, [])



@sql_server.feature(lsp.TEXT_DOCUMENT_DID_OPEN)
async def did_open(ls: InlineSqlLangServer, params: lsp.DidOpenTextDocumentParams):

    if not params:
        return
#     p = uri_to_path(params.text_document.uri)

#     sql_server.show_message_log(
# f"""workspace_path: {sql_server.workspace.root_path}
# workspace_uri: {sql_server.workspace.root_uri}
# file_uri: {params.text_document.uri}
# file_path: {p}""")

    ls.log_workspace_info()
    ls.warm_up(params.text_document.uri)


def command_location(ls: InlineSqlLangServer, args: tuple):
    """(document uri, line, character) from the VS Code extension's command arguments.

    The extension passes the document's Uri and the cursor Position; pygls
    hands each command argument to the handler as a separate positional arg.
    """
    try:
        uri_data, pos = args[0], args[1]
        return uri_data["external"], pos["line"], pos["character"]
    except (IndexError, KeyError, TypeError):
        ls.log_to_client(f"unexpected command arguments: {args!r}")
        return None


@sql_server.command(InlineSqlLangServer.CMD_SEND_SQL_TO_GUI)
def send_sql_to_gui(ls: InlineSqlLangServer, *args):
    """Sends the query under the cursor to the dabbler window of the file's IPython session."""
    q = None
    location = command_location(ls, args)
    if location is None:
        return
    uri, line, char = location

    document = ls.workspace.get_text_document(uri)
    sql_rng = get_sql2(document.source, line, char)

    if not sql_rng:
        ls.log_to_client(
            f"did not find sql range at line:{line + 1} char: {char + 1}"
        )
        return

    pos_in_range = sql_rng.cur_idx - sql_rng.start

    source = ls.snapshot_source(uri)
    if source is None:
        ls.notify_user(
            "dabbler: no IPython session has shared its database for this file yet. "
            "Load the extension with %load_ext dabbler.ext and run a cell."
        )
        return

    try:
        q, _, _ = ls.get_completer(uri).get_queries(pos_in_range,sql_rng.txt)
    except Exception as e:
        ls.log.debug(['error getting queries',e])

    if not q:
        ls.log_to_client("did not find query")
        return
    
    sql = f'{with_clause(q.cte_sibblings)}\n{q.sql}'
    
    if not ls.schema_cache.session_active(source):
        ls.notify_user(f"dabbler: the IPython session for {source} isn't running, so the query wasn't sent.")
        return

    ls.schema_cache.push_command(source, "run_sql", sql)

    # ls.log_to_client(f"args: {args}\n")
    return


# @sql_server.feature(lsp.TEXT_DOCUMENT_FORMATTING)
@sql_server.command(InlineSqlLangServer.CMD_FORMAT_CURRENT_STATEMENT)
def format_range(ls: InlineSqlLangServer, *args):
    """Formats the SQL statement under the cursor with sqlglot."""
    location = command_location(ls, args)
    if location is None:
        return
    uri, line, char = location

    document = ls.workspace.get_text_document(uri)
    sql_rng = get_range(document.source, line, char)
    if not sql_rng:
        return

    sql_rng = get_statement(sql_rng, document.source)
    if not sql_rng:
        return

    try:
        p = sqlglot.parse_one(sql_rng.txt)
    except:
        ls.log_to_client("problem parsing sql")
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
    ls.workspace_apply_edit(lsp.ApplyWorkspaceEditParams(edit=lsp.WorkspaceEdit(changes={document.uri: [text_edit]})))
    # ls.log_to_client(f"args: {args}\n")
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
    import os

    # set DABBLER_LSP_LOG to a file path to get a debug log of the language server
    if os.environ.get("DABBLER_LSP_LOG"):
        logging.basicConfig(filename=os.environ["DABBLER_LSP_LOG"], filemode="w", level=logging.DEBUG)
    sql_server.start_io()


if __name__ == "__main__":
    main()
