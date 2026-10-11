import asyncio
from typing import Optional
import sqlglot


from lsprotocol import types as lsp
from lsprotocol.types import (
    CompletionList,
)

from dabbler.lsp.diagnostics import sql_errors
from dabbler.lsp.parser import with_clause

from dabbler.lsp.server_classes import InlineSqlLangServer
from dabbler.lsp.sql_utils import (
    get_sql2,
    sql_range_at,
    line_col,
    get_range,
    get_statement,
)
from dabbler.lsp.completion_utils import pathlib_completetions


sql_server = InlineSqlLangServer("dabbler", "v0.2")

# seconds of no typing before a document's SQL is checked for syntax errors
DIAGNOSTICS_DELAY = 0.3


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

    sql_rng = sql_range_at(document.source, document.offset_at_position(params.position))

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




def publish_sql_errors(ls: InlineSqlLangServer, uri: str):
    """Checks every SQL string in the document with DuckDB's parser and publishes the errors."""
    try:
        document = ls.workspace.get_text_document(uri)
        version = document.version
        errors = sql_errors(document.source)
        diagnostics = [
            lsp.Diagnostic(
                range=lsp.Range(
                    start=document.client_position_at_offset(e.start),
                    end=document.client_position_at_offset(e.end),
                ),
                message=e.message,
                severity=lsp.DiagnosticSeverity.Error,
                source="dabbler",
            )
            for e in errors
        ]
        if ls.workspace.get_text_document(uri).version != version:
            return  # edited meanwhile; the newer check publishes
        ls.publish_diagnostics(uri, diagnostics, version)
    except Exception:
        ls.log.exception(f"failed to check SQL in {uri}")


def schedule_sql_errors(ls: InlineSqlLangServer, uri: str, delay: float):
    """Checks the document's SQL once typing pauses for `delay` seconds."""
    loop = asyncio.get_running_loop()
    pending = ls.pending_diagnostics.pop(uri, None)
    if pending:
        pending.cancel()
    ls.pending_diagnostics[uri] = loop.call_later(
        delay, lambda: loop.run_in_executor(None, publish_sql_errors, ls, uri)
    )


@sql_server.feature(lsp.TEXT_DOCUMENT_DID_CHANGE)
async def did_change(ls: InlineSqlLangServer, params: lsp.DidChangeTextDocumentParams):
    schedule_sql_errors(ls, params.text_document.uri, DIAGNOSTICS_DELAY)


@sql_server.feature(lsp.TEXT_DOCUMENT_DID_OPEN)
async def did_open(ls: InlineSqlLangServer, params: lsp.DidOpenTextDocumentParams):
    ls.log_workspace_info()
    ls.warm_up(params.text_document.uri)
    schedule_sql_errors(ls, params.text_document.uri, 0)


@sql_server.feature(lsp.TEXT_DOCUMENT_DID_CLOSE)
async def did_close(ls: InlineSqlLangServer, params: lsp.DidCloseTextDocumentParams):
    pending = ls.pending_diagnostics.pop(params.text_document.uri, None)
    if pending:
        pending.cancel()
    ls.publish_diagnostics(params.text_document.uri, [])


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
    sql_rng = sql_range_at(document.source, document.offset_at_position(lsp.Position(line, char)))

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
