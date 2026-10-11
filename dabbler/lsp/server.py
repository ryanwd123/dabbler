import asyncio
from typing import Optional

from lsprotocol import types as lsp
from lsprotocol.types import CompletionList

from dabbler import __version__
from dabbler.common import uri_to_path
from dabbler.lsp.completion_utils import pathlib_completetions
from dabbler.lsp.diagnostics import sql_errors
from dabbler.lsp.directives import directive_completions
from dabbler.lsp.formatting import FormatError, format_region
from dabbler.lsp.parser import with_clause
from dabbler.lsp.server_classes import InlineSqlLangServer
from dabbler.lsp.sql_regions import region_at
from dabbler.lsp.sql_utils import sql_range_at


sql_server = InlineSqlLangServer("dabbler", __version__)

# seconds of no typing before a document's SQL is checked for syntax errors
DIAGNOSTICS_DELAY = 0.3


@sql_server.thread()
@sql_server.feature(
    lsp.TEXT_DOCUMENT_COMPLETION,
    lsp.CompletionOptions(
        trigger_characters=[":", ".",'/', ' ','"',"'"],
    ),
)
def completions(
    ls: InlineSqlLangServer, params: Optional[lsp.CompletionParams] = None
) -> CompletionList:
    """Returns completion items."""
    if not params:
        return CompletionList(is_incomplete=False, items=[])

    document = ls.workspace.get_text_document(params.text_document.uri)
    current_line_txt = document.lines[params.position.line]

    if not params.context:
        return CompletionList(is_incomplete=False, items=[])

    trigger = params.context.trigger_character

    sql_rng = sql_range_at(document.source, document.offset_at_position(params.position))
    completer = ls.get_completer(params.text_document.uri)

    if not sql_rng:
        directive = directive_completion_list(ls, document, params.position)
        if directive is not None:
            return directive
        try:
            path_completions = pathlib_completetions(current_line_txt[:params.position.character], completer.pathlib_paths, ls.log)
            if path_completions:
                return CompletionList(is_incomplete=False, items=path_completions)
        except Exception as e:
            ls.log.debug(['pathlib completion error',e])
        return CompletionList(is_incomplete=False, items=[])

    if trigger == '"':
        return CompletionList(is_incomplete=False, items=[])

    pos_in_range = sql_rng.cur_idx - sql_rng.start

    comps = completer.route_completion2(pos_in_range, sql_rng.txt, trigger, snippets=ls.snippet_support())
    if comps:
        span = completer.replace_span(pos_in_range, sql_rng.txt)
        if span is not None:
            # the editor's word range would leave out a quote or stop at punctuation in a file name
            start, end = span
            rng = lsp.Range(
                document.client_position_at_offset(sql_rng.start + start),
                document.client_position_at_offset(sql_rng.start + end),
            )
            quoted = sql_rng.txt[start:start + 1] == '"'
            for item in comps.items:
                item.text_edit = lsp.TextEdit(rng, item.insert_text or item.label)
                item.filter_text = '"' + item.label.strip('"') if quoted else item.filter_text or item.label
        return comps
    return CompletionList(is_incomplete=False, items=[])


def directive_completion_list(ls: InlineSqlLangServer, document, position: lsp.Position) -> Optional[CompletionList]:
    """Completions in a `# dabbler use '<file>'` comment."""
    line_start = document.offset_at_position(lsp.Position(position.line, 0))
    column = document.offset_at_position(position) - line_start
    try:
        document_path = str(uri_to_path(document.uri))
    except ValueError:
        document_path = None
    found = directive_completions(document.lines[position.line], column, document_path)
    if found is None:
        return None
    rng = lsp.Range(
        document.client_position_at_offset(line_start + found.start),
        document.client_position_at_offset(line_start + found.end),
    )
    for item in found.items:
        item.text_edit = lsp.TextEdit(rng, item.insert_text or item.label)
    return CompletionList(is_incomplete=False, items=found.items)


@sql_server.thread()
@sql_server.feature(
    lsp.TEXT_DOCUMENT_SIGNATURE_HELP,
    lsp.SignatureHelpOptions(trigger_characters=["(", ","], retrigger_characters=[")"]),
)
def signature_help(ls: InlineSqlLangServer, params: lsp.SignatureHelpParams) -> Optional[lsp.SignatureHelp]:
    """Parameters of the function whose argument list the cursor is in."""
    document = ls.workspace.get_text_document(params.text_document.uri)
    sql_rng = sql_range_at(document.source, document.offset_at_position(params.position))
    if not sql_rng:
        return None
    try:
        return ls.get_completer(params.text_document.uri).signature_help(sql_rng.cur_idx - sql_rng.start, sql_rng.txt)
    except Exception:
        ls.log.exception("signature help failed")
        return None


@sql_server.thread()
@sql_server.feature(lsp.TEXT_DOCUMENT_HOVER)
def hover(ls: InlineSqlLangServer, params: lsp.HoverParams) -> Optional[lsp.Hover]:
    """Docs for functions, columns of tables and CTEs, and column types."""
    document = ls.workspace.get_text_document(params.text_document.uri)
    sql_rng = sql_range_at(document.source, document.offset_at_position(params.position))
    if not sql_rng:
        return None
    try:
        found = ls.get_completer(params.text_document.uri).hover(sql_rng.cur_idx - sql_rng.start, sql_rng.txt)
    except Exception:
        ls.log.exception("hover failed")
        return None
    if found is None:
        return None
    return lsp.Hover(
        contents=lsp.MarkupContent(kind=lsp.MarkupKind.Markdown, value=found.markdown),
        range=lsp.Range(
            document.client_position_at_offset(sql_rng.start + found.start),
            document.client_position_at_offset(sql_rng.start + found.end),
        ),
    )


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
        diagnostics += directive_diagnostics(ls, uri)
        if ls.workspace.get_text_document(uri).version != version:
            return  # edited meanwhile; the newer check publishes
        ls.publish_diagnostics(uri, diagnostics, version)
    except Exception:
        ls.log.exception(f"failed to check SQL in {uri}")


def directive_diagnostics(ls: InlineSqlLangServer, uri: str) -> list[lsp.Diagnostic]:
    """A warning on a `# dabbler use` comment whose database file can't be read."""
    found = ls.document_database(uri)
    if found is None or not found.database.error:
        return []
    document = ls.workspace.get_text_document(uri)
    return [lsp.Diagnostic(
        range=lsp.Range(
            start=document.client_position_at_offset(found.directive.start),
            end=document.client_position_at_offset(found.directive.end),
        ),
        message=f"dabbler: {found.database.error}",
        severity=lsp.DiagnosticSeverity.Warning,
        source="dabbler",
    )]


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

    source = ls.session_source(uri)
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


@sql_server.command(InlineSqlLangServer.CMD_FORMAT_CURRENT_STATEMENT)
def format_range(ls: InlineSqlLangServer, *args):
    """Formats the SQL string under the cursor with sqlglot's DuckDB dialect."""
    location = command_location(ls, args)
    if location is None:
        return
    uri, line, char = location

    document = ls.workspace.get_text_document(uri)
    region = region_at(document.source, document.offset_at_position(lsp.Position(line, char)))
    if region is None:
        ls.notify_user("dabbler: the cursor isn't in an SQL string.", lsp.MessageType.Info)
        return
    try:
        edit = format_region(document.source, region)
    except FormatError as e:
        ls.notify_user(f"dabbler: couldn't format the SQL: {e}", lsp.MessageType.Info)
        return

    text_edit = lsp.TextEdit(
        lsp.Range(document.client_position_at_offset(edit.start), document.client_position_at_offset(edit.end)),
        edit.new_text,
    )
    ls.workspace_apply_edit(lsp.ApplyWorkspaceEditParams(edit=lsp.WorkspaceEdit(changes={document.uri: [text_edit]})))
    return [text_edit]


def main():
    import logging
    import os

    # set DABBLER_LSP_LOG to a file path to get a debug log of the language server
    if os.environ.get("DABBLER_LSP_LOG"):
        logging.basicConfig(filename=os.environ["DABBLER_LSP_LOG"], filemode="w", level=logging.DEBUG)
    sql_server.start_io()


if __name__ == "__main__":
    main()
