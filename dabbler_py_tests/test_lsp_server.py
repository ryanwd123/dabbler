"""End-to-end tests: start the real language server over stdio and talk LSP to it.

These are the safety net for the pygls upgrade, so they only use the protocol,
never the server's internals. The server reads schemas from a schema cache file
that these tests write to directly, standing in for the IPython extension.
"""
import asyncio
import atexit
import os
import shutil
import sys
import tempfile

import pytest
import pytest_lsp
from lsprotocol import types
from pytest_lsp import ClientServerConfig, LanguageClient

from conftest import make_completion_db
from dabbler.db_stuff import get_db_data_new
from dabbler.schema_cache import CACHE_PATH_ENV, SchemaCache

# Keep the tests away from the real schema cache that VS Code and IPython use.
# Each test has its own workspace folder, so they can share this file.
ISOLATED_TEMP = tempfile.mkdtemp(prefix="dabbler_lsp_test_")
atexit.register(shutil.rmtree, ISOLATED_TEMP, ignore_errors=True)
CACHE_PATH = os.path.join(ISOLATED_TEMP, "schema_cache.sqlite3")
SERVER_ENV = {**os.environ, CACHE_PATH_ENV: CACHE_PATH}


@pytest_lsp.fixture(
    config=ClientServerConfig(
        server_command=[sys.executable, "-m", "dabbler.lsp"],
        server_env=SERVER_ENV,
    ),
)
async def client(lsp_client: LanguageClient, tmp_path):
    lsp_client.applied_edits = []

    @lsp_client.feature(types.WORKSPACE_APPLY_EDIT)
    def apply_edit(ls, params: types.ApplyWorkspaceEditParams):
        ls.applied_edits.append(params.edit)
        return types.ApplyWorkspaceEditResult(applied=True)

    lsp_client.initialize_result = await lsp_client.initialize_session(
        types.InitializeParams(
            capabilities=types.ClientCapabilities(),
            root_uri=tmp_path.as_uri(),
            workspace_folders=[types.WorkspaceFolder(uri=tmp_path.as_uri(), name="ws")],
        )
    )
    lsp_client.workspace_path = tmp_path
    yield
    await lsp_client.shutdown_session()


def open_doc(client: LanguageClient, source: str, name="nb.py", version=1):
    uri = (client.workspace_path / name).as_uri()
    client.text_document_did_open(
        types.DidOpenTextDocumentParams(
            text_document=types.TextDocumentItem(uri=uri, language_id="python", version=version, text=source)
        )
    )
    return uri


def position_of(source: str, marker: str):
    idx = source.index(marker) + len(marker)
    before = source[:idx]
    return types.Position(line=before.count("\n"), character=idx - (before.rfind("\n") + 1))


async def complete(client: LanguageClient, uri: str, position: types.Position, trigger=None):
    context = (
        types.CompletionContext(trigger_kind=types.CompletionTriggerKind.TriggerCharacter, trigger_character=trigger)
        if trigger
        else types.CompletionContext(trigger_kind=types.CompletionTriggerKind.Invoked)
    )
    result = await client.text_document_completion_async(
        types.CompletionParams(
            text_document=types.TextDocumentIdentifier(uri=uri), position=position, context=context
        )
    )
    if result is None:
        return []
    items = result.items if isinstance(result, types.CompletionList) else result
    return [x.label for x in items]


@pytest.mark.asyncio
async def test_server_advertises_capabilities(client: LanguageClient):
    # The completion triggers and our two commands are what the VS Code extension relies on.
    caps = client.initialize_result.capabilities
    assert set(caps.completion_provider.trigger_characters) >= {".", " ", ":"}
    assert {"sendSqlToDbDabbler", "dbDabblerFormatCurrentStatement"} <= set(caps.execute_command_provider.commands)


@pytest.mark.asyncio
async def test_keyword_completion(client: LanguageClient):
    source = 'import duckdb\nq = """--sql\nselect * f\n"""\n'
    uri = open_doc(client, source)
    labels = await complete(client, uri, position_of(source, "select * f"))
    assert "FROM" in labels


@pytest.mark.asyncio
async def test_builtin_table_completion_after_from(client: LanguageClient):
    source = 'q = """--sql\nselect * from \n"""\n'
    uri = open_doc(client, source)
    labels = await complete(client, uri, position_of(source, "from "), trigger=" ")
    assert "read_csv" in labels
    assert "read_parquet" in labels


@pytest.mark.asyncio
async def test_cte_column_completion(client: LanguageClient):
    source = 'db.sql("""--sql\nwith x as (select 1 as col_a, 2 as col_b)\nselect x. from x\n""")\n'
    uri = open_doc(client, source)
    labels = await complete(client, uri, position_of(source, "select x."), trigger=".")
    assert {"col_a", "col_b"} <= set(labels)


@pytest.mark.asyncio
async def test_no_sql_completion_outside_sql_strings(client: LanguageClient):
    source = "import os\nos.\n"
    uri = open_doc(client, source)
    labels = await complete(client, uri, position_of(source, "os."), trigger=".")
    assert labels == []


@pytest.mark.asyncio
async def test_syntax_error_diagnostic_and_clear(client: LanguageClient):
    source = 'q = """--sql\nselect * from t\n"""\n'
    uri = open_doc(client, source)

    def change(version, line, start, end, text):
        client.text_document_did_change(
            types.DidChangeTextDocumentParams(
                text_document=types.VersionedTextDocumentIdentifier(uri=uri, version=version),
                content_changes=[
                    types.TextDocumentContentChangePartial(
                        range=types.Range(
                            start=types.Position(line=line, character=start),
                            end=types.Position(line=line, character=end),
                        ),
                        text=text,
                    )
                ],
            )
        )

    await diagnostics_for(client, uri)
    # "select * from t" -> "select * from t wher x": DuckDB reads "wher" as an alias and stops at "x"
    change(2, 1, 15, 15, " wher x")
    diags = await diagnostics_for(client, uri, lambda d: len(d) == 1)
    assert "syntax error" in diags[0].message
    assert (diags[0].range.start.line, diags[0].range.start.character) == (1, 21)
    assert (diags[0].range.end.line, diags[0].range.end.character) == (1, 22)

    # fix it: "wher" -> "where"
    change(3, 1, 20, 20, "e")
    await diagnostics_for(client, uri, lambda d: len(d) == 0)


async def diagnostics_for(client: LanguageClient, uri: str, done=lambda d: True, timeout=5.0):
    """The document's diagnostics once the server has published some that satisfy done."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if uri in client.diagnostics and done(client.diagnostics[uri]):
            return client.diagnostics[uri]
        await asyncio.sleep(0.05)
    raise AssertionError(f"no matching diagnostics; last: {client.diagnostics.get(uri)}")


@pytest.mark.asyncio
async def test_errors_in_every_sql_string_are_reported(client: LanguageClient):
    source = (
        'a = db.sql("select * fro t")\n'
        'b = db.sql("select 1 from t where x qualify row_number() over () = 1")\n'
        'c = """--sql\ndelete from t wher x = 1\n"""\n'
    )
    uri = open_doc(client, source)
    diags = await diagnostics_for(client, uri, lambda d: len(d) == 2)
    starts = sorted((d.range.start.line, d.range.start.character) for d in diags)
    # 'fro' reads as an alias, so the error is at "t"; "wher" likewise, so it's at "x"
    assert starts == [(0, 25), (3, 19)]

    # an edit to the valid string in the middle keeps both errors
    client.text_document_did_change(
        types.DidChangeTextDocumentParams(
            text_document=types.VersionedTextDocumentIdentifier(uri=uri, version=2),
            content_changes=[types.TextDocumentContentChangeWholeDocument(text=source.replace("select 1", "select 2"))],
        )
    )
    await asyncio.sleep(0.6)
    assert len(client.diagnostics[uri]) == 2


@pytest.mark.asyncio
async def test_valid_duckdb_sql_has_no_errors(client: LanguageClient):
    source = (
        'db.sql("select list_transform([1, 2], x -> x + 1)")\n'
        'db.sql("delete from t where x = 1")\n'
        'db.sql("pivot t on year using sum(amount)")\n'
        'db.sql("select * from t qualify row_number() over (partition by a) = 1")\n'
        'db.sql(f"select * from {table} where a in {values}")\n'
        'db.sql("select 1\\nfrom t")\n'
    )
    uri = open_doc(client, source)
    assert list(await diagnostics_for(client, uri)) == []


@pytest.mark.asyncio
async def test_format_current_statement_command(client: LanguageClient):
    source = 'q = """--sql\nselect a,b from t where a=1\n"""\n'
    uri = open_doc(client, source)
    await client.workspace_execute_command_async(
        types.ExecuteCommandParams(
            command="dbDabblerFormatCurrentStatement",
            arguments=[{"external": uri}, {"line": 1, "character": 5}],
        )
    )
    assert len(client.applied_edits) == 1
    (edit,) = client.applied_edits[0].changes[uri]
    assert (edit.range.start.line, edit.range.end.line) == (1, 2)
    assert edit.new_text == "    SELECT\n        a,\n        b\n    FROM t\n    WHERE\n        a = 1\n"


def save_schema(workspace, notebook="nb.py", extra_sql=None):
    """Writes a snapshot the way the IPython extension does after running a cell."""
    db = make_completion_db()
    if extra_sql:
        db.sql(extra_sql)
    db_data = get_db_data_new(db, skip_ipython=True)
    db_data["cwd"] = str(workspace)
    cache = SchemaCache(CACHE_PATH)
    cache.save_snapshot(workspace / notebook, db_data)
    return cache


@pytest.mark.asyncio
async def test_completions_use_the_files_saved_schema(client: LanguageClient):
    save_schema(client.workspace_path)
    source = 'q = """--sql\nselect o. from orders o join \n"""\n'
    uri = open_doc(client, source)
    assert {"order_id", "amount"} <= set(await complete(client, uri, position_of(source, "select o."), trigger="."))
    assert {"orders", "customers"} <= set(await complete(client, uri, position_of(source, "join "), trigger=" "))


@pytest.mark.asyncio
async def test_newer_schema_is_picked_up_without_restart(client: LanguageClient):
    save_schema(client.workspace_path)
    source = 'q = """--sql\nselect * from \n"""\n'
    uri = open_doc(client, source)
    pos = position_of(source, "from ")
    assert "new_table" not in await complete(client, uri, pos, trigger=" ")

    save_schema(client.workspace_path, extra_sql="create table new_table(x int)")
    assert "new_table" in await complete(client, uri, pos, trigger=" ")


@pytest.mark.asyncio
async def test_helper_module_uses_notebook_schema(client: LanguageClient):
    save_schema(client.workspace_path, notebook="analysis.py")
    source = 'def load(db):\n    return db.sql("select * from ")\n'
    uri = open_doc(client, source, name="lib/helpers.py")
    assert "orders" in await complete(client, uri, position_of(source, "from "), trigger=" ")


@pytest.mark.asyncio
async def test_pathlib_completion_uses_session_paths(client: LanguageClient):
    data = client.workspace_path / "data"
    data.mkdir()
    (data / "sales.csv").write_text("a\n1\n")
    db_data = get_db_data_new(make_completion_db(), skip_ipython=True)
    db_data["paths"] = [["data_dir", str(data)]]
    SchemaCache(CACHE_PATH).save_snapshot(client.workspace_path / "nb.py", db_data)
    source = "df = pl.read_csv(data_dir / '"
    uri = open_doc(client, source)
    assert await complete(client, uri, position_of(source, "/ '"), trigger="'") == ["sales.csv"]


async def send_to_gui(client: LanguageClient, uri: str, line: int):
    await client.workspace_execute_command_async(
        types.ExecuteCommandParams(
            command="sendSqlToDbDabbler", arguments=[{"external": uri}, {"line": line, "character": 3}]
        )
    )


@pytest.mark.asyncio
async def test_send_sql_to_gui_queues_query_for_ipython(client: LanguageClient):
    cache = save_schema(client.workspace_path)
    nb = client.workspace_path / "nb.py"
    cache.heartbeat(nb)
    source = 'q = """--sql\nwith x as (select 1 as a)\nselect a from x\n"""\n'
    uri = open_doc(client, source)
    await send_to_gui(client, uri, line=2)
    (command,) = cache.take_commands(nb)
    assert command.cmd == "run_sql"
    assert "x as (select 1 as a)" in command.data and "select a from x" in command.data


@pytest.mark.asyncio
async def test_send_sql_to_gui_warns_when_ipython_is_not_running(client: LanguageClient):
    cache = save_schema(client.workspace_path)
    source = 'q = """--sql\nselect 1\n"""\n'
    uri = open_doc(client, source)
    await send_to_gui(client, uri, line=1)
    assert cache.take_commands(client.workspace_path / "nb.py") == []
    assert any("isn't running" in m.message for m in client.messages)


@pytest.mark.asyncio
async def test_send_sql_to_gui_warns_without_any_schema(client: LanguageClient):
    source = 'q = """--sql\nselect 1\n"""\n'
    uri = open_doc(client, source)
    await send_to_gui(client, uri, line=1)
    assert any("%load_ext dabbler.ext" in m.message for m in client.messages)


@pytest.mark.asyncio
async def test_commands_with_unexpected_arguments_are_logged_not_crashed(client: LanguageClient):
    for arguments in ([], [{"no_uri": 1}, {"line": 0, "character": 0}]):
        await client.workspace_execute_command_async(
            types.ExecuteCommandParams(command="dbDabblerFormatCurrentStatement", arguments=arguments)
        )
    assert sum("unexpected command arguments" in m.message for m in client.log_messages) == 2
    # still serving requests
    source = 'q = """--sql\nselect * f\n"""\n'
    uri = open_doc(client, source)
    assert "FROM" in await complete(client, uri, position_of(source, "select * f"))


@pytest.mark.asyncio
async def test_signature_help(client: LanguageClient):
    caps = client.initialize_result.capabilities
    assert {"(", ","} <= set(caps.signature_help_provider.trigger_characters)
    source = 'db.sql("select date_trunc(\'day\', ")\n'
    uri = open_doc(client, source)
    result = await client.text_document_signature_help_async(
        types.SignatureHelpParams(
            text_document=types.TextDocumentIdentifier(uri=uri),
            position=position_of(source, "date_trunc('day', "),
        )
    )
    assert result is not None
    assert result.active_parameter == 1
    assert result.signatures[result.active_signature].label.startswith("date_trunc(part VARCHAR")


@pytest.mark.asyncio
async def test_hover_over_function(client: LanguageClient):
    source = 'db.sql("select date_trunc(\'day\', now())")\n'
    uri = open_doc(client, source)
    result = await client.text_document_hover_async(
        types.HoverParams(text_document=types.TextDocumentIdentifier(uri=uri), position=position_of(source, "date_tr"))
    )
    assert result is not None
    assert "Truncate to specified precision" in result.contents.value
    start = source.index("date_trunc")
    assert (result.range.start.character, result.range.end.character) == (start, start + len("date_trunc"))


@pytest.mark.asyncio
async def test_quoted_name_completion_replaces_the_quote(client: LanguageClient):
    source = 'db.sql(\'select * from "Order Li\')\n'
    uri = open_doc(client, source)
    db = make_completion_db()
    SchemaCache(CACHE_PATH).save_snapshot(client.workspace_path / "nb.py", get_db_data_new(db, skip_ipython=True))
    result = await client.text_document_completion_async(
        types.CompletionParams(
            text_document=types.TextDocumentIdentifier(uri=uri),
            position=position_of(source, '"Order Li'),
            context=types.CompletionContext(trigger_kind=types.CompletionTriggerKind.Invoked),
        )
    )
    (item,) = [x for x in result.items if x.label == '"Order Lines"']
    start = source.index('"Order Li')
    assert (item.text_edit.range.start.character, item.text_edit.range.end.character) == (start, start + 9)
    assert item.text_edit.new_text == '"Order Lines"'
