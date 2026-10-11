import asyncio
import logging
import threading
from dataclasses import dataclass
from typing import Union

from lsprotocol import types as lsp
from pygls.lsp.server import LanguageServer

from dabbler.common import uri_to_path
from dabbler.db_stuff import get_default_db_data
from dabbler.lsp.completer import SqlCompleter
from dabbler.lsp.database_files import DatabaseFiles, DatabaseSource
from dabbler.lsp.directives import UseDirective, find_use_directive, resolve_database_path
from dabbler.schema_cache import SchemaCache


@dataclass
class DocumentDatabase:
    """A document's `# dabbler use` comment and what came of it."""
    directive: UseDirective
    path: Union[str, None]  # the file it names; None if it can't be resolved
    database: DatabaseSource


class InlineSqlLangServer(LanguageServer):
    CMD_SEND_SQL_TO_GUI = "sendSqlToDbDabbler"
    CMD_FORMAT_CURRENT_STATEMENT = "dbDabblerFormatCurrentStatement"

    def __init__(self, *args):
        super().__init__(*args)

        self.log = logging.getLogger("dabbler_lsp")
        self.schema_cache = SchemaCache()
        self.database_files = DatabaseFiles(self.schema_cache)
        # source file -> (its version in the schema cache, completer for it)
        self.completers: dict[str, tuple[str, SqlCompleter]] = {}
        self.default_completer: Union[SqlCompleter, None] = None
        self.completer_lock = threading.Lock()
        # uri -> scheduled syntax check, so a burst of edits is checked once
        self.pending_diagnostics: dict[str, asyncio.TimerHandle] = {}

    def log_to_client(self, message: str):
        """Writes to the extension's output channel."""
        self.window_log_message(lsp.LogMessageParams(type=lsp.MessageType.Log, message=message))

    def notify_user(self, message: str, type: lsp.MessageType = lsp.MessageType.Warning):
        """Shows a notification in the editor."""
        self.window_show_message(lsp.ShowMessageParams(type=type, message=message))

    def publish_diagnostics(self, uri: str, diagnostics: list[lsp.Diagnostic], version: Union[int, None] = None):
        self.text_document_publish_diagnostics(
            lsp.PublishDiagnosticsParams(uri=uri, diagnostics=diagnostics, version=version)
        )

    def snippet_support(self) -> bool:
        try:
            return bool(self.client_capabilities.text_document.completion.completion_item.snippet_support)  # type: ignore
        except AttributeError:
            return False

    def workspace_root(self) -> Union[str, None]:
        try:
            return self.workspace.root_path
        except Exception:
            return None

    def log_workspace_info(self):
        workspace_info = {
            "root_uri": self.workspace.root_uri,
            "root_path": self.workspace.root_path,
            "schema_cache": str(self.schema_cache.path),
        }
        self.log.debug(["workspace_info", workspace_info])

    def session_source(self, uri: str) -> Union[str, None]:
        """The IPython session's file whose schema applies to this document, if any."""
        try:
            path = uri_to_path(uri)
        except ValueError:
            return None
        return self.schema_cache.resolve_source(path, self.workspace_root())

    def document_database(self, uri: str) -> Union[DocumentDatabase, None]:
        """The database file a document names with `# dabbler use '<file>'`, read if it changed."""
        try:
            directive = find_use_directive(self.workspace.get_text_document(uri).source)
        except Exception:
            return None
        if directive is None:
            return None
        try:
            document_path = str(uri_to_path(uri))
        except ValueError:
            document_path = None
        path = resolve_database_path(directive.path, document_path)
        if path is None:
            message = "a relative path needs a saved file" if directive.path.strip() else "no database file given"
            return DocumentDatabase(directive, None, DatabaseSource(None, message))
        return DocumentDatabase(directive, path, self.database_files.source(path, self.workspace_root()))

    def snapshot_source(self, uri: str) -> Union[str, None]:
        """The source whose schema applies to this document: the database file of its
        `# dabbler use` comment, else the IPython session (see session_source)."""
        found = self.document_database(uri)
        if found and found.database.source:
            return found.database.source
        if found:
            self.log.debug(f"using the IPython session's schema: {found.database.error}")
        return self.session_source(uri)

    def get_default_completer(self) -> SqlCompleter:
        if self.default_completer is None:
            blank_db_data = get_default_db_data()
            blank_db_data["cwd"] = self.workspace_root()
            self.default_completer = SqlCompleter(blank_db_data)
        return self.default_completer

    def get_completer(self, uri: str) -> SqlCompleter:
        """Completer for a document.

        Completers read the schema cache on every request, so tables and columns
        are always current. They are recreated when the source's version changes,
        which refreshes per-source settings (working directory, file search path,
        extensions) and the cached columns of table functions and table macros.
        Checking the version is one primary-key lookup.
        """
        source = self.snapshot_source(uri)
        with self.completer_lock:
            if source is None:
                return self.get_default_completer()
            version = self.schema_cache.get_version(source)
            if version is None:
                return self.get_default_completer()
            cached = self.completers.get(source)
            if cached and cached[0] == version:
                return cached[1]
            try:
                completer = SqlCompleter(cache=self.schema_cache, source_file=source)
            except Exception:
                self.log.exception(f"failed to set up completions for {source}")
                return cached[1] if cached else self.get_default_completer()
            self.completers[source] = (version, completer)
        self.log.debug(f"schema of {source} is at version {version}")
        return completer

    def warm_up(self, uri: str):
        """Builds the document's completer in the background so the first completion is quick."""
        threading.Thread(target=self.get_completer, args=(uri,), daemon=True).start()
