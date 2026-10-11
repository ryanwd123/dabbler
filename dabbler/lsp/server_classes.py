import logging
import threading
from typing import Union

from lsprotocol import types as lsp
from pygls.lsp.server import LanguageServer

from dabbler.common import uri_to_path
from dabbler.db_stuff import get_default_db_data
from dabbler.lsp.completer import SqlCompleter
from dabbler.schema_cache import SchemaCache


class InlineSqlLangServer(LanguageServer):
    CMD_SEND_SQL_TO_GUI = "sendSqlToDbDabbler"
    CMD_FORMAT_CURRENT_STATEMENT = "dbDabblerFormatCurrentStatement"

    def __init__(self, *args):
        super().__init__(*args)

        self.log = logging.getLogger("dabbler_lsp")
        self.schema_cache = SchemaCache()
        # snapshot source file -> (snapshot fingerprint, completer built from that snapshot)
        self.completers: dict[str, tuple[str, SqlCompleter]] = {}
        self.default_completer: Union[SqlCompleter, None] = None
        self.completer_lock = threading.Lock()

    def log_to_client(self, message: str):
        """Writes to the extension's output channel."""
        self.window_log_message(lsp.LogMessageParams(type=lsp.MessageType.Log, message=message))

    def notify_user(self, message: str, type: lsp.MessageType = lsp.MessageType.Warning):
        """Shows a notification in the editor."""
        self.window_show_message(lsp.ShowMessageParams(type=type, message=message))

    def publish_diagnostics(self, uri: str, diagnostics: list[lsp.Diagnostic]):
        self.text_document_publish_diagnostics(lsp.PublishDiagnosticsParams(uri=uri, diagnostics=diagnostics))

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

    def snapshot_source(self, uri: str) -> Union[str, None]:
        """The IPython source file whose schema applies to this document, if any."""
        try:
            path = uri_to_path(uri)
        except ValueError:
            return None
        return self.schema_cache.resolve_source(path, self.workspace_root())

    def get_default_completer(self) -> SqlCompleter:
        if self.default_completer is None:
            blank_db_data = get_default_db_data()
            blank_db_data["cwd"] = self.workspace_root()
            self.default_completer = SqlCompleter(blank_db_data)
        return self.default_completer

    def get_completer(self, uri: str) -> SqlCompleter:
        """Completer for a document, rebuilt when IPython has saved a newer schema.

        Checking for a newer schema is one primary-key lookup, so this runs on
        every completion request instead of the IPython side pushing updates.
        """
        source = self.snapshot_source(uri)
        with self.completer_lock:
            if source is None:
                return self.get_default_completer()
            cached = self.completers.get(source)
            fingerprint = self.schema_cache.get_fingerprint(source)
            if cached and cached[0] == fingerprint:
                return cached[1]
            snapshot = self.schema_cache.get_snapshot(source)
            if snapshot is None:
                return self.get_default_completer()
            try:
                completer = SqlCompleter(snapshot.db_data)
            except Exception:
                self.log.exception(f"failed to build completer from schema of {source}")
                return cached[1] if cached else self.get_default_completer()
            self.completers[source] = (snapshot.fingerprint, completer)
        self.log_to_client(f"loaded schema from {source}")
        return completer

    def warm_up(self, uri: str):
        """Builds the document's completer in the background so the first completion is quick."""
        threading.Thread(target=self.get_completer, args=(uri,), daemon=True).start()
