from qtpy import QtWidgets, QtCore, QtGui
from qtpy.QtCore import Qt
from qtpy.QtGui import QSyntaxHighlighter, QTextCharFormat, QFont
from rapidfuzz import fuzz
import duckdb
from duckdb import tokenize, token_type
from dabbler.txt_util import get_idx, line_col
import json


class SqlSyntaxHighlighter(QSyntaxHighlighter):
    def __init__(self, parent=None):
        super().__init__(parent)

    

    def highlightBlock(self, text):
        # print(f"text: {text}")
        tokens = tokenize(text)
        for i, token in enumerate(tokens):
            start = token[0]
            end = tokens[i + 1][0] if i + 1 < len(tokens) else len(text)
            value = text[start:end].strip()
            # print(f"token: {token}", f'value: "{value}"')

            length = end - start
            format = QTextCharFormat()
            if token[1] == token_type.keyword:
                # format.setFontWeight(QFont.Bold)
                format.setForeground(QtGui.QColor('#469BD2'))
            if token[1] == token_type.string_const:
                format.setForeground(QtGui.QColor('#D69F84'))
            if token[1] == token_type.numeric_const:
                format.setForeground(QtGui.QColor('#B9CDAB'))
            if token[1] == token_type.comment:
                format.setForeground(QtGui.QColor('#6A9955'))
            if token[1] == token_type.operator:
                format.setFontWeight(QFont.Bold)
                if value in ['(', ')', '{', '}', '[', ']']:
                    format.setForeground(QtGui.QColor('#EDA022'))
                # else:
                    # format.setForeground(QtGui.QColor('gold'))
            
            self.setFormat(start, length, format)

#MARK: Worker
class CompletionWorker(QtCore.QObject):
    completionResult = QtCore.Signal(list, int)
    validationResult = QtCore.Signal(dict)
    

    def __init__(self, parent=None):
        super().__init__(parent)
        self.completer = None

    @QtCore.Slot(str)
    def validate(self, sql):
        try:
            j = duckdb.execute("""SELECT json_serialize_sql(?::VARCHAR)""",[sql]).fetchone()[0]
            p = json.loads(j)
        except Exception as e:
            p = {}
            print(e)
        self.validationResult.emit(p)

    
    def setup_completer(self):
        db = duckdb.connect('./../sample_data/test.db', read_only=True)  
        from dabbler.db_stuff import get_db_data_new
        from dabbler.lsp.completer import SqlCompleter

        db_data = get_db_data_new(db)
        self.completer = SqlCompleter(db_data)

    @QtCore.Slot(str, int, str)
    def request_completions(self, text:str, cursor_position, trigger):
        
        if self.completer is None:
            return []

        completions = self.completer.route_completion2(
            cursor_pos=cursor_position,
            txt=text,
            trigger=trigger,
        )
        if not completions:
            return []
        
        result = [c.label for c in completions.items]

        self.completionResult.emit(result, cursor_position)



class CompletionWidget(QtWidgets.QListWidget):
    completion_selected = QtCore.Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(Qt.WindowType.Popup | Qt.WindowType.FramelessWindowHint)
        self.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.setMouseTracking(True)
        self.itemClicked.connect(self.item_clicked)
        font = QtGui.QFont('Consolas', 14)
        self.setFont(font)


    def item_clicked(self, item):
        self.completion_selected.emit(item.text())
        self.hide()

    def mousePressEvent(self, event):
        super().mousePressEvent(event)
        self.hide()

#MARK: PlainTextEdit
class AutocompleteLineEdit(QtWidgets.QPlainTextEdit):

    request_Completions = QtCore.Signal(str, int, str)
    request_validation = QtCore.Signal(str)

    def __init__(self, parent=None, overlay:QtWidgets.QPlainTextEdit = None):
        super().__init__(parent)
        self.overlay = overlay
        self.insert_start = 0
        self.completion_widget = CompletionWidget(self)
        self.completion_widget.completion_selected.connect(self.insert_completion)
        self.textChanged.connect(self.update_completions)
        # self.textChanged.connect(self.apply_highlight)
        self.completion_widget.installEventFilter(self)
        self.completion_visible = False


        self.completion_worker = CompletionWorker()
        self.completion_worker.completionResult.connect(self.show_completion_widget)
        self.completion_worker.validationResult.connect(self.post_validation)

        self.request_Completions.connect(self.completion_worker.request_completions)
        self.request_validation.connect(self.completion_worker.validate)

        self.worker_thread = QtCore.QThread()
        self.completion_worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.completion_worker.setup_completer)
        self.worker_thread.start()


        font = QtGui.QFont('Consolas', 14)
        self.setFont(font)
        self.overlay.setFont(font)
        self.trigger_positon = 0
        self.full_completion_list = []
        self.highlighter = SqlSyntaxHighlighter(self.document())
        self.setStyleSheet("QPlainTextEdit { background-color: #1F1F1F00; color: #ffffff; }")
        
        
        # self.document().setDefaultStyleSheet(HtmlFormatter().get_style_defs())

    @QtCore.Slot(dict)
    def post_validation(self, p:dict):
        if 'error' in p and p['error']:
            try:
                pos = int(p['position'])
            except:
                pos = 0
            txt = self.toPlainText()
            lines = txt.split("\n")
            row, col = line_col(txt, pos)

            val_txt = '\n' * row + ' ' * len(lines[row]) + '   ' + p['error_message']
            self.overlay.setPlainText(val_txt)
        else:
            self.overlay.setPlainText("")


    

    def update_completions(self):
        text = self.toPlainText()
        self.request_validation.emit(text)
        cursor_position = self.textCursor().position()
        text_before_cursor = text[:cursor_position]

        if len(text_before_cursor) == 0:
            self.hide_completion_widget()
            # print("no text before cursor")
            return
        trigger = text_before_cursor[-1]
        # print(f"text_before_cursor: '{text_before_cursor}', trigger: {trigger}")
        if trigger in ['.', ' ']:
            # print("trigger in ['.', ' ']")
            self.hide_completion_widget()
            self.insert_start = cursor_position
            self.trigger_positon = cursor_position
            self.request_Completions.emit(text, cursor_position, trigger)
            return

        if cursor_position < self.trigger_positon:
            # print("cursor_position < self.trigger_positon")
            self.hide_completion_widget()
            return
        text_after_trigger = text[self.trigger_positon:cursor_position]
        # current_word = self.get_current_word(text_after_trigger)
        
        if self.completion_visible:
            # print("completion_visible, filtering completions, text_after_trigger: ", text_after_trigger)
            filtered_completions = self.filter_choices(text_after_trigger, self.full_completion_list)
            self.completion_widget.clear()
            if len(filtered_completions) > 0:
                self.completion_widget.addItems(filtered_completions)
            else:
                self.hide_completion_widget()
            return


        
        if not self.full_completion_list:
            self.insert_start = cursor_position
            self.request_Completions.emit(text, cursor_position, trigger)
            self.trigger_positon = cursor_position

            # self.full_completion_list = self.get_completion_list(text[:cursor_position])
            # self.trigger_positon = cursor_position
            # if not self.full_completion_list:
            #     return
            # self.show_completion_widget(self.full_completion_list)


    def get_current_word(self, text):
        words = text.split()
        return words[0] if words else ""


    def filter_choices(self, query:str, choices:list[str]):
        # print(f"query: {query}, choices: {choices}")
        #filter choices that contain the letters in the query in order case-insensitive
        letters = list(query.lower())
        filtered_choices = []
        for choice in choices:
            choice_lower = choice.lower()
            if all(letter in choice_lower for letter in letters):
                filtered_choices.append(choice)

        #sort the choices by similarity to the query
        filtered_choices.sort(key=lambda x: fuzz.ratio(x, query), reverse=True)

        return filtered_choices

    def show_completion_widget(self, completion_list, pos):
        self.completion_widget.clear()
        current_pos = self.textCursor().position()
        if not completion_list or pos != current_pos:
            self.hide_completion_widget()
            return
        self.full_completion_list = completion_list
        self.completion_widget.addItems(completion_list)

        # Calculate position for the completion widget
        cursor_rect = self.cursorRect()
        global_cursor_pos = self.mapToGlobal(cursor_rect.bottomLeft())
        
        # Adjust the widget size
        self.completion_widget.setFixedWidth(self.width())
        self.completion_widget.adjustSize()
        
        # Set the position
        self.completion_widget.move(global_cursor_pos)
        self.completion_widget.setCurrentIndex(self.completion_widget.model().index(0, 0))
        self.completion_widget.show()
        self.completion_visible = True
    
    def setCursorPosition(self, position):
        cursor = self.textCursor()
        cursor.setPosition(position)
        self.setTextCursor(cursor)

    def insert_completion(self, completion):
        cursor = self.textCursor()
        pos = cursor.position()

        cursor.setPosition(self.insert_start)
        cursor.setPosition(pos, QtGui.QTextCursor.MoveMode.KeepAnchor)

        cursor.removeSelectedText()
        cursor.insertText(completion)
        
        self.setTextCursor(cursor)
        self.hide_completion_widget()
        

    def hide_completion_widget(self):
        self.completion_widget.hide()
        self.full_completion_list = []
        self.completion_visible = False
        self.trigger_positon = 0
    


    def move_selected_lines(self, direction, copy=False):
        cursor = self.textCursor()
        start = cursor.selectionStart()
        end = cursor.selectionEnd()
        start_row, start_col = line_col(self.toPlainText(), start)
        end_row, end_col = line_col(self.toPlainText(), end)
        

        text = self.toPlainText()
    
        lines = text.split("\n")
        new_text = None
        
        if direction == 1 and end_row < len(lines) - 1:
            # Moving down
            moved_lines = lines[start_row:end_row + 1]
            cursor.setPosition(get_idx(text, start_row, 0))
            if copy:
                cursor.insertText("\n".join(moved_lines) + "\n")
                return
            cursor.setPosition(get_idx(text, end_row, len(lines[end_row])+1), QtGui.QTextCursor.MoveMode.KeepAnchor)
            moved_text = cursor.selectedText()
            cursor.removeSelectedText()
            cursor.movePosition(QtGui.QTextCursor.MoveOperation.StartOfLine)
            cursor.insertText(moved_text)
            new_text = self.toPlainText()
            new_start = get_idx(new_text, start_row + 1, start_col)
            # new_start = start + len(moved_text) + 1
            cursor.setPosition(new_start)
            cursor.setPosition(new_start - (start - end), QtGui.QTextCursor.MoveMode.KeepAnchor)
            self.setTextCursor(cursor)
            return
                        
        elif direction == -1 and start_row > 0:
            # Moving up
            moved_lines = lines[start_row:end_row + 1]
            # lines_above = "\n".join(lines[:start_row - 1])
            if copy:
                cursor.setPosition(get_idx(text, end_row, len(lines[end_row])+1))
                cursor.insertText("\n".join(moved_lines) + "\n")
                return
            cursor.setPosition(get_idx(text, start_row, 0))
            cursor.setPosition(get_idx(text, end_row, len(lines[end_row])+1), QtGui.QTextCursor.MoveMode.KeepAnchor)
            moved_txt = cursor.selectedText()
            cursor.removeSelectedText()
            cursor.movePosition(QtGui.QTextCursor.MoveOperation.StartOfLine)
            cursor.insertText(moved_txt)    
            new_text = self.toPlainText()
            new_start = get_idx(new_text, start_row - 1, start_col)
            cursor.setPosition(new_start)
            cursor.setPosition(new_start + (end - start), QtGui.QTextCursor.MoveMode.KeepAnchor)
            self.setTextCursor(cursor) 
            
            
            return

    # def keyReleaseEvent(self, event: QtGui.QKeyEvent) -> None:
    #     ctrl = event.modifiers() == Qt.KeyboardModifier.ControlModifier
    #     alt = event.modifiers() == Qt.KeyboardModifier.AltModifier
    #     altShift = event.modifiers() == Qt.KeyboardModifier.AltModifier | Qt.KeyboardModifier.ShiftModifier
    #     key = event.key()
    #     text = event.text()

    #     if text in ['(', '[', '{']:
    #         cursor = self.textCursor()
    #         cursor.insertText(text)
    #         cursor.movePosition(QtGui.QTextCursor.MoveOperation.PreviousCharacter)
    #         self.setTextCursor(cursor)
    #         return


    #     return super().keyReleaseEvent(event)


    def keyPressEvent(self, event: QtGui.QKeyEvent):
        ctrl = event.modifiers() == Qt.KeyboardModifier.ControlModifier
        alt = event.modifiers() == Qt.KeyboardModifier.AltModifier
        altShift = event.modifiers() == Qt.KeyboardModifier.AltModifier | Qt.KeyboardModifier.ShiftModifier
        key = event.key()
        text = event.text()

        if text in ['(', '[', '{']:
            cursor = self.textCursor()
            cursor.insertText(text)
            if text == '(':
                cursor.insertText(')')
            elif text == '[':
                cursor.insertText(']')
            elif text == '{':
                cursor.insertText('}')
            cursor.movePosition(QtGui.QTextCursor.MoveOperation.PreviousCharacter)
            self.setTextCursor(cursor)
            return


        if key == Qt.Key.Key_D and ctrl:
            print("ctrl+d")
            # self.hide_completion_widget()
            # cursor = self.textCursor()
            # cursor.setPosition(0)
            # cursor.setPosition(5, QtGui.QTextCursor.MoveMode.KeepAnchor)
            # cursor.insertText("apple 222\nasdbd")
            fmt = QtGui.QTextCharFormat()
            fmt.setForeground(QtGui.QColor('red'))
            self.highlighter.setFormat(0, 5, fmt)
            return

        


        
        if not self.completion_widget.isVisible():
            
            if key in (Qt.Key.Key_Enter, Qt.Key.Key_Return):
                cursor = self.textCursor()
                cursor2 = self.textCursor()

                cursor.select(QtGui.QTextCursor.SelectionType.LineUnderCursor)
                txt = cursor.selectedText()
                indent = len(txt) - len(txt.lstrip())
            
                cursor2.insertText("\n")
                cursor2.insertText(" " * indent)                
                self.setTextCursor(cursor2)
                return




            if key == Qt.Key.Key_Space and ctrl:
                self.update_completions()
                return

            if alt:
                if key == Qt.Key.Key_Up:
                    self.move_selected_lines(-1)
                    return
                elif key == Qt.Key.Key_Down:
                    self.move_selected_lines(1)
                    return
            if altShift:
                if key == Qt.Key.Key_Up:
                    self.move_selected_lines(-1, copy=True)
                    return
                elif key == Qt.Key.Key_Down:
                    self.move_selected_lines(1, copy=True)
                    return

            if key == Qt.Key.Key_Tab:
                cursor = self.textCursor()
                cursor.insertText("    ")
                return

        if self.completion_widget.isVisible():

            if key == Qt.Key.Key_Down:
                self.completion_widget.setCurrentRow(
                    (self.completion_widget.currentRow() + 1) % self.completion_widget.count()
                )
                return
            elif key == Qt.Key.Key_Up:
                self.completion_widget.setCurrentRow(
                    (self.completion_widget.currentRow() - 1) % self.completion_widget.count()
                )
                return
            elif key in (Qt.Key.Key_Enter, Qt.Key.Key_Return, Qt.Key.Key_Tab):
                current_item = self.completion_widget.currentItem()
                if current_item:
                    self.insert_completion(current_item.text())
                    self.hide_completion_widget()
                    return
            elif key == Qt.Key.Key_Escape:
                self.hide_completion_widget()
                self.full_completion_list = []
                return

        super().keyPressEvent(event)

    def eventFilter(self, obj, event):
        if obj == self.completion_widget and event.type() == QtCore.QEvent.KeyPress:
            self.keyPressEvent(event)
            return True
        return super().eventFilter(obj, event)

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Autocomplete LineEdit Example")
        self.setGeometry(-900, 100, 800, 600)

        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
    
        self._layout = QtWidgets.QVBoxLayout()
        central_widget.setLayout(self._layout)

        self.overlay_text_area = QtWidgets.QPlainTextEdit(self)
        self.editor = AutocompleteLineEdit(self,self.overlay_text_area)
        self.overlay_text_area.setReadOnly(True)
        self.overlay_text_area.setStyleSheet("QPlainTextEdit { background-color: #1F1F1F; color: red; }")
        self.overlay_text_area.show()
        self.overlay_text_area.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.overlay_text_area.setPlainText("           test")
        self.overlay_text_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.overlay_text_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.overlay_text_area.lower()
        #sync overlay scrollbars
        self.editor.verticalScrollBar().valueChanged.connect(self.overlay_text_area.verticalScrollBar().setValue)
        self.editor.horizontalScrollBar().valueChanged.connect(self.overlay_text_area.horizontalScrollBar().setValue)
        
        


        self.editor.setPlainText("""--sql
Select 
    t.PERMIT_NUMBER, 
    t.PERMIT_ADDRESS

from trees t
                                         
""")
        self._layout.addWidget(self.editor)
        # self._layout.addWidget(self.overlay_text_area)

        self._layout.setSpacing(0)
        self._layout.setContentsMargins(0, 0, 0, 0)
        

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        # print("resize event")
        self.set_overaly_size()
        return super().resizeEvent(event)

    def set_overaly_size(self):
        #make same size as editor
        self_rect = self.editor.geometry()
        # print("self_rect: ", self_rect)
        self.overlay_text_area.setFixedWidth(self_rect.width())
        self.overlay_text_area.setFixedHeight(self_rect.height())
        # self.overlay_text_area.setGeometry(0,0)


if __name__ == "__main__":
    app = QtWidgets.QApplication([])
    window = MainWindow()
    window.show()
    window.set_overaly_size()
    app.exec_()