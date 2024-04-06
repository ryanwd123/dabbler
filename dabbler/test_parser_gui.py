#%%

from qtpy import QtWidgets, QtGui, QtCore
from txt_util import line_col, move_line, get_idx
from gui_stuff import gui_style
from dabbler.lsp.new_parser import interactive_parse_new


key_map = {

}

class Shortcut:
    def __init__(self, Seq, Parent, target, description:str=None):
        pp = f'{Seq: <10}{description}'
        key_map[Seq] = pp
        if not isinstance(Seq, list):
            Seq = [Seq]
        for i in Seq:
            sc = QtGui.QShortcut(QtGui.QKeySequence(i), Parent)
            sc.activated.connect(target)


class ShortcutHelp(QtWidgets.QWidget):
    def __init__(self, parent:'SqlEditor'):
        super().__init__()
        self.setWindowTitle('Shortcuts')
        # self.setGeometry(100, 100, 800, 600)
        self.app2 = parent

        self.text_edit = QtWidgets.QLabel()
        self.text_edit.setText('\n'.join(key_map.values()))

        #make always on top
        self.setWindowFlags(QtCore.Qt.WindowType.WindowStaysOnTopHint)

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.addWidget(self.text_edit)
        self.setLayout(self.layout)
        self.short_keys = [ 
            Shortcut("Ctrl+H", self, self.app2.show_shortcuts, 'Show shortcuts'),
            Shortcut("Ctrl+w", self, self.app2.show_shortcuts, 'close window'),
            Shortcut("Ctrl+q", self, self.app2.show_shortcuts, 'close window'),
        ]

class SqlTextBox(QtWidgets.QPlainTextEdit):
    def __init__(self,parent:'EditorWindow'=None):
        super().__init__()    
        # self.document().contentsChanged.connect(self.txt_change)
        # self.textChanged.connect(self.txt_change)
        self.editor = parent
        self.cursorPositionChanged.connect(self.cursor_move)

    def cursor_move(self):
        cursor = self.textCursor()
        pos = cursor.position()
        self.editor.cur_pos.setText(f'cursor_pos: {pos}')

        result = interactive_parse_new(self.toPlainText(), pos)
        if result:
            if result.token_history:
                self.editor.all_toekns_box.clear()
                self.editor.all_toekns_box.addItems(result.token_history)
            else:
                self.editor.all_toekns_box.clear()

            self.editor.tree_text.clear()
            if result.tree:
                self.editor.tree_text.setPlainText(result.tree.pretty())

            if result.choices:
                self.editor.choices_box.clear()
                self.editor.choices_box.addItems(result.choices)
            else:
                self.editor.choices_box.clear()

            if result.tokens_to_pos:
                self.editor.token_history.clear()
                self.editor.token_history.addItems(result.tokens_to_pos)
            else:                
                self.editor.token_history.clear()
            if result.duration:
                self.editor.duartion.addItem(str(result.duration))

        else:
            self.editor.choices_box.clear()
            self.editor.token_history.clear()
            self.editor.all_toekns_box.clear()


    def keyPressEvent (self, keyEvent:QtGui.QKeyEvent):
        vis = self.editor.comp.popup().isVisible()
        # print(keyEvent.key(), keyEvent.nativeModifiers(),f'popup: {vis}')
        if keyEvent.key() == 16777218 and keyEvent.nativeModifiers() == 513: #shift tab key
                        # print('tab key')
            keyEvent = QtGui.QKeyEvent(
                QtCore.QEvent.Type.KeyPress,
                32, #space key
                QtCore.Qt.KeyboardModifiers(keyEvent.nativeModifiers())
            )
            txt = self.toPlainText()
            idx = self.textCursor().position()
            anchor = self.textCursor().anchor()
            row, col = line_col(txt, idx)
            arow, acol = line_col(txt, anchor)
            lines = txt.split('\n')
            start = min(row,arow)
            end = max(row,arow)
            for i in range(start, end+1):
                if lines[i].startswith('    '):
                    lines[i] = lines[i][4:]
                else:
                    lines[i] = lines[i].lstrip()

            new_txt = '\n'.join(lines)
            self.setPlainText(new_txt)
            cursor = self.textCursor()
            cursor.setPosition(get_idx(new_txt, row, len(lines[row])-1), QtGui.QTextCursor.MoveMode.MoveAnchor)
            cursor.setPosition(get_idx(new_txt, arow, len(lines[arow])-1), QtGui.QTextCursor.MoveMode.KeepAnchor)
            self.setTextCursor(cursor)
            return

        if keyEvent.key() == 16777217 and keyEvent.nativeModifiers() == 512: #tab key
            # print('tab key')
            keyEvent = QtGui.QKeyEvent(
                QtCore.QEvent.Type.KeyPress,
                32, #space key
                QtCore.Qt.KeyboardModifiers(keyEvent.nativeModifiers())
            )
            txt = self.toPlainText()
            idx = self.textCursor().position()
            anchor = self.textCursor().anchor()
            row, col = line_col(txt, idx)
            arow, acol = line_col(txt, anchor)
            lines = txt.split('\n')
            start = min(row,arow)
            end = max(row,arow)
            for i in range(start, end+1):
                lines[i] = '    ' + lines[i]

            new_txt = '\n'.join(lines)
            self.setPlainText(new_txt)
            cursor = self.textCursor()
            cursor.setPosition(get_idx(new_txt, row, col+4), QtGui.QTextCursor.MoveMode.MoveAnchor)
            cursor.setPosition(get_idx(new_txt, arow, acol+4), QtGui.QTextCursor.MoveMode.KeepAnchor)

            self.setTextCursor(cursor)
            return
        super().keyPressEvent(keyEvent)


class EditorWindow(QtWidgets.QWidget):
    def __init__(self, parent:'SqlEditor'):
        super().__init__()
        self.setWindowTitle('SQL Editor')
        self.setGeometry(100, 100, 1600, 900)
        self.app2 = parent

        self.text_edit = SqlTextBox(self)
        self.text_edit.setPlaceholderText('Enter your SQL here')
        # self.text_edit.setFont(QtGui.QFont('Arial', 12))


        # self.monaco_widget.setLanguage('sql')
        self.token_history = QtWidgets.QListWidget()
        self.token_history.setFixedWidth(200)
        self.choices_box = QtWidgets.QListWidget()
        self.choices_box.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.MultiSelection)
        self.choices_box.setFixedWidth(200)
        self.all_toekns_box = QtWidgets.QListWidget()
        self.all_toekns_box.setFixedWidth(200)
        self.duartion = QtWidgets.QListWidget()
        self.duartion.setFixedWidth(100)

        self.cur_pos = QtWidgets.QLabel('cursor_pos: 0')
        self.header_label = QtWidgets.QLabel('sql editor')

        self.tree_text = QtWidgets.QPlainTextEdit()
        self.tree_text.setReadOnly(True)

        self.layoutz = QtWidgets.QGridLayout()
        self.layoutz.addWidget(self.cur_pos,0,0,QtCore.Qt.AlignmentFlag.AlignRight)
        self.layoutz.addWidget(self.text_edit, 1, 0, 2, 1)
        self.layoutz.addWidget(QtWidgets.QLabel('choices'),0,1)
        self.layoutz.addWidget(self.choices_box,1,1)
        self.layoutz.addWidget(QtWidgets.QLabel('choices'),2,1)
        self.layoutz.addWidget(QtWidgets.QLabel('all tokens'),0,2)
        self.layoutz.addWidget(self.all_toekns_box,1,2)
        self.layoutz.addWidget(QtWidgets.QLabel('tokens before cursor'),0,3)
        self.layoutz.addWidget(self.token_history,1,3)
        self.layoutz.addWidget(QtWidgets.QLabel('duration'),0,4)
        self.layoutz.addWidget(self.duartion,1,4)

        self.layoutz.addWidget(self.tree_text,1,5)


        self.setLayout(self.layoutz)

        
        self.short_keys = [
            Shortcut("Ctrl+H", self, self.show_shortcuts, 'show/hide shortcuts'),
            # Shortcut("Alt+Up", self, lambda:self.move(-1), 'Move line up'),
            # Shortcut("Alt+Down", self, lambda:self.move(1), 'Move line down'),
            Shortcut("Ctrl+N", self, self.app2.new_window, 'New window'),
            Shortcut("Ctrl+=", self, self.txt_bigger, 'zoom in'),
            Shortcut("Ctrl+-", self, self.txt_smaller, 'zoom out'),
            Shortcut("Ctrl+w", self, self.close, 'close window'),
            Shortcut("Ctrl+q", self, self.close, 'close window'),
            Shortcut("Ctrl+t", self, self.test_index, 'test'),

        ]
        self.text_edit.setPlainText('select\n   *\nfrom potato p')
        self.comp = QtWidgets.QCompleter(['apple', 'banana', 'cherry','lime'] + [f'word_{x}' for x in range(50)], self.text_edit)
        self.comp.setCompletionMode(QtWidgets.QCompleter.PopupCompletion)
        self.comp.setWidget(self.text_edit)
        self.comp_popup = self.comp.popup()
        for i in range(4):
            self.txt_bigger()

 

    def test_index(self):
        pos = self.text_edit.textCursor().position()
        qt_row = self.text_edit.textCursor().blockNumber()
        qt_col = self.text_edit.textCursor().columnNumber()
        char_w = self.text_edit.fontMetrics().width('a')
        char_h = self.text_edit.fontMetrics().height()
        rect = self.text_edit.cursorRect()
        

        self.comp.complete(QtCore.QRect((qt_col) * char_w + 3, (qt_row+1) * char_h + 8,  char_w * 40, 1))
        # if self.comp.popup().isVisible():
        #     self.comp.popup().hide()
        # else:
        #     self.comp.popup().show()
        txt = self.text_edit.toPlainText()
        line, col = line_col(txt, pos)
        idx = get_idx(txt, line, col)
        print(f'pos: {pos}, rect: {rect}, line: {line}, col: {col}, idx: {idx} qtrow: {qt_row}, qtcol{qt_col}, {char_w}, {char_h}')

    def show_shortcuts(self):
        self.app2.show_shortcuts()
        self.activateWindow()

    # def text_selected(self):
    #     cursor = self.text_edit.textCursor()
    #     txt = cursor.selectionStart(), cursor.selectionEnd()
    #     cursor.setPosition(4, QtGui.QTextCursor.MoveMode.MoveAnchor)
    #     cursor.setPosition(8, QtGui.QTextCursor.MoveMode.KeepAnchor)
    #     cursor.anchor()
    #     cursor.position()
    #     self.text_edit.setTextCursor(cursor)
    #     print(f'text selected {txt}')

    def txt_bigger(self):
        editor_font = self.text_edit.font()
        popup_font = self.comp_popup.font()
        new_size = editor_font.pointSize() + 1
        editor_font.setPointSize(new_size)
        popup_font.setPointSize(new_size)
        self.text_edit.setFont(editor_font)
        self.comp_popup.setFont(popup_font)
        self.choices_box.setFont(editor_font)
        self.token_history.setFont(editor_font)
        self.all_toekns_box.setFont(editor_font)
        
    
    def txt_smaller(self):
        editor_font = self.text_edit.font()
        popup_font = self.comp_popup.font()
        new_size = editor_font.pointSize() - 1
        editor_font.setPointSize(new_size)
        popup_font.setPointSize(new_size)
        self.text_edit.setFont(editor_font)
        self.comp_popup.setFont(popup_font)
        self.choices_box.setFont(editor_font)
        self.token_history.setFont(editor_font)
        self.all_toekns_box.setFont(editor_font)

    def tst_move(self):
        cursor = self.text_edit.textCursor()
        cursor.setPosition(4, QtGui.QTextCursor.MoveMode.MoveAnchor)
        self.text_edit.setTextCursor(cursor)

    def move(self,direction:int):
        txt = self.text_edit.toPlainText()
        pos = self.text_edit.textCursor().position()
        anchor = self.text_edit.textCursor().anchor()
        result = move_line(txt, pos, anchor, direction)
        if result:
            new_idx, newanchor, new_txt = result
            self.text_edit.setPlainText(new_txt)
            cursor = self.text_edit.textCursor()
            cursor.setPosition(newanchor, QtGui.QTextCursor.MoveMode.MoveAnchor)
            cursor.setPosition(new_idx, QtGui.QTextCursor.MoveMode.KeepAnchor)
            self.text_edit.setTextCursor(cursor)

    def closeEvent(self, a0: None) -> None:
        if len(self.app2.windows) == 1:
            self.app2.quit()
        return super().closeEvent(a0)



class SqlEditor(QtWidgets.QApplication):
    def __init__(self):
        super().__init__([])
        self.windows:list[EditorWindow] = []
        self.new_window()
        self.shortcut_keys = [
        ]
        self.popup = ShortcutHelp(self)

        
        
    def new_window(self):
        window = EditorWindow(self)
        window.show()

        

        self.windows.append(window)

    def run(self):
        self.setStyleSheet(gui_style)
        self.exec_()

    def quit(self):
        super().quit()


    def show_shortcuts(self):
        if self.popup.isVisible():
            self.popup.hide()
        else:
            self.popup.show()


app = SqlEditor()
app.run()

# %%



