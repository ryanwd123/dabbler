try:
    from PySide6 import QtWidgets, QtCore, QtGui
    from PySide6.QtCore import Qt, Signal, Slot
    from PySide6.QtGui import QFont, QFontMetrics, QKeySequence, QShortcut
except ImportError:
    from qtpy import QtWidgets, QtCore, QtGui
    from qtpy.QtCore import Qt, Signal, Slot
    from qtpy.QtGui import QFont, QFontMetrics, QKeySequence, QShortcut