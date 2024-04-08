import sys
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QLabel
from PyQt5.QtCore import QTimer, QTime

class TimerWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.startTime = None

    def initUI(self):
        # Layout
        self.layout = QVBoxLayout()
        
        # Button
        self.button = QPushButton('Start Timer')
        self.button.clicked.connect(self.startTimer)
        self.layout.addWidget(self.button)
        
        # Label
        self.label = QLabel('Press the button to start the timer.')
        self.layout.addWidget(self.label)
        
        # Timer
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.updateTimer)
        
        self.setLayout(self.layout)
    
    def startTimer(self):
        self.startTime = QTime.currentTime()
        self.timer.start(1000)  # Update every second
        self.button.setEnabled(False)  # Disable the button to prevent restarts
    
    def updateTimer(self):
        if self.startTime:
            elapsed = self.startTime.secsTo(QTime.currentTime())
            self.label.setText(f'Seconds since the button was pressed: {elapsed}')
    
    def reset(self):
        self.timer.stop()
        self.button.setEnabled(True)
        self.label.setText('Press the button to start the timer.')
        self.startTime = None

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = TimerWidget()
    ex.show()
    sys.exit(app.exec_())
