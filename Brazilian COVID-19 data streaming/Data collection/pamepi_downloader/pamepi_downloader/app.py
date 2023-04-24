import sys
import json
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QMessageBox, QPushButton, QMenu, QSystemTrayIcon, QRadioButton
)

from PySide6.QtCore import QRunnable, Slot, QThreadPool
from PySide6.QtGui import QIcon, QAction

from ui_form import Ui_MainWindow
from modules.srag import run


class Worker(QRunnable):
    def __init__(self, fn, *args, **kwargs):
        super().__init__()

        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    @Slot()
    def run(self):
        self.fn(*self.args, **self.kwargs)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.show()
        self.trayIcon()
        self.ui.pushButton.clicked.connect(self.get_all_radiobutton)
        self.ui.pushButton_2.clicked.connect(self.create_settings)
        self.threadpool = QThreadPool()

    def get_all_radiobutton(self):
        names = [
            radio.text()
            for radio in self.ui.verticalGroupBox_2.findChildren(QRadioButton)
            if radio.isChecked()
        ]
        if names:
            worker = Worker(run, names)
            self.threadpool.start(worker)

    def create_settings(self):
        settings = [
            radio.text()
            for radio in self.ui.verticalGroupBox.findChildren(QRadioButton)
            if radio.isChecked()
        ]
        time = str(self.ui.timeEdit.time().toPython())

        with open('settings.json', 'w') as fset:
            data = {'period': settings[0], 'time': time}
            fset.write(
                json.dumps(data)
            )

    def trayIcon(self):
        self.tray = QSystemTrayIcon()
        self.tray.setIcon(QIcon('logo.png'))
        self.tray.setVisible(True)

        self.menu = QMenu()
        self.restore_action = QAction('Restore', self)
        self.quit_action = QAction('Quit', self)

        self.menu.addAction(self.restore_action)
        self.menu.addAction(self.quit_action)

        self.tray.activated.connect(self.tray_icon_activated)
        self.restore_action.triggered.connect(self.showNormal)
        self.quit_action.triggered.connect(self.quit)

        self.tray.setContextMenu(self.menu)
        self.tray.show()

    def hideEvent(self, event):
        self.hide()

    def tray_icon_activated(self, reason):
        if reason == QSystemTrayIcon.DoubleClick:
            self.show()

    def quit(self):
        self.tray.hide()
        exit()
        self.close()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setApplicationName('PAMepi')
    window = MainWindow()

    sys.exit(app.exec())
