# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'form.ui'
##
## Created by: Qt User Interface Compiler version 6.4.2
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QAction, QBrush, QColor, QConicalGradient,
    QCursor, QFont, QFontDatabase, QGradient,
    QIcon, QImage, QKeySequence, QLinearGradient,
    QPainter, QPalette, QPixmap, QRadialGradient,
    QTransform)
from PySide6.QtWidgets import (QApplication, QGridLayout, QGroupBox, QMainWindow,
    QMenu, QMenuBar, QPushButton, QRadioButton,
    QSizePolicy, QStatusBar, QTabWidget, QTimeEdit,
    QVBoxLayout, QWidget)

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(418, 328)
        font = QFont()
        font.setPointSize(12)
        MainWindow.setFont(font)
        icon = QIcon()
        icon.addFile(u"logo.png", QSize(), QIcon.Normal, QIcon.Off)
        MainWindow.setWindowIcon(icon)
        MainWindow.setTabShape(QTabWidget.Rounded)
        MainWindow.setDockNestingEnabled(True)
        MainWindow.setDockOptions(QMainWindow.AllowNestedDocks|QMainWindow.AllowTabbedDocks)
        MainWindow.setUnifiedTitleAndToolBarOnMac(False)
        self.actionpt_BR = QAction(MainWindow)
        self.actionpt_BR.setObjectName(u"actionpt_BR")
        self.actionen_US = QAction(MainWindow)
        self.actionen_US.setObjectName(u"actionen_US")
        self.actionPAMepi = QAction(MainWindow)
        self.actionPAMepi.setObjectName(u"actionPAMepi")
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.gridLayout = QGridLayout(self.centralwidget)
        self.gridLayout.setObjectName(u"gridLayout")
        self.verticalGroupBox = QGroupBox(self.centralwidget)
        self.verticalGroupBox.setObjectName(u"verticalGroupBox")
        self.verticalLayout_2 = QVBoxLayout(self.verticalGroupBox)
        self.verticalLayout_2.setObjectName(u"verticalLayout_2")
        self.radioButton_5 = QRadioButton(self.verticalGroupBox)
        self.radioButton_5.setObjectName(u"radioButton_5")

        self.verticalLayout_2.addWidget(self.radioButton_5)

        self.radioButton_6 = QRadioButton(self.verticalGroupBox)
        self.radioButton_6.setObjectName(u"radioButton_6")
        self.radioButton_6.setCheckable(True)
        self.radioButton_6.setChecked(False)

        self.verticalLayout_2.addWidget(self.radioButton_6)

        self.radioButton_7 = QRadioButton(self.verticalGroupBox)
        self.radioButton_7.setObjectName(u"radioButton_7")

        self.verticalLayout_2.addWidget(self.radioButton_7)

        self.timeEdit = QTimeEdit(self.verticalGroupBox)
        self.timeEdit.setObjectName(u"timeEdit")

        self.verticalLayout_2.addWidget(self.timeEdit)

        self.pushButton_2 = QPushButton(self.verticalGroupBox)
        self.pushButton_2.setObjectName(u"pushButton_2")

        self.verticalLayout_2.addWidget(self.pushButton_2)


        self.gridLayout.addWidget(self.verticalGroupBox, 0, 1, 1, 1)

        self.verticalGroupBox_2 = QGroupBox(self.centralwidget)
        self.verticalGroupBox_2.setObjectName(u"verticalGroupBox_2")
        self.verticalLayout = QVBoxLayout(self.verticalGroupBox_2)
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.radioButton = QRadioButton(self.verticalGroupBox_2)
        self.radioButton.setObjectName(u"radioButton")
        self.radioButton.setChecked(False)
        self.radioButton.setAutoExclusive(False)

        self.verticalLayout.addWidget(self.radioButton)

        self.radioButton_2 = QRadioButton(self.verticalGroupBox_2)
        self.radioButton_2.setObjectName(u"radioButton_2")
        self.radioButton_2.setChecked(False)
        self.radioButton_2.setAutoExclusive(False)

        self.verticalLayout.addWidget(self.radioButton_2)

        self.radioButton_3 = QRadioButton(self.verticalGroupBox_2)
        self.radioButton_3.setObjectName(u"radioButton_3")
        self.radioButton_3.setChecked(False)
        self.radioButton_3.setAutoExclusive(False)

        self.verticalLayout.addWidget(self.radioButton_3)

        self.radioButton_4 = QRadioButton(self.verticalGroupBox_2)
        self.radioButton_4.setObjectName(u"radioButton_4")
        self.radioButton_4.setChecked(False)
        self.radioButton_4.setAutoExclusive(False)

        self.verticalLayout.addWidget(self.radioButton_4)

        self.pushButton = QPushButton(self.verticalGroupBox_2)
        self.pushButton.setObjectName(u"pushButton")

        self.verticalLayout.addWidget(self.pushButton)


        self.gridLayout.addWidget(self.verticalGroupBox_2, 0, 0, 1, 1)

        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(MainWindow)
        self.menubar.setObjectName(u"menubar")
        self.menubar.setGeometry(QRect(0, 0, 418, 27))
        self.menuSettings_Configura_es = QMenu(self.menubar)
        self.menuSettings_Configura_es.setObjectName(u"menuSettings_Configura_es")
        self.menuLanguage = QMenu(self.menubar)
        self.menuLanguage.setObjectName(u"menuLanguage")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(MainWindow)
        self.statusbar.setObjectName(u"statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.menubar.addAction(self.menuLanguage.menuAction())
        self.menubar.addAction(self.menuSettings_Configura_es.menuAction())
        self.menuSettings_Configura_es.addAction(self.actionPAMepi)
        self.menuLanguage.addAction(self.actionpt_BR)
        self.menuLanguage.addAction(self.actionen_US)

        self.retranslateUi(MainWindow)

        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"PAMepi", None))
        self.actionpt_BR.setText(QCoreApplication.translate("MainWindow", u"pt-BR", None))
        self.actionen_US.setText(QCoreApplication.translate("MainWindow", u"en-US", None))
        self.actionPAMepi.setText(QCoreApplication.translate("MainWindow", u"PAMepi", None))
        self.verticalGroupBox.setTitle(QCoreApplication.translate("MainWindow", u"Period", None))
        self.radioButton_5.setText(QCoreApplication.translate("MainWindow", u"daily", None))
        self.radioButton_6.setText(QCoreApplication.translate("MainWindow", u"weekly", None))
        self.radioButton_7.setText(QCoreApplication.translate("MainWindow", u"monthly", None))
        self.pushButton_2.setText(QCoreApplication.translate("MainWindow", u"Set", None))
        self.verticalGroupBox_2.setTitle(QCoreApplication.translate("MainWindow", u"Databases", None))
        self.radioButton.setText(QCoreApplication.translate("MainWindow", u"SRAG", None))
        self.radioButton_2.setText(QCoreApplication.translate("MainWindow", u"Sindrome Gripal", None))
        self.radioButton_3.setText(QCoreApplication.translate("MainWindow", u"Vacina\u00e7\u00e3o covid-19", None))
        self.radioButton_4.setText(QCoreApplication.translate("MainWindow", u"Wesley Cota", None))
        self.pushButton.setText(QCoreApplication.translate("MainWindow", u"Start", None))
        self.menuSettings_Configura_es.setTitle(QCoreApplication.translate("MainWindow", u"About", None))
        self.menuLanguage.setTitle(QCoreApplication.translate("MainWindow", u"Language", None))
    # retranslateUi

