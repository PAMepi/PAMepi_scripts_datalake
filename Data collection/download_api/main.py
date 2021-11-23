#!/usr/bin/env python3

import sys
import os
import csv
from datetime import date, datetime
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QComboBox,
                               QPushButton, QLabel, QHBoxLayout, QVBoxLayout,
                               QGridLayout, QLineEdit, QFormLayout, QDateEdit,
                               QFileDialog, QRadioButton, QProgressBar)
from PySide6.QtGui import QIcon
from PySide6.QtCore import Qt, QObject, Signal, Slot, QDate, QThread


from pyOpenDatasus import PyOpenDatasus


class MainWindow(QMainWindow):

    apiName = {
        # 'SRAG 2021': 'srag',
        'Registro de Ocupação Hospitalar COVID-19': 'leitos_covid19',
        'Campanha Nacional de Vacinação contra Covid-19': 'vacinacao',
        'Notificações de Síndrome Gripal': 'notificacao_sg',
        'Brasil.io': 'brasil_io',
        # 'Google Mobility': 'gg_mobility'
    }

    def __init__(self):
        super().__init__()

        self.__path = os.path.expanduser('~/Downloads/')

        self.setupIU()

        self.button.clicked.connect(self.download)

        self.btn_dir.clicked.connect(self.set_folder_download)

        self.combo_base.currentTextChanged.connect(
            lambda x:
                self.check_api.setEnabled(False)
                if x == 'Google Mobility' or x == 'SRAG 2021'
                else self.check_api.setEnabled(True),
        )
        self.combo_state_.currentTextChanged.connect(
            lambda x: (
                self.combo_state.setEnabled(False) if x == 'Brasil'

                else self.combo_state.setEnabled(True),

                self.combo_state.clear(),

                self.combo_state.addItems(
                    sorted(
                        {
                            'Brasil': [
                                'Brasil'
                            ],
                            'Estado': [
                                'Rondônia', 'Amapá', 'Amazonas', 'Pará',
                                'Roraima', 'Acre', 'Tocantins', 'Alagoas',
                                'Bahia', 'Ceará', 'Maranhão', 'Paraíba',
                                'Pernambuco', 'Piauí', 'Rio Grande do Norte',
                                'Sergipe', 'Distrito Federal', 'Goiás',
                                'Mato Grosso', 'Mato Grosso do Sul',
                                'Espírito Santo', 'Minas Gerais',
                                'Rio de Janeiro', 'São Paulo', 'Paraná',
                                'Rio Grande do Sul', 'Santa Catarina',
                            ],
                            'Região': [
                                'Norte',
                                'Nordeste',
                                'Centro-Oeste',
                                'Sudeste',
                                'Sul',
                            ]
                        }[x]
                   )
                )
            )
        )

    def setupIU(self):

        combo_layout = QGridLayout()
        form_layout = QFormLayout()
        form_layout_ = QFormLayout()
        combo_layout.setContentsMargins(30, 30, 30, 30)
        combo_layout.setHorizontalSpacing(30)
        combo_layout.setSpacing(30)

        self.pbar = QProgressBar()
        self.pbar.setValue(0)
        self.pbar.setMinimum(0)
        self.pbar.setMaximum(100)

        widget = QWidget()
        widget.setLayout(combo_layout)

        self.combo_base = QComboBox()
        self.combo_base.setEditable(True)
        self.combo_state_ = QComboBox()
        self.combo_state_.setMinimumContentsLength(20)
        self.combo_state_.setEditable(True)
        self.combo_state = QComboBox()
        self.combo_state.setEnabled(False)
        self.combo_state.setEditable(True)
        self.check_api = QRadioButton('Download via api')
        self.check_url = QRadioButton('Download via link direto')
        self.dt_init = QDateEdit()
        d_min = QDate(2020, 1, 1)
        d_max = QDate(date.today())
        self.dt_init.setDate(d_min)
        self.dt_init.setMinimumDate(d_min)
        self.dt_init.setMaximumDate(d_max)

        self.dt_lst = QDateEdit()
        self.dt_lst.setDate(d_max)
        self.dt_lst.setMinimumDate(d_min)
        self.dt_lst.setMaximumDate(d_max)

        self.lbl = QLabel('Salvar arquivos em:')
        self.btn_dir = QPushButton(self.__path)

        form_layout_.addRow(self.check_api, self.check_url)
        form_layout.addRow(self.lbl)
        form_layout.addRow(self.btn_dir)

        form_date = QFormLayout()
        form_date.addRow('Data inicial', self.dt_init)
        form_date.addRow('Data final', self.dt_lst)

        self.button = QPushButton('Download')

        self.combo_base.addItems(
            sorted(
                [
                    'SRAG 2021',
                    'Registro de Ocupação Hospitalar COVID-19',
                    'Campanha Nacional de Vacinação contra Covid-19',
                    'Notificações de Síndrome Gripal',
                    'Brasil.io',
                    'Google Mobility',
                ]
            )
        )

        self.combo_state.addItem('Brasil')

        self.combo_state_.addItems([
            'Brasil', 'Região', 'Estado'
        ])

        combo_layout.addWidget(self.combo_base, 0, 0)
        combo_layout.addWidget(self.combo_state_, 0, 1)
        combo_layout.addWidget(self.combo_state, 1, 1)
        combo_layout.addLayout(form_layout_, 1, 0)
        combo_layout.addLayout(form_layout, 2, 0, Qt.AlignLeft)
        combo_layout.addLayout(form_date, 2, 1)
        combo_layout.addWidget(self.pbar, 4, 0, 4, 2)#, Qt.AlignBottom)
        combo_layout.addWidget(self.button, 8, 0, 4, 2, Qt.AlignBottom)

        self.setCentralWidget(widget)

        self.setWindowTitle('Programa Legal')
        self.show()

    def set_folder_download(self):

        _ = QFileDialog().getExistingDirectory(dir=self.__path)
        if not len(_):
            self.__path = self.__path
        else:
            self.__path = _
        self.btn_dir.setText(self.__path)

    def prepare_vars(self):
        uf = {
            'Brasil': [
                'AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO', 'AL', 'BA',
                'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE', 'DF', 'GO',
                'MT', 'MS', 'ES', 'MG', 'RJ', 'SP', 'PR', 'RS', 'SC',
            ],
            'Norte': [
                'AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'
            ],
            'Nordeste': [
                'AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'
            ],
            'Centro-Oeste': [
                'DF', 'GO', 'MT', 'MS'
            ],
            'Sudeste': [
                'ES', 'MG', 'RJ', 'SP'
            ],
            'Sul': [
                'PR', 'RS', 'SC'
            ],

            'Acre': 'AC', 'Amapá': 'AP', 'Amazonas': 'AM', 'Pará': 'PA',
            'Rondônia': 'RO', 'Roraima': 'RR', 'Tocantins': 'TO',

            'Alagoas': 'AL', 'Bahia': 'BA', 'Ceará': 'CE', 'Maranhão': 'MA',
            'Paraíba': 'PB', 'Pernambuco': 'PE', 'Piauí': 'PI',
            'Rio Grande do Norte': 'RN', 'Sergipe': 'SE',

            'Distrito Federal': 'DF', 'Goiás': 'GO', 'Mato Grosso': 'MT',
            'Mato Grosso do Sul': 'MS',

            'Espírito Santo': 'ES', 'Minas Gerais': 'MG',
            'Rio de Janeiro': 'RJ', 'São Paulo': 'SP',

            'Paraná': 'PR', 'Rio Grande do Sul': 'RS', 'Santa Catarina': 'SC',
        }

        date_min = (
            str(self.dt_init.date().day()) +
            '-'  +
            str(self.dt_init.date().month()) +
            '-' +
            str(self.dt_init.date().year())
        )
        date_max = (
            str(self.dt_lst.date().day()) +
            '-'  +
            str(self.dt_lst.date().month()) +
            '-' +
            str(self.dt_lst.date().year())
        )
        return (self.combo_base.currentText(),
                date_min, date_max,
                uf[self.combo_state.currentText()],
                self.__path)

    def download_api(self, api, locale, dt_min, dt_max, path):
        self.pyapi = PyOpenDatasus()
        # self.pyapi.pbarVal.connect(self.pbar.setValue)

        reg = ''
        if self.combo_state_.currentText() == 'Brasil':
            reg = self.combo_state_.currentText()
        else:
            reg = self.combo_state.currentText()

        dt_min = datetime.strptime(
            dt_min,
            '%d-%m-%Y'
        ).strftime('%Y-%#m-%#d')
        dt_max = datetime.strptime(
            dt_max, '%d-%m-%Y'
        ).strftime('%Y-%#m-%#d')

        filename = os.path.join(path, f'{api}_{reg}_{dt_min}-{dt_max}.csv')

        stt = ''
        if api != 'notificacao_sg':
            if isinstance(locale, list):
                for local in locale:
                    local = '"' + local + '"'
                    stt += local + ' OR '
                stt = stt[:-3]
            else:
                stt = locale

            self.pyapi.search(api, stt.lower(), dt_min, dt_max)
            self.pyapi.download(filename)

        elif api == 'notificacao_sg':
            if isinstance(locale, list):
                list(
                    map(
                        lambda x:
                        (self.pyapi.search(api, x.lower(), dt_min, dt_max),
                         self.pyapi.download(
                             os.path.join(path,
                                          f'{api}_{x}_{dt_min}-{dt_max}.csv'
                                          )
                         )
                        ),
                        locale
                    )
                )

            else:
                self.pyapi.search(api, locale.lower(), dt_min, dt_max)
                self.pyapi.download(filename)

    def download_url():
        pass

    def download(self):
        api_name, dt_min, dt_max, locale, path = self.prepare_vars()

        if self.check_api.isChecked():
            try:
                api = self.apiName[api_name]
                # self.download_api(api, locale, dt_min, dt_max, path)
                self.thread_api = Thread(
                    self.download_api, api, locale, dt_min, dt_max, path
                )
                self.thread_api.start()
            except KeyError:
                pass

    @Slot(int)
    def update_widgets(self, status):
        if status:
            pass
        else:
            pass


class Thread(QThread):

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def stop(self):
        try:
            self.terminate()
        except RuntimeError:
            pass

    def run(self):
        self.func(*self.args, **self.kwargs)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    # window.setWindowIcon(QIcon('pamepi.ico'))
    window.setWindowIcon(QIcon('pamepi.jpeg'))
    sys.exit(app.exec_())
