#!/usr/bin/env python3

import sys
import os

from src.download import main


LOCAL_DOWNLOAD = os.path.join(os.path.dirname(__file__), 'data') # edite esta linha para mudar o local de download 

# exemplo:
# LOCAL_DOWNLOAD = os.path.expanduser('~/Downloads') # tentarar salvar na pasta de downloads


if __name__ == '__main__':
    try:
        main(LOCAL_DOWNLOAD, sys.argv[1], sys.argv[2])
    except IndexError:
        print('\n\tPrecisa de dois argumentos :0')
        print('\tTente: ')
        print('\tpython app.py \'2022-06-01\' \'2022-06-05\'\n')
