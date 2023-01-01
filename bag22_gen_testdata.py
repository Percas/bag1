#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Doel: maak testdata door een subset te nemen van vbo.csv, pnd.csv, num.csv 
van x %, als volgt

1. neem x % num
2. vbo die bij num horen
3. pnd die bij vbo horen
4. kopieer sta, lig, opr, wpl in zijn geheel
5. neem vbovk in vbovk_pndvk.csv die bij vbo horen

Aanpak:
    1. lees num, vbo, pnd, vbovk_pndvk (de laatste uit koppelvlak 3)
    2. selecteer x % num
    3. selecteer hierbij de vbo
    4. selecteer hierbij de pnd
    5. schrijf alles weg
    
0.2 bugfix: koppeling num met vbo levert te veel records. Je moet unieke
numid nemen (natuurlijk)    

03. inlezen met inlees functie, timing, kopieer ook sta, lig, opr, wpl
"""


import pandas as pd
# import numpy as np
import sys
import os
import baglib
import time
import shutil
# from pathlib import Path
from baglib import BAG_TYPE_DICT



# #############################################################################
# print('00.............Define functions...............................')
# #############################################################################

# #############################################################################
# print('00.............Initializing variables...............................')
# #############################################################################

tic = time.perf_counter()

print('-------------------------------------------')
print('------------- Start bag_gen_testdata ------')
print('-------------------------------------------')

BASEDIR = '/home/anton/python/bag/'
DATADIR = BASEDIR + 'data/'
DIR02 = DATADIR + '02-csv/'
DIR03 = DATADIR + '03-bewerktedata/'

ONTBASEDIR = BASEDIR + 'ont/'
ONTDATADIR = ONTBASEDIR + 'data/'
ONTDIR02 = ONTDATADIR + '02-csv/'
ONTDIR03 = ONTDATADIR + '03-bewerktedata/'

month_lst = os.listdir(DIR02)
if len(sys.argv) <= 1:
    sys.exit('Usage: bepaal-hoofdpnd <month>, where <month> in '
             + str(month_lst))
current_month = sys.argv[1]
if current_month not in month_lst:
    sys.exit('Usage: bepaal-hoofdpnd <month>, where <month> in '
             + str(month_lst))


k2dir = DIR02 + current_month + '/'
k3dir = DIR03 + current_month + '/'

ontk2dir = ONTDIR02 + current_month + '/'
ontk3dir = ONTDIR03 + current_month + '/'

current_month = int(current_month)
current_year = int(current_month/100)

INPUT_FILS_DICT = {'vbo': k2dir + 'vbo.csv',
                   'pnd': k2dir + 'pnd.csv',
                   'num': k2dir + 'num.csv',
                   'vbovk_pndvk': k3dir + 'vbovk_pndvk.csv'}

OUTPUT_FILS_DICT = {'vbo': ontk2dir + 'vbo.csv',
                    'pnd': ontk2dir + 'pnd.csv',
                    'num': ontk2dir + 'num.csv',
                    'vbovk_pndvk': ontk3dir + 'vbovk_pndvk.csv'}

vbovk = ['vboid', 'vbovkid']
pndvk = ['pndid', 'pndvkid']
ontbd = {}

KEY_DICT = {'vbo': vbovk,
            'pnd': pndvk}
  
printit = True
  
PERC = 1

print('\tHuidige maand:', current_month)
print('\tPercentage testdata vbo, pnd, num:', PERC, '%')

# #############################################################################
print('\n Inlezen van vbo.csv, pnd.csv, num.csv, vbovk_pndvk-----')
# #############################################################################

bd = baglib.read_input_csv(INPUT_FILS_DICT, BAG_TYPE_DICT)

print('\n----Selecteer', PERC, 'procent van records in num.csv---')
ontbd['num'] = bd['num'].sample(frac=PERC/100)

print('\nSelecteer alle vk van de gekozen NUM')
ontbd['num'] = pd.merge(ontbd['num']['numid'].drop_duplicates(),
                       bd['num'], how='inner')
print('\tPercentage num records geselecteerd:',
      round(100 * ontbd['num'].shape[0] / bd['num'].shape[0], 3))
# print(ontbd['num'].head())

print('\nSelecteer alle vbovk op dat num...')
ontbd['vbo'] = pd.merge(bd['vbo'],
                       ontbd['num']['numid'].drop_duplicates(),
                       how='inner')
# print(ontbd['vbo'].info())
print('\tPercentage vbo records geselecteerd:',
      round(100 * ontbd['vbo'].shape[0] / bd['vbo'].shape[0], 3))

print('\nSelecteer bij die vbos de bijbehordende panden...')
ontbd['pnd'] = pd.merge(ontbd['vbo']['pndid'].drop_duplicates(),
                       bd['pnd'], how='inner')
# print(ontbd['pnd'].info())
print('\tPercentage pnd records geselecteerd:',
      round(100 * ontbd['pnd'].shape[0] / bd['pnd'].shape[0], 3))

print('\nSelecteer bij de vbos de bijbehorende vbovk_pndvk...')
ontbd['vbovk_pndvk'] = pd.merge(ontbd['vbo']['vboid'].drop_duplicates(),
                       bd['vbovk_pndvk'], how='inner')
# print(ontbd['pnd'].info())
print('\tPercentage vbovk_pndvk records geselecteerd:',
      round(100 * ontbd['vbovk_pndvk'].shape[0] / bd['vbovk_pndvk'].shape[0], 3))


print('\nMappen aanmaken in de ontwikkelomgeving ont (indien nodig):')
baglib.make_dir(ontk2dir)
baglib.make_dir(ontk3dir)

# #############################################################################
print('\n Bewaren in van vbo.csv, pnd.csv, num.csv, vbovk_pndvk-----')
# #############################################################################

for bob in INPUT_FILS_DICT.keys():
    print('\tWriting', ontbd[bob].shape[0], 'records to', OUTPUT_FILS_DICT[bob])
    ontbd[bob].to_csv(OUTPUT_FILS_DICT[bob], index=False)
    

print('-------Kopieer de rest van de bestanden------')
rest = ['sta', 'lig', 'opr', 'wpl', 'wplgem']
for r in rest:
    print('\t1-1 kopieren van', r, 'naar ontwikkelomgeving')
    shutil.copy(k2dir + r + '.csv', ontk2dir)


toc = time.perf_counter()
baglib.print_time(toc - tic, 'testdata for vbo, pnd, num generated in', printit)

