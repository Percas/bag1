#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Doel: maak testdata door een subset te nemen van vbo.csv, pnd.csv, num.csv 
van x %, als volgt

1. neem x % num
2. vbo die bij num horen
3. pnd die bij vbo horen
4. kopieer sta, lig, opr, wpl in zijn geheel

Aanpak:
    1. lees num, vbo, pnd
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

month_lst = os.listdir(DIR02)
if len(sys.argv) <= 1:
    sys.exit('Usage: bepaal-hoofdpnd <month>, where <month> in '
             + str(month_lst))
current_month = sys.argv[1]
if current_month not in month_lst:
    sys.exit('Usage: bepaal-hoofdpnd <month>, where <month> in '
             + str(month_lst))


k2dir = DIR02 + current_month + '/'

ontk2dir = ONTDIR02 + current_month + '/'

current_month = int(current_month)
current_year = int(current_month/100)

INPUT_FILS_DICT = {'vbo': k2dir + 'vbo.csv',
                   'pnd': k2dir + 'pnd.csv',
                   'num': k2dir + 'num.csv'}


vbovk = ['vboid', 'vbovkid']
pndvk = ['pndid', 'pndvkid']

KEY_DICT = {'vbo': vbovk,
            'pnd': pndvk}
  
printit = True
  
PERC = 1

print('\tHuidige maand:', current_month)
print('\tPercentage testdata vbo, pnd, num:', PERC, '%')

# #############################################################################
print('\n----1. Inlezen van vbo.csv, pnd.csv en num.csv-----')
# #############################################################################

bd = baglib.read_input_csv(INPUT_FILS_DICT, BAG_TYPE_DICT)

print('\n----Selecteer', PERC, 'procent van records in num.csv---')
ontnumvk_df = bd['num'].sample(frac=PERC/100)

print('\tSelecteer alle vk van de gekozen NUM')
ontnumvk_df = pd.merge(ontnumvk_df['numid'].drop_duplicates(),
                       bd['num'], how='inner')
print('\tPercentage num records geselecteerd:',
      round(100 * ontnumvk_df.shape[0] / bd['num'].shape[0], 3))
# print(ontnumvk_df.head())

print('\tSelecteer alle vbovk op dat num...')
ontvbovk_df = pd.merge(bd['vbo'],
                       ontnumvk_df['numid'].drop_duplicates(),
                       how='inner')
# print(ontvbovk_df.info())
print('\tPercentage vbo records geselecteerd:',
      round(100 * ontvbovk_df.shape[0] / bd['vbo'].shape[0], 3))

print('\tSelecteer bij die vbos de bijbehordende panden...')
ontpndvk_df = pd.merge(ontvbovk_df['pndid'].drop_duplicates(),
                       bd['pnd'], how='inner')
# print(ontpndvk_df.info())
print('\tPercentage pnd records geselecteerd:',
      round(100 * ontpndvk_df.shape[0] / bd['pnd'].shape[0], 3))

print('\tCreating directories and files in ONT:')
baglib.make_dir(ontk2dir)
# Path(OUTPUTDIR).mkdir(parents=True, exist_ok=True)

print('\tWriting', ontnumvk_df.shape[0], 'records num.csv to',
      ontk2dir, '...')
outputfile = ontk2dir + 'num.csv'
ontnumvk_df.to_csv(outputfile, index=False)

print('\tWriting', ontpndvk_df.shape[0], 'records pnd.csv to',
      ontk2dir, '...')
outputfile = ontk2dir + 'pnd.csv'
ontpndvk_df.to_csv(outputfile, index=False)

print('\tWriting', ontvbovk_df.shape[0], 'records vbo.csv to',
      ontk2dir, '...')
outputfile = ontk2dir + 'vbo.csv'
ontvbovk_df.to_csv(outputfile, index=False)

print('-------Kopieer de rest van de bestanden------')
rest = ['sta', 'lig', 'opr', 'wpl']
for r in rest:
    print('\t1-1 kopieren van', r, 'naar ontwikkelomgeving')
    shutil.copy(k2dir + r + '.csv', ontk2dir)


toc = time.perf_counter()
baglib.print_time(toc - tic, 'testdata for vbo, pnd, num generated in', printit)

