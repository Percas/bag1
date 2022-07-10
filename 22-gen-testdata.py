#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spyder Editor

Doel: maak een subset van vbo.csv, pnd.csv, num.csv van x %, (x commandline),
als volgt
1. neem x % num
2. vbo die bij num horen
3. pnd die bij vbo horen

Aanpak:
    1. lees num, vbo, pnd
    2. selecteer x % num
    3. selecteer hierbij de vbo
    4. selecteer hierbij de pnd
    5. schrijf alles weg
    
0.2 bugfix: koppeling num met vbo levert te veel records. Je moet unieke
numid nemen (natuurlijk)    

"""

import pandas as pd
import numpy as np
import sys
import os
# import baglib
from pathlib import Path



# #############################################################################
# print('00.............Define functions...............................')
# #############################################################################
def read_csv(inputdir, file_with_bag_objects, dtype_dict, vkid_cols):
    """
    Read voorkomens from file in inputdir, do some counting.

    Returns
    -------
    Dataframe with voorkomens.

    """
    # print('\tread ', file_with_bag_objects, '...')
    _df = pd.read_csv(inputdir + file_with_bag_objects,
                      dtype=dtype_dict)
    _all_voorkomens = _df.shape[0]
    _all_kadaster_voorkomens = _df[vkid_cols].drop_duplicates().shape[0]
    _verschil = _all_voorkomens - _all_kadaster_voorkomens
    print('\tVoorkomens totaal:', _all_voorkomens)
    print('\tVoorkomens cf kadaster (unieke vk):', _all_kadaster_voorkomens)
    print('\tZelf aangemaakte voorkomens:', _verschil)
    return _df

def get_df_from_csv(idir, csv_file, dtype_dict, cols):
    """Return dataframe from csv_file in dir_path."""
    try:
        # prtxt('Contents of this dir: ' +
        #          str(os.listdir(idir)))
        _df = read_csv(idir,
                       csv_file,
                       dtype_dict, cols)
        return _df
    except FileNotFoundError:
        print('Error: kan dit bestand niet openen: ' +
                  idir + csv_file)


# #############################################################################
# print('00.............Initializing variables...............................')
# #############################################################################
'''
os.chdir('..')
BASEDIR = os.getcwd() + '/'
baglib.print_omgeving(BASEDIR)
'''
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

INPUTDIR = DIR02 + current_month + '/'
OUTPUTDIR = ONTDIR02 + current_month + '/'

current_month = int(current_month)
current_year = int(current_month/100)

PERC = 1
# #############################################################################
print('\n----1. Inlezen van vbo.csv, pnd.csv en num.csv---------------------')
# #############################################################################

print('\n\t1.1 VBO:------------------------------------------')
vbovk_df = get_df_from_csv(INPUTDIR, 'vbo.csv',
                           {'vboid': str, 'status': str,
                            'pndid': str, 'numid' : str,
                            'vbovkid': np.short,
                            'vbovkbg': int,
                            'vbovkeg': int},
                           ['vboid', 'vbovkid'])
# vbovk_df.drop(['idx'], axis=1, inplace=True)
# vbo_cols = list(vbovk_df.columns)
vbovk_u = vbovk_df[['vboid', 'vbovkid']].drop_duplicates()
n_vbovk = vbovk_df.shape[0]
n_vbovk_u = vbovk_u.shape[0]
verschil = n_vbovk - n_vbovk_u
perc = round(100 * verschil / n_vbovk_u, 2)
print('\tPerc zelf aangemaakt (=vbo met dubbel pnd):', perc)

print('\n\t1.2 PND:------------------------------------------')
pndvk_df = read_csv(INPUTDIR, 'pnd.csv',
                    {'pndid': str, 'pndstatus': str,
                     'pndvkid': np.short, 'pndvkbg': int,
                     'pndvkeg': int, 'bouwjaar': np.short,
                     'docnr': str}, ['pndid', 'pndvkid'])
# pndvk_df.drop(['idx'], axis=1, inplace=True)
# pnd_cols = list(pndvk_df.columns)

print('\n\t1.2 NUM:------------------------------------------')
numvk_df = read_csv(INPUTDIR, 'num.csv',
                    {'numid': str, 'numstatus': str,
                    'numvkid': np.short, 'numvkbg': int,
                    'numvkeg': int}, ['numid', 'numvkid'])
try:
    numvk_df.drop(['idx'], axis=1, inplace=True)
except KeyError:
    pass

print('\n----Selecteer', PERC, 'procent van records in num.csv---------------')
ontnumvk_df = numvk_df.sample(frac=PERC/100)

print('\tSelecteer alle vk van de gekozen NUM')
ontnumvk_df = pd.merge(ontnumvk_df['numid'].drop_duplicates(),
                       numvk_df, how='inner')
print('\tPercentage num records geselecteerd:',
      round(100 * ontnumvk_df.shape[0] / numvk_df.shape[0], 3))
# print(ontnumvk_df.info())

print('\tSelecteer alle vbovk op dat num...')
ontvbovk_df = pd.merge(vbovk_df,
                       ontnumvk_df['numid'].drop_duplicates(),
                       how='inner')
# print(ontvbovk_df.info())
print('\tPercentage vbo records geselecteerd:',
      round(100 * ontvbovk_df.shape[0] / vbovk_df.shape[0], 3))

print('\tSelecteer bij die vbos de bijbehordende panden...')
ontpndvk_df = pd.merge(ontvbovk_df['pndid'].drop_duplicates(),
                       pndvk_df, how='inner')
# print(ontpndvk_df.info())
print('\tPercentage pnd records geselecteerd:',
      round(100 * ontpndvk_df.shape[0] / pndvk_df.shape[0], 3))

print('\tCreating directories and files in ONT:')

Path(OUTPUTDIR).mkdir(parents=True, exist_ok=True)

print('\tWriting', ontnumvk_df.shape[0], 'records num.csv to',
      OUTPUTDIR, '...')
outputfile = OUTPUTDIR + 'num.csv'
ontnumvk_df.to_csv(outputfile, index=False)

print('\tWriting', ontpndvk_df.shape[0], 'records pnd.csv to',
      OUTPUTDIR, '...')
outputfile = OUTPUTDIR + 'pnd.csv'
ontpndvk_df.to_csv(outputfile, index=False)

print('\tWriting', ontvbovk_df.shape[0], 'records vbo.csv to',
      OUTPUTDIR, '...')
outputfile = OUTPUTDIR + 'vbo.csv'
ontvbovk_df.to_csv(outputfile, index=False)
