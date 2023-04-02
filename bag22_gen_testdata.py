#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Doel: maak testdata door een subset te nemen van vbo.csv, pnd.csv, num.csv 
van x %, als volgt

1. neem x % num
2. vbo die bij num horen
3. pnd die bij vbo horen
4. kopieer sta, lig, opr, wpl in zijn geheel
5. neem vbovk in vbovk_hoofdpndvk.csv die bij vbo horen

Aanpak:
    1. lees num, vbo, pnd, vbovk_hoofdpndvk (de laatste uit koppelvlak 3)
    2. selecteer x % num
    3. selecteer hierbij de vbo
    4. selecteer hierbij de pnd
    5. schrijf alles weg
    
0.2 bugfix: koppeling num met vbo levert te veel records. Je moet unieke
numid nemen (natuurlijk)    

03. inlezen met inlees functie, timing, kopieer ook sta, lig, opr, wpl

04. loop toegevoegd om extracten te vergelijken: er komt een init bij en stap 2 wijzigt

    Init:
        0. Neem x % num van de laatste maand: init_num
    Loop over de laatste 5 extractmaanden:
        1. lees num, vbo, pnd, vbovk_hoofdpndvk (de laatste uit koppelvlak 3)
        2. selecteer uit num die num die ook in init_num zitten
        3. selecteer hierbij de vbo
        4. selecteer hierbij de pnd
        5. schrijf alles weg
        
        
"""


import pandas as pd
# import numpy as np
import sys
import os
import baglib
import time
import shutil
# from pathlib import Path
from config import BAG_TYPE_DICT
import random
import bag23a_fix_vk
import bag33_hoofdpnd



# #############################################################################
# print('00.............Initializing variables...............................')
# #############################################################################

tic = time.perf_counter()
ll = 40 # loglevel

baglib.printkop(ll+40, 'Start bag_gen_testdata')
baglib.aprint(ll, 'loglevel is', ll, '\n')

# uitgangspunt: draai gen_testdata altijd in ont/bin
BASEDIR = os.path.join('..', '..')
DATADIR = os.path.join(BASEDIR, 'data')
DIR02 = os.path.join(DATADIR, '02-csv')
DIR03 = os.path.join(DATADIR, '03-bewerktedata')

ONTBASEDIR = '..' # BASEDIR + 'ont/'
ONTDATADIR = os.path.join(ONTBASEDIR, 'data')
ONTDIR02 = os.path.join(ONTDATADIR, '02-csv')
ONTDIR03 = os.path.join(ONTDATADIR, '03-bewerktedata')


month_lst = os.listdir(DIR02)
if len(sys.argv) <= 1:
    sys.exit('Usage: bepaal-hoofdpnd <month>, where <month> in '
             + str(month_lst))
current_month = sys.argv[1]
if current_month not in month_lst:
    sys.exit('Usage: bepaal-hoofdpnd <month>, where <month> in '
             + str(month_lst))

PERC = 0.005
baglib.aprint(ll, '\tInit: meest recente extract_maand:', current_month)
baglib.aprint(ll, '\tGekozen percentage testdata vbo, pnd, num. Ruim:', PERC)


# initi
baglib.aprint(ll+20, '\n\tSelecteer ruim', PERC, 'random records in num.csv')

init_num_sample = pd.read_csv(os.path.join(DIR02, current_month, 'num.csv'),
                              header=0,
                              skiprows=lambda i: i>0 and random.random() > PERC,
                              dtype=BAG_TYPE_DICT)['numid'].drop_duplicates()
init_num_sample = init_num_sample.rename('numid')

aantal_extract_maanden = 5 # doe het voor afgelopen n maanden
extract_month_lst = baglib.make_month_lst(current_month, aantal_extract_maanden)

baglib.aprint(ll+20, '\n\tGenereer testdata voor deze maanden:')
baglib.aprint(ll+20, '\t' + str(extract_month_lst))


# TEST_D = {'gemid': ['0160'],
#           'vboid': ['0160010000062544']}


baglib.aprint(ll, '\n\tStart de loop over de extract maanden')
for extract_month in extract_month_lst:

    baglib.aprint(ll+40, '\n\n\tBezig met maand:', extract_month)

    k2dir = os.path.join(DIR02, extract_month)
    k3dir = os.path.join(DIR03, extract_month)
    ontk2dir = os.path.join(ONTDIR02, extract_month)
    ontk3dir = os.path.join(ONTDIR03, extract_month)
    baglib.aprint(ll, '\tMappen aanmaken in de ontwikkelomgeving ont (indien nodig):')
    baglib.make_dir(ontk2dir)
    baglib.make_dir(ontk3dir)

    INPUT_FILES_DICT = {'vbo': os.path.join(k2dir, 'vbo.csv'),
                        'pnd': os.path.join(k2dir, 'pnd.csv'),
                        'num': os.path.join(k2dir, 'num.csv'),
                        'vbovk_hoofdpndvk': os.path.join(k3dir,'vbovk_hoofdpndvk.csv')}

    OUTPUT_FILES_DICT = {'vbo': os.path.join(ontk2dir, 'vbo.csv'),
                         'pnd': os.path.join(ontk2dir, 'pnd.csv'),
                         'num': os.path.join(ontk2dir, 'num.csv'),
                         'vbovk_hoofdpndvk': os.path.join(ontk3dir,'vbovk_hoofdpndvk.csv')}

    
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    ontbd = {}
    
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk}
      
    

    # ################## Inlezen ################################
    baglib.aprint(ll)


    def maak_sample_op_ont(ll, bob, input_sample, input_prod, output_ont):
        baglib.aprint(ll, '\t\tInlezen', bob+'.csv')
        _prod_df = pd.read_csv(input_prod,
                         dtype=BAG_TYPE_DICT)
        baglib.aprint(ll, '\t\tMaken van het sample voor', bob)
        _sample = pd.merge(_prod_df, input_sample, how='inner')
        baglib.aprint(ll, '\t\tOpslaan van het sample voor', bob)
        _sample.to_csv(output_ont, index=False)
        baglib.aprint(ll, '\t\tDaadwerkelijkr grootte', bob, 'sample:',
                      _sample.shape[0]/_prod_df.shape[0])
        return _sample


    maak_sample_op_ont(ll, 'num', init_num_sample,
                       os.path.join(DIR02, extract_month, 'num.csv'),
                       OUTPUT_FILES_DICT['num'])

    vbo_sample = maak_sample_op_ont(ll, 'vbo', init_num_sample,
                                    os.path.join(DIR02, extract_month, 'vbo.csv'),
                                    OUTPUT_FILES_DICT['vbo'])

    # baglib.debugprint(title='na het maken van het vbo sample',
    #                   df=vbo_sample,
    #                   colname='vboid',
    #                   vals=TEST_D['vboid'],
    #                   loglevel=40)

    maak_sample_op_ont(ll, 'pnd',
                       vbo_sample['pndid'].drop_duplicates().rename('pndid'),
                       os.path.join(DIR02, extract_month, 'pnd.csv'),
                       OUTPUT_FILES_DICT['pnd'])


    maak_sample_op_ont(ll, 'vbovk_hoofdpndvk',
                       vbo_sample['vboid'].drop_duplicates().rename('vboid'),
                       os.path.join(DIR03, current_month, 'vbovk_hoofdpndvk.csv'),
                       OUTPUT_FILES_DICT['vbovk_hoofdpndvk'])




    baglib.aprint(ll+20, '\n\tKopieer de rest van de bestanden\n')
    rest = ['sta', 'lig', 'opr', 'wpl']
    for r in rest:
        print('\t1-1 kopieren van', r, 'naar ontwikkelomgeving')
        shutil.copy(os.path.join(k2dir, r + '.csv'), ontk2dir)
    
    bag23a_fix_vk.bag_fix_vk(current_month=current_month,
                             koppelvlak3=ONTDIR03,
                             koppelvlak2=ONTDIR02,
                             loglevel=ll)
    # leidt voor elk vbo voorkomen (vbovk) een precies 1 pndvk af. Het hoofdpndvk

    
    
toc = time.perf_counter()
baglib.aprint(ll+40,'\n*** gen_testdata duurde', (toc - tic)/60, 'min')
