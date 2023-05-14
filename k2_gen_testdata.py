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
from config import BAG_TYPE_DICT, LOGFILE, FILE_EXT, OMGEVING
# import random
import logging
# import bag23_fix_vk
# import bag33_hoofdpnd



# #############################################################################
# print('00.............Initializing variables...............................')
# #############################################################################

tic = time.perf_counter()
ll = 40 # loglevel
file_ext = 'parquet'
FRAC = 0.005
aantal_extract_maanden = 4 # genereer testdata voor afgelopen n maanden

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGFILE),
        logging.StreamHandler()])    
logit = logging.getLogger()
logit.warning(OMGEVING)
logit.setLevel(logging.DEBUG)


logit.info('Start bag_gen_testdata')

# uitgangspunt: draai gen_testdata altijd in ont/bin
BASEDIR = os.path.join('..', '..')
DATADIR = os.path.join(BASEDIR, 'data')
DIR02 = os.path.join(DATADIR, '02-gestandaardiseerd')
DIR03 = os.path.join(DATADIR, '3a-bewerkt')

ONTBASEDIR = '..'
ONTDATADIR = os.path.join(ONTBASEDIR, 'data')
ONTDIR02 = os.path.join(ONTDATADIR, '02-gestandaardiseerd')
ONTDIR03 = os.path.join(ONTDATADIR, '3a-bewerkt')


month_lst = os.listdir(DIR02)
if len(sys.argv) <= 1:
    sys.exit('Usage: bepaal-hoofdpnd <month>, where <month> in '
             + str(month_lst))
current_month = sys.argv[1]
if current_month not in month_lst:
    sys.exit('Usage: bepaal-hoofdpnd <month>, where <month> in '
             + str(month_lst))

logit.info(f'init: meest recente extract_maand: {current_month}')
logit.info(f'gekozen percentage testdata vbo, pnd, num. Ruim: {FRAC}')


# initi
logit.info(f'selecteer ruim {FRAC} random records in num.{FILE_EXT}')

init_num_sample = baglib.read_parquet(input_file=os.path.join(DIR02, current_month, 'num'),
                                      bag_type_d=BAG_TYPE_DICT, output_file_type='pandas',
                                      logit=logit)['numid']
init_num_sample = init_num_sample.sample(frac=FRAC)

init_num_sample = init_num_sample.rename('numid')

print('Debug:', init_num_sample.head())

extract_maand_lst = baglib.make_month_lst(current_month, aantal_extract_maanden)
logit.info(f'genereer testdata voor deze maanden: {extract_maand_lst}')


# TEST_D = {'gemid': ['0160'],
#           'vboid': ['0160010000062544']}

def maak_sample_op_ont(bagobject, input_sample,
                       input_file_prod, output_file_ont,
                       logit=logit):
    ''' Read for bagobject the input_file_prod and take a sample with input_sample and
    save it to output_ont.'''
    
    logit.info(f'inlezen {bagobject}.{FILE_EXT}')

    # _prod_df = pd.read_csv(input_file_prod,
    #                  dtype=BAG_TYPE_DICT)
    _prod_df = baglib.read_parquet(input_file=input_file_prod,
                                   bag_type_d=BAG_TYPE_DICT,
                                   output_file_type='pandas',
                                   logit=logit)
    logit.info(f'maken van het sample voor {bagobject}')
    _sample = pd.merge(_prod_df, input_sample, how='inner')
    logit.info(f'opslaan van het sample voor {bagobject}')
    # _sample.to_csv(output_ont, index=False)
    baglib.save_df2file(df=_sample, outputfile=output_file_ont,
                        file_ext=FILE_EXT, includeindex=False, append=False, logit=logit)
    logit.info(f'daadwerkelijkr grootte {bagobject} sample: {_sample.shape[0]/_prod_df.shape[0]}')
    return _sample




logit.info('start de loop over de extract maanden')
for extract_maand in extract_maand_lst:

    logit.info(f'bezig met maand: {extract_maand}')

    k2dir = os.path.join(DIR02, extract_maand)
    k3dir = os.path.join(DIR03, extract_maand)
    ontk2dir = os.path.join(ONTDIR02, extract_maand)
    ontk3dir = os.path.join(ONTDIR03, extract_maand)
    logit.info('mappen aanmaken in de ontwikkelomgeving ont (indien nodig):')
    baglib.make_dirs(ontk2dir)
    baglib.make_dirs(ontk3dir)

    '''
    INPUT_FILES_DICT = {'vbo': os.path.join(k2dir, 'vbo'),
                        'pnd': os.path.join(k2dir, 'pnd'),
                        'num': os.path.join(k2dir, 'num')}
                        # 'vbovk_hoofdpndvk': os.path.join(k3dir,'vbovk_hoofdpndvk')}

    OUTPUT_FILES_DICT = {'vbo': os.path.join(ontk2dir, 'vbo'),
                         'pnd': os.path.join(ontk2dir, 'pnd'),
                         'num': os.path.join(ontk2dir, 'num')}
                         # 'vbovk_hoofdpndvk': os.path.join(ontk3dir,'vbovk_hoofdpndvk')}
    '''
    
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    ontbd = {}
    
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk}
      
    

    # ################## Inlezen ################################



    # maak en bewaar het num sample voor de extract_maand uit het initiele num sample
    maak_sample_op_ont(bagobject='num',
                       input_sample=init_num_sample,
                       input_file_prod=os.path.join(DIR02, extract_maand, 'num'),
                       output_file_ont=os.path.join(ONTDIR02, extract_maand, 'num'))


    # maak het vbo_sample uit het num sample
    vbo_sample = maak_sample_op_ont(bagobject='vbo',
                                    input_sample=init_num_sample,
                                    input_file_prod=os.path.join(DIR02, extract_maand, 'vbo'),
                                    output_file_ont=os.path.join(ONTDIR02, extract_maand, 'vbo'))

    # baglib.debugprint(title='na het maken van het vbo sample',
    #                   df=vbo_sample,
    #                   colname='vboid',
    #                   vals=TEST_D['vboid'],
    #                   loglevel=40)

    # use the pnd in the vbo_sample to create the pnd sampl
    maak_sample_op_ont(bagobject='pnd',
                       input_sample=vbo_sample['pndid'].drop_duplicates().rename('pndid'),
                       input_file_prod=os.path.join(DIR02, extract_maand, 'pnd'),
                       output_file_ont=os.path.join(ONTDIR02, extract_maand, 'pnd'))


    # maak_sample_op_ont(bagobject='vbovk_hoofdpndvk',
    #                    input_sample=vbo_sample['vboid'].drop_duplicates().rename('vboid'),
    #                    input_file_prod=os.path.join(DIR3a, extract_maand, 'vbovk_hoofdpndvk'),
    #                    output_file_ont=os.path.join(ONTDIR3a, extract_maand, 'vbovk_hoofdpndvk'))





    logit.info('kopieer de rest van de bestanden\n')
    rest = ['sta', 'lig', 'opr', 'wpl']
    for r in rest:
        print('\t1-1 kopieren van', r, 'naar ontwikkelomgeving')
        shutil.copy(os.path.join(k2dir, r + '.' + file_ext), ontk2dir)
    
    # bag23_fix_vk.bag_fix_vk(current_month=current_month,
    #                         koppelvlak3=ONTDIR03,
    #                         koppelvlak2=ONTDIR02,
    #                         loglevel=ll)
    # leidt voor elk vbo voorkomen (vbovk) een precies 1 pndvk af. Het hoofdpndvk

    
    
toc = time.perf_counter()
logit.info(f'*** gen_testdata duurde {(toc - tic)/60} min')
