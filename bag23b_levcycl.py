#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on June 11, 2022

Purpose: make the levenscyclus file for VBO: 
    vbovk met bouwjaar en typeinliggend
    restriction: vbovk must be in IN_VOORRAAD

stappen:
    
    1. inlezen van
        vbo (k2)
        vbovk-pndvk (k3)
        pnd (k2)
        vbo-wpl (k3)
    2. koppel alles aan vbo
    3. rename de velden en bewaar
    
    
0.1: initial version based on 23-bew-data.py
0.2: nieuwe versie met flink wat input eerder gemaakt

Notes:
    voorkomen id zou een goede toevoeging zijn in het levenscyclus bestand
    net als inliggend ipv type inliggend

"""

# ################ import libraries ###############################
import pandas as pd
import baglib
from baglib import BAG_TYPE_DICT
import os
import sys
import time
from config import LOCATION


# import numpy as np


# ############### Define functions ################################

def bag_levcycl(current_month='202208',
                koppelvlak2='../data/02-csv',
                koppelvlak3='../data/03-bewerkte-data',
                loglevel=True):
    '''Maak het levenscyclus bestand met VBO voorkomens, waarin het bouwjaar,
    inliggend, voorraad en alle mogelijke woonfuncties van dit vbovk zijn
    afgeleid.'''

    tic = time.perf_counter()
   
    print('-------------------------------------------')
    print('------------- Start bag_vbovk_levcycl -----')
    print('-------------------------------------------')

    INPUTDIR = koppelvlak2 + current_month + '/'
    K2DIR = INPUTDIR
    K3DIR = koppelvlak3 + current_month + '/'
    OUTPUTDIR = koppelvlak3 + current_month + '/'
    # IN_VOORRAAD = ['inge', 'inni', 'verb', 'buig']
    
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    
    KEY_DICT = {'vbo': vbovk,
                'vbovk-wplvk': vbovk,
                'vbovk-pndvk': vbovk,
                'vbovk-nvbo': vbovk,
                'pnd': pndvk}    
    
    INPUT_FILES_DICT = {'vbo': K2DIR + 'vbo.csv',
                       'pnd': K2DIR + 'pnd.csv',
                       'vbovk-pndvk': K3DIR + 'vbovk-pndvk.csv',
                       'vbovk-wplvk': K3DIR + 'vbovk-wplvk.csv',
                       'vbovk-nvbo':  K3DIR + 'vbovk-nvbo.csv'}
    
    # bagobj_d = {} # dict to store the bagobject df's
    # peildatum = baglib.last_day_of_month(current_month)
    LEVCYCL_COLS = {
        'vboid': 'OBJECTNUMMER',
        'vbovkbg': 'AANVLEVCYCLWOONNIETWOON',
        'vbovkeg': 'EINDLEVCYCLWOONNIETWOON',
        # 'dummy1': 'KENMERKWIJZIGDATUM',
        'voorraad': 'INVOORRAAD',
        # 'voorraadtype': 'VBOVOORRAADTYPE',
        'bouwjaar': 'VBOBOUWJAAR',
        # 'typeinliggend': 'VBOTYPEINLIGGEND',
        'inliggend': 'VBOINLIGGEND',        #
        'oppervlakte': 'VBOOPPERVLAKTE',
        'vbostatus': 'VBOSTATUS',
        'woon': 'VBOWOONFUNCTIE',
        'over': 'VBOOVERIGE_GEBRUIKSFUNCTIE',
        'kant': 'VBOKANTOORFUNCTIE',
        'gezo': 'VBOGEZONDHEIDSFUNCTIE',
        'bij1': 'VBOBIJEENKOMSTFUNCTIE',
        'ondr': 'VBOONDERWIJSFUNCTIE',
        'wink': 'VBOWINKELFUNCTIE',
        'sprt': 'VBOSPORTFUNCTIE',
        'logi': 'VBOLOGIESFUNCTIE',
        'indu': 'VBOINDUSTRIEFUNCTIE',
        'celf': 'VBOCELFUNCTIE'}
    
    pd.set_option('display.max_columns', 20)
    
    
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    nrec = {}       # aantal records
    nkey = {}       # aantal keyrecords
    
    # if printit = True bepaal-hoofdpnd prints extra info
    printit = True



    # ############################################################################
    print('\n----1. Inlezen bagobjecten die we nodig hebben-------------------\n')
    # ############################################################################
    
    tic = time.perf_counter()
    
    
    bd = baglib.read_input_csv(INPUT_FILES_DICT, BAG_TYPE_DICT)
    
    print()
    
    for bob in INPUT_FILES_DICT.keys():
        bd[bob].set_index(KEY_DICT[bob], inplace=True)
        (nrec[bob], nkey[bob]) = baglib.df_comp(bd[bob])
        print('\trecords', bob + ':', nrec[bob], '\tvk',
              KEY_DICT[bob], ':', nkey[bob])
    
    
    # ############################################################################
    print('\n----2. Koppel alles aan de vbovk---------------------------------\n')
    # ############################################################################
    
    print('\t--- 2.1 Verwijder pnd uit vbo en ontdubbel vbovk')
    # bd['vbo'].set_index(vbovk, inplace=True)
    bd['vbo'].drop(axis=1, columns=['pndid'], inplace=True)
    bd['vbo'] = baglib.ontdubbel_idx_maxcol(bd['vbo'], ['numid'])
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'],
                                                nrec=nrec['vbo'], nkey=nkey['vbo'],
                                                u_may_change=True)
    
    print('\n\t--- 2.2 koppel pndvk erbij')
    
    bd['vbo'] = pd.merge(bd['vbo'], bd['vbovk-pndvk'], how='left',
                         left_on=vbovk, right_on=vbovk)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'],
                                                nrec=nrec['vbo'], nkey=nkey['vbo'],
                                                u_may_change=False)
    bd['vbo'] = baglib.recast_df_floats(bd['vbo'], BAG_TYPE_DICT)
    
    
    
    print('\n\t--- 2.2 koppel wplvk erbij')
    
    bd['vbo'] = pd.merge(bd['vbo'], bd['vbovk-wplvk'], how='left',
                         left_on=vbovk, right_on=vbovk)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'],
                                                nrec=nrec['vbo'], nkey=nkey['vbo'],
                                                u_may_change=False)
    
    print('\n\t--- 2.3 koppel vbovk_nvbo erbij')
    
    bd['vbo'] = pd.merge(bd['vbo'], bd['vbovk-nvbo'], how='left',
                         left_on=vbovk, right_on=vbovk)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'],
                                                nrec=nrec['vbo'], nkey=nkey['vbo'],
                                                u_may_change=False)
    
    # I dont know why, but merging on something else then index screws things up
    # So lets reset it for now:
    bd['vbo'] = bd['vbo'].reset_index()
    
    print('\n\t--- 2.4 koppel pnd erbij')
    
    bd['vbo'] = pd.merge(bd['vbo'], bd['pnd'], how='left',
                         left_on=pndvk, right_on=pndvk)
    
    
    
    # and set it again
    # bd['vbo'] = bd['vbo'].set_index(vbovk)
    
    
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], key_lst=vbovk,
                                                nrec=nrec['vbo'], nkey=nkey['vbo'],
                                                u_may_change=False)
    
    
    bd['vbo'] = baglib.recast_df_floats(bd['vbo'], BAG_TYPE_DICT)
    
    
    # print('DEBUG0')
    # print(bd['vbo'].info())
    
    
    # bd['vbo'] = bd['vbo'].reset_index()
    # bd['vbo'] = bd['vbo'].columns(LEVCYCL_COLS)[LEVCYCL_COLS.values()]
    
    # print('DEBUG1')
    # print(bd['vbo'].info())
    
    bd['vbo'] = bd['vbo'].rename(columns =LEVCYCL_COLS)
    bd['vbo'] = bd['vbo'].rename(columns =LEVCYCL_COLS)[LEVCYCL_COLS.values()]
    
    print('DEBUG2')
    print(bd['vbo'].info())
    
    
    print('\n\tWegschrijven naar levcycl.csv')
    outputfile = OUTPUTDIR + 'levcycl.csv'
    bd['vbo'].to_csv(outputfile, index=False)
    
    toc = time.perf_counter()
    baglib.print_time(toc - tic, '\n------------- Einde bag_vbovk_pndvk in',
                      loglevel)


'''             
# ########################################################################
print('------------- Start bag_levcycl lokaal ------------- \n')
# ########################################################################
'''

if __name__ == '__main__':

    print('-------------------------------------------')
    print('-------------', LOCATION['OMGEVING'], '-----------')
    print('-------------------------------------------\n')

    DATADIR_IN = LOCATION['DATADIR_IN']
    DATADIR_OUT = LOCATION['DATADIR_OUT']
    DIR00 = DATADIR_IN + '00-zip/'
    DIR01 = DATADIR_OUT + '01-xml/'
    DIR02 = DATADIR_OUT + '02-csv/'
    DIR03 = DATADIR_OUT + '03-bewerktedata/'
    current_month = baglib.get_arg1(sys.argv, DIR02)
    printit=True

    bag_levcycl(current_month=current_month,
                koppelvlak2=DIR02,
                koppelvlak3=DIR03,
                loglevel=printit)


