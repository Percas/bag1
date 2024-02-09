#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on June 11, 2022
Updated Oct 2023

Doel: maak een levenscyclus bestand met vbo voorkomens, waarin je voor elk voorkomen bepaalt:
    bouwjaar (obv het gekoppelde hoofdpand voorkomen)
    inliggendheid (met zijn hoevelen het vbovk in een hoofdpand liggen)
    in_voorraad (of het vbovk de juiste status heeft)

stappen:
    
    1. inlezen van
        vbo (k3)
        vbovk-hoofdpndvk (k3)
        pnd (k3)
        vbo-wpl (k3)
    2. koppel alles aan vbo
    3. rename de velden en bewaar
    
    
0.1: initial version based on 23-bew-data.py
0.2: nieuwe versie met flink wat input eerder gemaakt
0.3: versie gebaseerd op gefixte vk uit k3

Notes:
    voorkomen id zou een goede toevoeging zijn in het levenscyclus bestand
    net als inliggend ipv type inliggend

"""

# ################ import libraries ###############################
import pandas as pd
import baglib
import os
import sys
import time
from config import OMGEVING, DIR03, BAG_TYPE_DICT


# import numpy as np


# ############### Define functions ################################

def k3_levcycl(maand='testdata23', ):
    '''Maak het levenscyclus bestand met VBO voorkomens, waarin het bouwjaar,
    inliggend, voorraad en alle mogelijke woonfuncties van dit vbovk zijn
    afgeleid.'''

    tic = time.perf_counter()
    ll= loglevel
    baglib.printkop(ll+40, 'Start bag_levcycl')
    K3DIR = os.path.join(koppelvlak3, current_month)
    # OUTPUTDIR = koppelvlak3 + current_month + '/'
    # IN_VOORRAAD = ['inge', 'inni', 'verb', 'buig']
    
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    
    KEY_DICT = {'vbo': vbovk,
                'vbovk-wplvk': vbovk,
                'vbovk-hoofdpndvk': vbovk,
                'vbovk-nvbo': vbovk,
                'pnd': pndvk}    
    
    INPUT_FILES_DICT = {'vbo': os.path.join(K3DIR, 'vbo.csv'),
                       'pnd': os.path.join(K3DIR, 'pnd.csv'),
                       'vbovk-hoofdpndvk': os.path.join(K3DIR, 'vbovk_hoofdpndvk.csv'),
                       # 'vbovk-gemvk': K3DIR + 'vbovk_gemvk.csv',
                       'vbovk-nvbo':  os.path.join(K3DIR, 'vbovk_nvbo.csv')}
    
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
    
    TEST_D = {'vbo': ['0003010000133554']}



    # #################################################################
    baglib.printkop(ll+30, '1. Inlezen bagobjecten die we nodig hebben')
    
    bd = baglib.read_input_csv(ll, INPUT_FILES_DICT, BAG_TYPE_DICT)
    
    for bob in INPUT_FILES_DICT.keys():
        (nrec[bob], nkey[bob]) = baglib.df_comp(ll, bd[bob], key_lst=KEY_DICT[bob])
        baglib.aprint(ll, '\n\trecords', bob + ':', nrec[bob], '\tvk',
              KEY_DICT[bob], ':', nkey[bob])

        # baglib.aprint(ll, '\t1a. Controle op dubbele voorkomens')
        tmp_df = baglib.find_double_vk(bd[bob],  KEY_DICT[bob][0], KEY_DICT[bob][1])
        
        # print(tmp_df.info())
        max1 = tmp_df['aantal'].iloc[0]
        # str1 = '\t' + tmp_df.head(10).to_str().replace('\n', '\n\t')
        if max1 > 1: 
            baglib.aprint(ll, '\tDubbele vk in', bob+'aflopend gesorteerd:')
            baglib.aprint(ll, tmp_df.head(10))
            baglib.aprint(ll, '\n')
        else:
            baglib.aprint(ll, '\tgeen dubbele voorkomens')
    
    baglib.aprint(ll, '\n\t1b. Controle op aantallen')
    # a_unieke_vbovk = nkey['vbo']
    # a_unieke_pndvk = nkey['pnd']
    # a_unieke_vbovk_koppeltabel = nkey['vbovk-hoofdpndvk']
    a_unieke_pndvk_koppeltabel = bd['vbovk-hoofdpndvk'][pndvk].drop_duplicates().shape[0]
    baglib.aprint(ll, '\taantal_unieke_pndvk_koppeltabel:', a_unieke_pndvk_koppeltabel)
    # (both, df_1not2, df_2not1) = baglib.diff_df(bd['vbovk-hoofdpndvk'][pndvk], bd['pnd'][pndvk]) 
    # baglib.aprint(ll, df_1not2.head(), df_2not1.head())
    
    baglib.debugprint(title='\t\tNa het inlezen:',
                      df=bd['vbo'], colname='vboid',
                      vals=TEST_D['vbo'], loglevel=ll)



    # #################################################################
    baglib.printkop(ll+30, '2. Koppel aan vbovk_hoofdpndvk: a, b, c')


    baglib.aprint(ll+20, '\t 2a. vbovk-hoofdpndvk -> vbo geeft numvk, opp, ...\n')
    dropcols = ['pndid', 'pndvkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'vbovkid_org']
    levcycl = pd.merge(bd['vbovk-hoofdpndvk'],
                       bd['vbo'].drop(columns=dropcols).drop_duplicates(), how='inner', on=vbovk)
    (nrec1, nkey1) = baglib.df_comp(ll, levcycl, key_lst=vbovk, nrec=nrec['vbo'], nkey=nkey['vbo'], u_may_change=False)
    
    # print(levcycl.info())

    baglib.aprint(ll+20, '\n\t 2b. vbovk-hoofdpndvk -> pnd geeft bouwjaar, ...\n')
    dropcols = ['pndvkbg', 'pndvkeg', 'pndstatus', 'docnr', 'docdd']
    levcycl = pd.merge(levcycl,
                       bd['pnd'].drop(columns=dropcols), how='inner', on=pndvk)
    (nrec1, nkey1) = baglib.df_comp(ll, levcycl, key_lst=vbovk, nrec=nrec1, nkey=nkey1, u_may_change=False)

    # print(levcycl.info())

    baglib.aprint(ll+20, '\n\t 2c. vbovk-hoofdpndvk -> , vbovk_nvbo geeft inliggend...\n')
    dropcols = ['pndid', 'pndvkid']
    levcycl = pd.merge(levcycl,
                       bd['vbovk-nvbo'].drop(columns=dropcols), how='inner', on=vbovk)
    (nrec1, nkey1) = baglib.df_comp(ll, levcycl, key_lst=vbovk, nrec=nrec1, nkey=nkey1, u_may_change=False)



    # baglib.aprint(ll, '\t1a. Controle op dubbele voorkomens')
    tmp_df = baglib.find_double_vk(levcycl,  'vboid', 'vbovkid')
    
    # print(tmp_df.info())
    max1 = tmp_df['aantal'].iloc[0]
    # str1 = '\t' + tmp_df.head(10).to_str().replace('\n', '\n\t')
    if max1 > 1: 
        baglib.aprint(ll, '\tDubbele vk in levcycl aflopend gesorteerd:')
        baglib.aprint(ll, tmp_df.head(10))
        baglib.aprint(ll, '\n')
    else:
        baglib.aprint(ll, '\tgeen dubbele voorkomens')


   
    
    # levcycl = levcycl.rename(columns =LEVCYCL_COLS)
    # bd['vbo'] = bd['vbo'].rename(columns =LEVCYCL_COLS)[LEVCYCL_COLS.values()]
    
    
    # #################################################################
    baglib.printkop(ll+30, '3. Wegschrijven naar levcycl.csv ')

    outputfile = os.path.join(K3DIR, 'levcycl.csv')
    levcycl.to_csv(outputfile, index=False)
    
    toc = time.perf_counter()
    baglib.aprint(ll+40, '\n*** Einde bag_levcycl in', (toc - tic)/60, 'min ***\n')


# --------------------------------------------------------------------------
# ################ Main program ###########################
# --------------------------------------------------------------------------

if __name__ == '__main__':

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOGFILE),
            logging.StreamHandler()])
    logit = logging.getLogger()
    logit.debug('Start k3_levcycl lokaal')

    maand_lst = baglib.get_args(sys.argv, KOPPELVLAK0)

    for maand in maand_lst:
            k3_levcycl(maand, logit)



