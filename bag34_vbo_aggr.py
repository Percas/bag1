#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 17 Dec 2022

Doel:
    Maak de tabel voorraad woningen niet woningen
    https://opendata.cbs.nl/statline/#/CBS/nl/dataset/81955NED/table?fromstatweb

    Beginnend met de voorraad uitgesplitst naar gemeente op de 1e van de maand.
    
    Stappen:
        1. Lees vbo, num, opr, wpl in in koppelvlak 3
        vbo = verblijfsobjecten
        num = nummeraanduidingen
        opr = openbare ruimten
        wpl = gemeente
        noot: in koppelvlak 3 is aan elk vbovk precies 1 num vk gekoppeld.
        idem voor num-opr, opr-wpl en wpl-gem.
        
        2. maak de koppeling vbo vk - gem vk via bovenstaande 4 relaties. 
        
        3. bepaal of het vbo vk in voorraad is: daarvoor moet het de status 
        in gebruik, ingebruik niet ingemeten, verbouwing of buiten gebruik 
        hebben. Daarnaast moet het vbo vk te koppelen zijn aan een bestaand
        gem vk (is al geregeld in de vorig stap)
    
        4. selecteer de vbo vk die op de eerste van de maand geldig zijn
        
        5. aggregeer naar gemeente en tel de vbo vk
                  

"""

# ################ import libraries ###############################
import pandas as pd
import numpy as np
import sys
# import os
import time
import baglib
from baglib import BAG_TYPE_DICT
from config import LOCATION


# ############### Define functions ################################

def bag_vbo_aggr(current_month='testdata',
                 koppelvlak3='../data/03-bewerkte-data',
                 koppelvlak4='../data/04-aggr',
                 loglevel=40):

    tic = time.perf_counter()
    
    print('-------------------------------------------')
    print('--- Start bag_vbo_aggr;', current_month, ' -----')
    print('-------------------------------------------')

    # #############################################################################
    # print('00.............Initializing variables...............................')
    # #############################################################################
    # month and dirs
    INPUTDIR = koppelvlak3 + current_month + '/'
    K3DIR = INPUTDIR
    OUTPUTDIR = koppelvlak4 + current_month + '/'
    baglib.make_dir(OUTPUTDIR)
    
    if (current_month == 'testdata') or (current_month == 'backup_testdata'):
        current_year = 2000
    else:
        current_month = int(current_month)
        current_year = int(current_month/100)
    
    # vbo is in voorraad als status is 
    # v3, v4, v6, v8, zie status_dict in bag12_xml2csv.py
    
    IN_VOORRAAD = ['v3', 'v4', 'v6', 'v8']
    pd.set_option('display.max_columns', 20)
    INPUT_FILES_DICT = {'vbo': K3DIR + 'vbo.csv',
                        # 'pnd': K3DIR + 'pnd.csv',
                        'num': K3DIR + 'num.csv',
                        'opr': K3DIR + 'opr.csv',
                        'wpl': K3DIR + 'wpl.csv'}
                        # 'wplgem': K3DIR + 'wplgem.csv'}
        
    vbovk = ['vboid', 'vbovkid']
    # pndvk = ['pndid', 'pndvkid']
    numvk = ['numid', 'numvkid']
    oprvk = ['oprid', 'oprvkid']
    wplvk = ['wplid', 'wplvkid']
    # wplgemvk = ['wplid', 'wplgemvkid']        
    
    KEY_DICT = {'vbo': vbovk,
                # 'pnd': pndvk,
                'num': numvk,
                'opr': oprvk,
                'wpl': wplvk}
    
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    
    FUTURE_DATE = 20321231
   
    '''    
    print('\n---------------DOELSTELLING--------------------------------')
    print('----Bepaal de vbo voorraad per gemeente per eerste vd maand')
    print('-----------------------------------------------------------')
    '''

    # #############################################################################
    print('\n----1. Inlezen van de inputbestanden -----------------------\n')
    # #############################################################################
    
    # print('huidige maand (verslagmaand + 1):', current_month)
    
    bd = baglib.read_input_csv(loglevel, INPUT_FILES_DICT, BAG_TYPE_DICT)

    # #############################################################################
    print('\n----2. maak de koppeling vbo vk - gem vk -----------------------\n')
    # #############################################################################
    
    bd['vbo'] = bd['vbo'].drop(columns=['vbovkid_org', 'pndid', 'pndvkid', 'vbostatus', 'oppervlakte', 'vbogmlx', 'vbogmly']).drop_duplicates()
    bd['vbo'] = pd.merge(bd['vbo'], bd['num'], how='inner', on=numvk)
    bd['vbo'].drop(columns=['numvkid_oud', 'numvkbg', 'numvkeg', 'numstatus', 'huisnr', 'postcode', 'typeao'], inplace=True)
    bd['vbo'] = pd.merge(bd['vbo'], bd['opr'], how='inner', on=oprvk)
    bd['vbo'].drop(columns=['numid', 'numvkid', 'oprvkid_oud', 'oprvkbg', 'oprvkeg', 'oprstatus', 'oprnaam', 'oprtype'], inplace=True)
    bd['vbo'] = pd.merge(bd['vbo'], bd['wpl'], how='inner', on=wplvk)
    bd['vbo'].drop(columns=['oprid', 'oprvkid', 'wplid', 'wplvkid', 'wplvkbg', 'wplvkeg', 'wplstatus'], inplace=True)

    print(bd['vbo'].info())
    print(bd['wpl'].info())

# ########################################################################
print('------------- Start bag_vbo_aggr lokaal ------------- \n')
# ########################################################################


if __name__ == '__main__':

    print('-------------------------------------------')
    print('-------------', LOCATION['OMGEVING'], '-----------')
    print('-------------------------------------------\n')

    DATADIR_IN = LOCATION['DATADIR_IN']
    DATADIR_OUT = LOCATION['DATADIR_OUT']
    DIR03 = DATADIR_IN + '03-bewerktedata/'
    DIR04 = DATADIR_OUT + '04-aggr/'
    current_month = baglib.get_arg1(sys.argv, DIR03)

    ll = 40
    
    bag_vbo_aggr(current_month=current_month,
                    koppelvlak3=DIR03,
                    koppelvlak4=DIR04,
                    loglevel=ll)


