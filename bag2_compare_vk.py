#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Monday Jan 9, 2023

In de BAG mogen afgesloten voorkomens van bag objecten niet meer wijzigen.

Doel: controleer voor gegeven maand t of de in t-1 afgesloten voorkomens (vk)
ongewijzigd in maand t aanwezig zijn.

Stappen:
    
    0: lees bagobjecten (bob) in
    1. loop over bob:
        onderzoek afgesloten vk van bob eenheden uit t-1 die weg zijn in t
        ook interessant zijn vk van bob eenheden in t met een oude begindatum,
        die niet in t-1 aanwezig zijn


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
from config import FUTURE_DATE




# ############### Define functions ################################

def bag_compare_vk(loglevel = 10,
                   current_month='testdata23',
                   koppelvlak2='../data/02-csv',
                   future_date=FUTURE_DATE):

    tic = time.perf_counter()
    ll = loglevel
    
    baglib.aprint(ll+40, '-------------------------------------------')
    baglib.aprint(ll+40, '--- Start bag_compare_vk', current_month, ' -----')
    baglib.aprint(ll+40, '-------------------------------------------')

    # #############################################################################
    # baglib.aprint(ll+40, '00.............Initializing variables...............................')
    # #############################################################################
    # month and dirs
    
    prev_month = baglib.prev_month(current_month)
    K2DIR = koppelvlak2 + current_month + '/'
    PREVDIR = koppelvlak2 +  prev_month + '/'
    # current_year = int(current_month/100)
    
    pd.set_option('display.max_columns', 20)
    
    CURR_FILES_DICT = {'vbo': K2DIR + 'vbo.csv',
                       'pnd': K2DIR + 'pnd.csv',
                       'num': K2DIR + 'num.csv',
                       'opr': K2DIR + 'opr.csv',
                       'wpl': K2DIR + 'wpl.csv'}

    PREV_FILES_DICT = {'vbo': PREVDIR + 'vbo.csv',
                       'pnd': PREVDIR + 'pnd.csv',
                       'num': PREVDIR + 'num.csv',
                       'opr': PREVDIR + 'opr.csv',
                       'wpl': PREVDIR + 'wpl.csv'}
        
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    numvk = ['numid', 'numvkid']
    oprvk = ['oprid', 'oprvkid']
    wplvk = ['wplid', 'wplvkid']
    # gemvk = ['gemid', 'gemvkid']
    # wplgemvk = ['wplid', 'wplgemvkid']        
    
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk,
                'num': numvk,
                'opr': oprvk,
                'wpl': wplvk}
                # 'gem': gemvk}
 
    TEST_D = {'gemid': ['0457', '0363', '0003'],
              'wplid': ['3386', '1012', '3631'],
              'oprid': ['0457300000000259', '0457300000000260', '0003300000116985'],
              'numid': ['0003200000136934'],
              'vboid': ['0388010000212290'],
              'pndid': ['0003100000117987']}

    baglib.aprint(ll+40, '\n---------------DOELSTELLING--------------------------------')
    baglib.aprint(ll+40, 'Doel: controleer voor gegeven maand t of de in t-1 afgesloten voorkomens (vk)\n',
          'ongewijzigd in maand t aanwezig zijn.')
    baglib.aprint(ll+40, '-----------------------------------------------------------')


    # #############################################################################
    baglib.aprint(ll+40, '\n----Loop over de bag objecten (bob)-----------------------')
    # #############################################################################

    for bob, vk in KEY_DICT.items():
        
        baglib.aprint(loglevel+20, '\n---------- Loop: inlezen', bob, 'van', current_month, '---------')
        
        current = pd.read_csv(CURR_FILES_DICT[bob], dtype = BAG_TYPE_DICT)
        #                      dtype = {bob+'id': 'string',
        #                               bob+'vkid': np.short,
        #                               bob+'vkeg': np.uintc})
        # baglib.aprint(ll, '\tvelden van', bob, 'in', current_month, ':\n', current.info())

        baglib.aprint(loglevel+20, '\tInlezen', bob, 'van', prev_month, '...')

        previous = pd.read_csv(PREV_FILES_DICT[bob],  dtype = BAG_TYPE_DICT) 
        #                       dtype = {bob+'id': 'string',
        #                                bob+'vkid': np.short,
        #                                bob+'vkeg': np.uintc})
        # baglib.aprint(ll, '\tvelden van', bob, 'in', prev_month, ':\n', previous.info())
        
        baglib.aprint(ll+20, '\tVergelijken van', bob, 'van', current_month, 'met', bob, 'van', prev_month)
        missing = baglib.find_missing_vk(bob=bob, current=current, previous=previous,
                                         vk=vk, future_date=FUTURE_DATE, loglevel=ll, test_d=TEST_D)

        # baglib.aprint(ll+40, 'DEBUG:', missing.empty, missing.head())

        if missing.empty:
            baglib.aprint(ll+40, '==> Van bagobject', bob, 'zijn er geen afgesloten vk verloren gegaan!\n')
        else:
            baglib.aprint(ll+40, '==> Van bagobject', bob, 'zijn er WEL afgesloten vk verloren gegaan!')
            baglib.aprint(ll+40, '\tEEerste 5 vermiste vk', vk, 'in', current_month, ':\n', missing[vk].head(5), '\n')
            baglib.aprint(ll+40, '\tBewaren vermiste', bob, 'vk...')
            outputfile = K2DIR + 'vermiste_'+bob+'.csv'
            missing.sort_values(by=[bob+'id', bob+'vkid']).to_csv(outputfile, index=False)

# ########################################################################
# ########################################################################

# loglevel ll:
# ll = -10 # minstel loggin
# ll = 0  # bijna geen logging
# ll = 10 # hoofdkoppen
# ll = 20 # koppen
ll = 30 # tellingen
# ll = 40 # data voorbeelden 


if __name__ == '__main__':

    baglib.aprint(ll+40, '------------- Start bag_compare_vk lokaal ------------- \n')

    baglib.aprint(ll+40, '-------------------------------------------')
    baglib.aprint(ll+40, '-------------', LOCATION['OMGEVING'], '-----------')
    baglib.aprint(ll+40, '-------------------------------------------\n')


    DATADIR_IN = LOCATION['DATADIR_IN']
    DATADIR_OUT = LOCATION['DATADIR_OUT']
    DIR00 = DATADIR_IN + '00-zip/'
    DIR01 = DATADIR_OUT + '01-xml/'
    DIR02 = DATADIR_OUT + '02-csv/'
    DIR03 = DATADIR_OUT + '03-bewerktedata/'
    current_month = baglib.get_arg1(sys.argv, DIR02)
    
    bag_compare_vk(loglevel=ll,
                   current_month=current_month,
                   koppelvlak2=DIR02,
                   future_date=FUTURE_DATE)


