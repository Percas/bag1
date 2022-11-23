#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 27 aug 2022

Doel Analyseer de verschillende fase overgangen bij vbo's'
"""

# ################ import libraries ###############################
import pandas as pd
# import numpy as np
import sys
# import os
import time
import baglib
from baglib import BAG_TYPE_DICT
from config import LOCATION


# ############### Define functions ################################

def bag_vbo_status(current_month='202208',
                   koppelvlak2='../data/02-csv',
                   koppelvlak3='../data/03-bewerkte-data',
                   loglevel=True):

    tic = time.perf_counter()
    
    print('-------------------------------------------')
    print('------------- Start bag_vbostatus -------')
    print('-------------------------------------------')

    # #############################################################################
    # print('00.............Initializing variables...............................')
    # #############################################################################
    # month and dirs
    k2dir = koppelvlak2 + current_month + '/'
    k3dir = koppelvlak3 + current_month + '/'
    baglib.make_dir(k3dir)
    
    # current_month = int(current_month)
    # current_year = int(current_month/100)
    
    pd.set_option('display.max_columns', 20)
    
    INPUT_FILS_DICT = {'vbo': k2dir + 'vbo.csv'}
    
    
    vbovk = ['vboid', 'vbovkid']
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    # nrec = {}       # aantal records
    # nkey = {}       # aantal keyrecords
    
    # if printit = True bepaal-hoofdpnd prints extra info
    # printit = True
    
    
    
    print('\n---------------DOELSTELLING--------------------------------')
    print('----1. Bepaal voor elke vbo zijn rijtje met statussen')
    print('-----------------------------------------------------------')
    
    # #############################################################################
    print('\n----1. Inlezen van de inputbestandensv -----------------------\n')
    # #############################################################################
    
    # print('huidige maand (verslagmaand + 1):', current_month)
    
    bd = baglib.read_input_csv(INPUT_FILS_DICT, BAG_TYPE_DICT)

    (nrec, nkey) = baglib.df_comp(bd['vbo'], key_lst=vbovk)
    cols = ['vboid', 'vbovkid', 'vbovkbg', 'vbostatus']
    cols2 = ['vboid', 'vbostatus']
    cols3 = ['vboid', 'vbovkid']
    bd['vbo'] = bd['vbo'][cols].sort_values(axis=0, by=cols3)
    print('\tVerwijderen opeenvolgende gelijke statussen bij vbo...')
    bd['vbo'] = bd['vbo'].loc[(bd['vbo'][cols2].shift() != bd['vbo'][cols2]).any(axis=1)] 
    
    # print(bd['vbo'].head(30))
    (nrec, nkey) = baglib.df_comp(bd['vbo'], key_lst=vbovk, nrec=nrec, nkey=nkey)
    stat_df = bd['vbo']
    
    
    
    print('\tMaak statusrij per id. duurt even ...')
    stat_df['vbostatusrij'] = stat_df.groupby('vboid')['vbostatus'].transform(lambda x: ''.join(x))
    # stat_df['vbostatusrij'] = stat_df.groupby('pndid')['pndstatus'].transform(lambda x: ''.join(x))
    # print(stat_df.head(30))
    # print(stat_df.info())
    
    print('\tTel aantal keren dat een vbo statusrij voorkomt')
    stat_df = stat_df.groupby('vbostatusrij').size().sort_values(ascending=False)
    # piep.columns = ['aantal']

    # print(piep.sort_values('aantal', axis=0, ascending=False).head(30))
    print(stat_df.head(30))
    
    # ggbyp = dedup.groupby('vbostatus').transform(lambda x: x/sum(x))
    # print(ggbyp.head(30))

    print('\tBewaren van', stat_df.shape[0], 'statusrijen met hun aantal...')
    outputfile = k3dir + 'vbo_statusrij.csv'
    stat_df.to_csv(outputfile, index=True)


    toc = time.perf_counter()
    baglib.print_time(toc - tic, 'countin vbovk in', printit)
  
    

# ########################################################################
print('------------- Start bag_vbovk_pndvk lokaal ------------- \n')
# ########################################################################

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

    bag_vbo_status(current_month=current_month,
                   koppelvlak2=DIR02,
                   koppelvlak3=DIR03,
                   loglevel=printit)

