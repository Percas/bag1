#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 27 aug 2022

Doel Analyseer de verschillende fase overgangen bij vbo's'

Maak rijtjes van opeenvolgende overgangen van vbo en bijbehorende pnd.
bijvoorbeeld: p1,v1,p2,v3,v4
    
    
stappen: 
    1. maak voor vbo en pnd de statusrijtjes
    2. haal de dubbele eruit
    3. merge de twee en sorteer
    4. als de statusovergang van vbo en pnd op dezelfde dag, conateneer deze status
    5. maak statusrijtje van een vbo
    6. tel en sorteer de verschillende rijtjes aflopend


    status_dict = {
        'Verblijfsobject gevormd':                      'v1',
        'Niet gerealiseerd verblijfsobject':            'v2',
        'Verblijfsobject in gebruik (niet ingemeten)':  'v3',
        'Verblijfsobject in gebruik':                   'v4',
        'Verblijfsobject ingetrokken':                  'v5',
        'Verblijfsobject buiten gebruik':               'v6',
        'Verblijfsobject ten onrechte opgevoerd':       'v7',
        'Verbouwing verblijfsobject':                   'v8',
        'Bouwvergunning verleend':                      'p1',
        'Bouw gestart':                                 'p2',
        'Pand in gebruik (niet ingemeten)':             'p3',
        'Pand in gebruik':                              'p4',
        'Verbouwing pand':                              'p5',
        'Pand gesloopt':                                'p6',
        'Niet gerealiseerd pand':                       'p7',
        'Pand ten onrechte opgevoerd':                  'p8',
        'Pand buiten gebruik':                          'p9',
        'Sloopvergunning verleend':                     'p0'}



"""

# ################ import libraries ###############################
import pandas as pd
import numpy as np
import sys
# import os
import time
import baglib
# from baglib import BAG_TYPE_DICT
from config import LOCATION


# ############### Define functions ################################

def bag_vbo_status(current_month='testdata',
                   koppelvlak2='../data/02-csv',
                   koppelvlak3='../data/03-bewerkte-data',
                   loglevel=True):

    tic = time.perf_counter()
    
    print('-------------------------------------------')
    print('--- Start bag_vbostatus;', current_month, '-------')
    print('-------------------------------------------')

    # #############################################################################
    # print('00.............Initializing variables...............................')
    # #############################################################################
    k3dir = koppelvlak3 + current_month + '/'
    vbovk_pndvk_file = k3dir + 'vbovk_pndvk.csv'
    vbovk = ['vboid', 'vbovkid2']
    pd.set_option('display.max_columns', 20)
    
    print('\n---------------DOELSTELLING--------------------------------')
    print('---- Bepaal voor elke vbo zijn rijtje met statussen')
    print('-----------------------------------------------------------')
    
    print('\n-----------------------------------------------------------')
    print('----1. Inlezen van de inputbestand -----------------------\n')
    print('-----------------------------------------------------------')
    
    stat_df = pd.read_csv(vbovk_pndvk_file, 
                          dtype = {'vboid': 'string',
                                   'pndid': 'string',
                                   'vbovkid': np.short,
                                   'vbovkid2': np.short,
                                   'pndvkid': np.short,
                                   'vbovkbg': np.uintc, 
                                   'vbovkeg': np.uintc,
                                   'vbostatus': 'string',
                                   'pndstatus': 'string'})
    (nrec, nkey) = baglib.df_comp(stat_df, key_lst=vbovk)
    # print(stat_df.info())
    
    stat_df['status'] = stat_df['vbostatus'] + stat_df['pndstatus']
    cols = ['vboid', 'vbovkid', 'vbovkid2', 'vbovkbg', 'vbovkeg', 'status']
    stat_df = stat_df[cols]

    print('\n-----------------------------------------------------------')
    print('\t2a. Verwijderen opeenvolgende gelijke statussen...')
    print('-----------------------------------------------------------')
    cols = ['vboid', 'vbovkid', 'vbovkid2', 'vbovkbg', 'status']
    cols2 = ['vboid', 'status']
    cols3 = ['vboid', 'vbovkid2']
    stat_df = stat_df[cols].sort_values(axis=0, by=cols3)
    stat_df = stat_df.loc[(stat_df[cols2].shift() != stat_df[cols2]).any(axis=1)] 
    # print(stat_df.head(30))
    (nrec, nkey) = baglib.df_comp(stat_df, key_lst=vbovk, nrec=nrec, nkey=nkey, u_may_change=True)
   
    # print(stat_df.info())
    print('\n-----------------------------------------------------------')
    print('\t5. Maak statusrij per id. duurt even ...')
    print('-----------------------------------------------------------')
    stat_df['statusrij'] = stat_df.groupby('vboid')['status'].transform(lambda x: ','.join(x))
    # stat_df['statusrij'] = stat_df.groupby('pndid')['pndstatus'].transform(lambda x: ''.join(x))
    # print(stat_df.head(30))
    # print(stat_df.info())

    cols = ['vboid', 'statusrij']
    stat_df = stat_df[cols].drop_duplicates().sort_values('statusrij')
    (nrec, nkey) = baglib.df_comp(stat_df, key_lst=['vboid'], nrec=nrec, nkey=nkey, u_may_change=True)
   
    print(stat_df.head(30))
    
    print('\tBewaren van', stat_df.shape[0], 'vbo met statusrij...')
    outputfile = k3dir + 'vbo_metstatusrij.csv'
    stat_df.to_csv(outputfile, index=False)
    
    print('\n-----------------------------------------------------------')
    print('\t6. Tel aantal keren dat een vbo statusrij voorkomt')
    print('-----------------------------------------------------------')
    stat_df = stat_df.groupby('statusrij').size().sort_values(ascending=False)
    # piep.columns = ['aantal']

    # print(piep.sort_values('aantal', axis=0, ascending=False).head(30))
    print(stat_df.head(30))
    
    # ggbyp = dedup.groupby('vbostatus').transform(lambda x: x/sum(x))
    # print(ggbyp.head(30))

    print('\tBewaren van', stat_df.shape[0], 'statusrijen met hun aantal...')
    outputfile = k3dir + 'statusrij_aantal.csv'
    stat_df.to_csv(outputfile, index=True)


    toc = time.perf_counter()
    baglib.print_time(toc - tic, 'vbostatus duurde', printit)
    
    

# ########################################################################
print('------------- Start vbo status lokaal ------------- \n')
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

