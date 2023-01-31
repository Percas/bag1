#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 27 aug 2022

Doel Analyseer de status overgangen bij vbo's en hun bijbehorende hoofpand'

stappen: 
    0. inlezen vbo.csv uit koppelvlak 3. Hier zit pnd vk en status al bij.
    1. verwijder opeenvolgende records bij gelijkblijvende statussen (vbo + pnd)
    2. maak de statusovergangen (shift -1 in pandas)
    3. tel ze en bereken min, max en gemiddelde doorlooptijd van de overgang
    4. bewaar ze in koppelvlak 4: statusovergang.csv
"""

# ################ import libraries ###############################
import pandas as pd
import numpy as np
import sys
import os
import time
import baglib
# from baglib import BAG_TYPE_DICT
from config import *

# ############### Define functions ################################

def bag_vbostatus(current_month='testdata',
                  koppelvlak3=os.path.join('..', 'data', '03-bewerkte-data'),
                  koppelvlak4=os.path.join('..', 'data', '04-aggr'),
                  loglevel=10):

    tic = time.perf_counter()
    ll = loglevel
    
    baglib.printkop(ll+40, 'Start bag_vbostatus' + current_month)

    # #############################################################################
    # baglib.aprint(ll+40, '00.............Initializing variables...............................')
    # #############################################################################
    k3dir = os.path.join(koppelvlak3, current_month)
    k4dir = os.path.join(koppelvlak4, current_month)
    baglib.make_dir(k4dir)


    vbovk_pndvk_file = os.path.join(k3dir, 'vbovk_hoofdpndvk.csv')
    vbovk = ['vboid', 'vbovkid']
    pd.set_option('display.max_columns', 20)
    ll = loglevel

    # oude status_dict
    '''
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
    '''
    status_inverse = dict(list(zip(status_dict.values(), status_dict.keys())))



    
    baglib.aprint(ll+40, '\n---------------DOELSTELLING--------------------------------')
    baglib.aprint(ll+40, '---- Analyseer de status overgangen bij vbos en hun bijbehorende hoofpand')
    baglib.aprint(ll+40, '-----------------------------------------------------------')
    
    baglib.aprint(ll+30, '\n-----------------------------------------------------------')
    baglib.aprint(ll+30, '----0. inlezen vbo.csv uit koppelvlak 3. Hier zit pnd vk en status al bij----')
    baglib.aprint(ll+30, '-----------------------------------------------------------')
    
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
    
    baglib.aprint(ll+10, '\n\t0a. we sorteren op vboid, vbovkid')
    cols = ['vboid', 'vbovkid',  'vbovkbg', 'vbovkeg', 'vbostatus', 'pndstatus']
    stat_df = stat_df[cols].sort_values(by=['vboid', 'vbovkid'])

    (nrec, nkey) = baglib.df_comp(ll+10, stat_df, key_lst=vbovk)

    # stat_df['vbostat'] = stat_df['vbostatus']
    baglib.aprint(ll+10, '\t0b. indikken: status in gebruik (niet ingemeten) => in gebruik')
    stat_df['vbostatus'] = stat_df['vbostatus'].replace(to_replace={'v3' : 'v4'})
    stat_df['pndstatus'] = stat_df['pndstatus'].replace(to_replace={'p4' : 'p5'})
    
    baglib.aprint(ll, stat_df.head(10))
    



    baglib.aprint(ll+30, '\n-----------------------------------------------------------')
    baglib.aprint(ll+30, '----1. verwijder opeenvolgende records bij gelijkblijvende statussen (vbo + pnd)...')
    baglib.aprint(ll+30, '-----------------------------------------------------------')

    # drop consecutive duplicates:
    # https://stackoverflow.com/questions/19463985/pandas-drop-consecutive-duplicates
    # The any() method returns one value for each column, True if ANY value in that column is True, otherwise False.
    cols = ['vboid', 'vbostatus', 'pndstatus']
    stat_df = stat_df.loc[(stat_df[cols].shift(-1) != stat_df[cols]).any(axis=1)] 
    
    (nrec, nkey) = baglib.df_comp(ll+10, stat_df, key_lst=vbovk, nrec=nrec, nkey=nkey, u_may_change=True)
    baglib.aprint(ll, stat_df.head(10))
    




    baglib.aprint(ll+30, '\n-----------------------------------------------------------')
    baglib.aprint(ll+30, '----2. maak de statusovergangen (shift -1 in pandas) ...')
    baglib.aprint(ll+30, '-----------------------------------------------------------')
    status_ovg = stat_df.copy()
    baglib.aprint(ll+20, '\n\t2a. met pd.shift(-1 )haal je een veld van het volgende record naar het huidige')
    status_ovg['vbostatus_nieuw'] = status_ovg['vbostatus'].shift(periods=-1)
    status_ovg['pndstatus_nieuw'] = status_ovg['pndstatus'].shift(periods=-1)
    status_ovg['vbovkeg'] = status_ovg['vbovkbg'].shift(periods=-1)

    baglib.aprint(ll, status_ovg.head(10))

    baglib.aprint(ll+30, '\n\t2b. verwijder het laatste vbovk in een groep van een vbo')
    _idx = status_ovg.groupby(['vboid'])['vbovkid'].transform(max) != status_ovg['vbovkid']
    status_ovg = status_ovg.loc[_idx].astype({'vbovkeg': int})
    cols = ['vboid', 'vbostatus', 'vbostatus_nieuw', 'pndstatus', 'pndstatus_nieuw', 'vbovkbg', 'vbovkeg']

    baglib.aprint(ll, status_ovg.head(10))

    '''
    baglib.anastatus(loglevel=ll, df=status_ovg, overgang=v1p3_v4p5)

    v1p3_v4p5 = {
        'vbostatus': 'v1',
        'vbostatus_nieuw': 'v4',
        'pndstatus': 'p3',
        'pndstatus_nieuw': 'p5'}
    '''
    
    baglib.aprint(ll, status_ovg.loc[(status_ovg['vbostatus'] == 'v1') &
                         (status_ovg['pndstatus'] == 'p3') &
                         (status_ovg['vbostatus_nieuw'] == 'v4') &
                         (status_ovg['pndstatus_nieuw'] == 'p5')].head())
    
    
    



    baglib.aprint(ll+30, '\n\t2c. bereken de doorlooptijd (dlt) van de faseovergang')
    status_ovg['dd_bg'] = pd.to_datetime(status_ovg['vbovkbg'].astype(str), format='%Y%m%d')
    status_ovg['dd_eg'] = pd.to_datetime(status_ovg['vbovkeg'].astype(str), format='%Y%m%d')
    status_ovg['dlt'] = status_ovg['dd_eg'] - status_ovg['dd_bg']

    status_ovg.drop(columns=['vbovkbg', 'vbovkeg', 'vbovkid', 'dd_bg', 'dd_eg'], inplace=True)
    baglib.aprint(ll, status_ovg.head(10))


    baglib.aprint(ll+30, '\n-----------------------------------------------------------')
    baglib.aprint(ll+30, '----3. tel ze en bereken min, max en gemiddelde doorlooptijd van de overgang')
    baglib.aprint(ll+30, '-----------------------------------------------------------')

    cols = ['vbostatus', 'vbostatus_nieuw', 'pndstatus', 'pndstatus_nieuw']
    status_ovg = status_ovg.groupby(cols).agg(aantal=('dlt', 'size'),
                                          laagste=('dlt', 'min'),
                                          hoogste=('dlt', 'max'),
                                          gem=('dlt', 'mean')).sort_values(by='aantal', ascending=False).reset_index()
    
    baglib.aprint(ll+30, '\n\t3b. kolommen netjes maken...')
    status_ovg['gem (mnd)'] = (status_ovg['gem'].round('d').astype(str).str.replace("days", "").astype(int)/30.4).round(2)
    status_ovg['laagste (mnd)'] = (status_ovg['laagste'].astype(str).str.replace("days", "").astype(int)/30.4).round(2)
    status_ovg['hoogste (mnd)'] = (status_ovg['hoogste'].astype(str).str.replace("days", "").astype(int)/30.4).round(2)
    status_ovg['perc'] = ((status_ovg['aantal'] / status_ovg['aantal'].sum()) * 100).round(2)
    status_ovg['cum_perc'] = ((status_ovg['aantal'].cumsum() / status_ovg['aantal'].sum()) * 100).round(2)
    status_ovg.replace({'vbostatus' : status_inverse,
                      'vbostatus_nieuw' : status_inverse,
                      'pndstatus' : status_inverse,
                      'pndstatus_nieuw' : status_inverse,
                      }, inplace=True)
    status_ovg.drop(columns=['gem', 'laagste', 'hoogste'], inplace=True)
    
    baglib.aprint(ll, status_ovg.head(10))
    # baglib.aprint(ll, status_ovg.info())

    baglib.aprint(ll+30, '\n-----------------------------------------------------------')
    baglib.aprint(ll+30, '----4. bewaar ze in koppelvlak 4: statusovergang.csv-----')
    baglib.aprint(ll+30, '-----------------------------------------------------------')

    outputfile = os.path.join(k4dir, 'statusovergang.csv')
    status_ovg.to_csv(outputfile, index=False)

    
    '''
    Het maken van statusrijtjes is uitgecommentarieerd for now
    
    # baglib.aprint(ll+40, stat_df.info())
    baglib.aprint(ll+40, '\n-----------------------------------------------------------')
    baglib.aprint(ll+40, '\t5. Maak statusrij per id. duurt even ...')
    baglib.aprint(ll+40, '-----------------------------------------------------------')
    stat_df['statusrij'] = stat_df.groupby('vboid')['status'].transform(lambda x: ','.join(x))
    # stat_df['statusrij'] = stat_df.groupby('pndid')['pndstatus'].transform(lambda x: ''.join(x))
    # baglib.aprint(ll+40, stat_df.head(30))
    # baglib.aprint(ll+40, stat_df.info())

    cols = ['vboid', 'statusrij']
    stat_df = stat_df[cols].drop_duplicates().sort_values('statusrij')
    (nrec, nkey) = baglib.df_comp(ll, stat_df, key_lst=['vboid'], nrec=nrec, nkey=nkey, u_may_change=True)
   
    # baglib.aprint(ll+40, stat_df.head(30))
    
    baglib.aprint(ll+40, '\tBewaren van', stat_df.shape[0], 'vbo met statusrij...')
    outputfile = os.path.join(k3dir, 'vbo_metstatusrij.csv')
    stat_df.to_csv(outputfile, index=False)
    
    baglib.aprint(ll+40, '\n-----------------------------------------------------------')
    baglib.aprint(ll+40, '\t6. Tel aantal keren dat een vbo statusrij voorkomt')
    baglib.aprint(ll+40, '-----------------------------------------------------------')
    stat_df = stat_df.groupby('statusrij').size().sort_values(ascending=False)
    # piep.columns = ['aantal']

    # baglib.aprint(ll+40, piep.sort_values('aantal', axis=0, ascending=False).head(30))
    baglib.aprint(ll+40, stat_df.head(30))
    
    # ggbyp = dedup.groupby('vbostatus').transform(lambda x: x/sum(x))
    # baglib.aprint(ll+40, ggbyp.head(30))

    baglib.aprint(ll+40, '\tBewaren van', stat_df.shape[0], 'statusrijen met hun aantal...')
    outputfile = os.path.join(k3dir, 'statusrij_aantal.csv')
    stat_df.to_csv(outputfile, index=True)
    '''  

    toc = time.perf_counter()
    baglib.aprint(ll+40, '\n-*** Einde vbo_status in', (toc - tic)/60, 'min ***\n')
    

# ########################################################################
# ########################################################################

if __name__ == '__main__':


    ll = 20
    baglib.printkop(ll+40, OMGEVING + 'Lokale aanroep')
    current_month = baglib.get_arg1(sys.argv, DIR02)


    print('------------- Start bag_vbostatus lokaal ------------- \n')
    bag_vbostatus(current_month=current_month,
                   koppelvlak3=DIR03,
                   koppelvlak4=DIR04,
                   loglevel=ll)

