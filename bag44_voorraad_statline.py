#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 17 Dec 2022

Doel:
    Download tabel van Statline
    https://opendata.cbs.nl/statline/#/CBS/nl/dataset/81955NED/table?fromstatweb
    
   
"""

# ################ import libraries ###############################
import pandas as pd
import numpy as np
import sys
import os
import time
import cbsodata
import baglib
from config import OMGEVING, DIR02, DIR03, DIR04, BAG_TYPE_DICT, IN_VOORRAAD

# ############### Define functions ################################

def bag_download_statline(koppelvlak4='../data/04-aggr',
                          loglevel=20):

    tic = time.perf_counter()
    
    # #############################################################################
    baglib.printkop(ll+40, 'Start download tabel van StatLine')

    # #############################################################################
    K4DIR = os.path.join(koppelvlak4, current_month)
    baglib.make_dir(K4DIR)
    
    pd.set_option('display.max_columns', 20)

    cbs_81955NED_file = os.path.join(K4DIR, 'cbs_81955NED.csv')
    
    baglib.aprint(ll+30, '\n---------------DOELSTELLING--------------------------------')
    baglib.aprint(ll+40, '----Bepaal de vbo voorraad per gemeente per eerste vd maand')
    baglib.aprint(ll+30, '-----------------------------------------------------------')

 
   # #############################################################################
    baglib.printkop(ll, '1. Inlezen van de inputbestanden')

    if os.path.isfile(cbs_81955NED_file):
        baglib.aprint(ll+20, '\tFile cbs_81955NED.csv is aanwezig')
        cbs_81955NED = pd.read_csv(cbs_81955NED_file)
    else:
        baglib.aprint(ll+20, '\tProbeer 81955NED te downloaden van CBS')
        cbs_81955NED = pd.DataFrame(cbsodata.get_data('81955NED'))
        baglib.aprint(ll+20, '\tBewaren van 81955NED')
        cbs_81955NED.to_csv(cbs_81955NED_file, index=False)
        
    cbs_81955NED = cbs_81955NED.\
        astype(dtype={'Gebruiksfunctie': 'category',
                      'RegioS': 'category',
                      'Perioden': 'string'
                      # 'BeginstandVoorraad_1': np.uintc,
                      # 'Nieuwbouw_2': np.uintc,
                      # 'OverigeToevoeging_3': np.uintc,
                      # 'Sloop_4': np.uintc,
                      # 'OverigeOnttrekking_5': np.uintc,
                      # 'Correctie_6': np.uintc,
                      # 'SaldoVoorraad_7': np.uintc,
                      # 'EindstandVoorraad_8': np.uintc
                      }).\
        drop(columns=['Nieuwbouw_2', 
                      'OverigeToevoeging_3',
                      'Sloop_4',
                      'OverigeOnttrekking_5',
                      'Correctie_6',
                      'SaldoVoorraad_7'])

    baglib.aprint(ll, '\tHernoemen januari naar 01, februari naar 02, etc...')
    month_dict = dict(zip([' januari', ' februari', ' maart', ' april', ' mei', ' juni', ' juli', ' augustus', ' september', ' oktober', ' november', ' december'],
                          ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']))
    cbs_81955NED['Perioden'] = cbs_81955NED['Perioden'].replace(month_dict, regex=True)
    cbs_81955NED = cbs_81955NED[cbs_81955NED['Perioden'].str.len() == 6]

    baglib.aprint(ll, cbs_81955NED['Perioden'].head(50))
    

ll = 40
# ########################################################################
baglib.aprint(ll, 'Start bag_vbo_aggr lokaal')

if __name__ == '__main__':

    baglib.printkop(ll+40, OMGEVING)
    current_month = baglib.get_arg1(sys.argv, DIR03)
    
    bag_download_statline(koppelvlak4=DIR04, loglevel=ll)


