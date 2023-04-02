#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 17 Dec 2022

Doel:
    Bepaal de voorraad per gemeente op de eerste van de maand met 5 opeenvolgende extracten
    
    Stappen:
        0. maak een loop over de laatste 5 extract maanden; 
        verslagmaand, ..., verslagmaand - 4 

        1. Lees voor maand vbo, num, opr, wpl in in koppelvlak 3
            lees ook maar de wpl-naam uit koppelvlak 2
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
        
        6. sla op in een dataframe met kolommen extractmaand, rijen gemeentecode
        en cellen voorraad
        
        
    noot: kennelijk telt een wonging die ook andere functies heeft niet mee
    bij deze laatste, maar alleen als woning.

    Oud doel:
        Maak de tabel voorraad woningen niet woningen
        https://opendata.cbs.nl/statline/#/CBS/nl/dataset/81955NED/table?fromstatweb
        
        Beginnend met de voorraad uitgesplitst naar gemeente op de 1e van de maand.
        

    
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

def bag_vbo_aggr(current_month='testdata23',
                 koppelvlak2='../data/02-csv',
                 koppelvlak3='../data/03-bewerkte-data',
                 koppelvlak4='../data/04-aggr',
                 loglevel=20):

    tic = time.perf_counter()
    
    # #############################################################################
    baglib.printkop(ll, 'Start bag_vbo_aggr; ' + str(current_month))

    # #############################################################################
    # print('00.............Initializing variables...............................')
    # #############################################################################
    # month and dirs
    # INPUTDIR = koppelvlak3 + current_month + '/'
    K2DIR = os.path.join(koppelvlak2, current_month)
    K3DIR = os.path.join(koppelvlak3, current_month)
    K4DIR = os.path.join(koppelvlak4, current_month)
    baglib.make_dir(K4DIR)
    
    if (current_month == 'testdata02') or (current_month == 'testdata23'):
        current_year = 2000
    else:
        current_month = int(current_month)
        current_year = int(current_month/100)
    
    # vbo is in voorraad als status is 
    # v3, v4, v6, v8, zie status_dict in bag12_xml2csv.py
    # IN_VOORRAAD = ['v3', 'v4', 'v6', 'v8'] wordt al geimporteerd uit config.py
    pd.set_option('display.max_columns', 20)

    INPUT_FILES_DICT = {'vbo': os.path.join(K3DIR, 'vbo.csv'),
                        'num': os.path.join(K3DIR, 'num.csv'),
                        'opr': os.path.join(K3DIR, 'opr.csv'),
                        'wpl': os.path.join(K3DIR, 'wpl.csv'),
                        'wpl_naam': os.path.join(K2DIR, 'wpl_naam.csv')}

    cbs_81955NED_file = os.path.join(K4DIR, 'cbs_81955NED.csv')
    
    vbovk = ['vboid', 'vbovkid']
    numvk = ['numid', 'numvkid']
    oprvk = ['oprid', 'oprvkid']
    wplvk = ['wplid', 'wplvkid']
    # wplgemvk = ['wplid', 'wplgemvkid']        
    
    KEY_DICT = {'vbo': vbovk,
                'num': numvk,
                'opr': oprvk,
                'wpl': wplvk}
    
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    nrec = {}
    nkey = {}

    TEST_D = {'gemid': ['GM0014', 'GM0034', 'GM0096'],
              'wplid': ['3386', '1012', '3631'],
              'oprid': ['0457300000000259', '0457300000000260', '0003300000116985'],
              # 'numid': ['1979200000000546', '0457200000521759', '0457200000521256'],
              # 'numid': ['0388200000212289'],
              'numid': ['0003200000136934'],
              'vboid': ['1714010000784185'],
              # 'vboid': ['0007010000000192'],
              # 'pndid': ['0388100000202416', '0388100000231732', '0388100000232080', '0388100000232081']
              'pndid': ['0003100000117987']}


    
    baglib.aprint(ll+30, '\n---------------DOELSTELLING--------------------------------')
    baglib.aprint(ll+40, '----Bepaal de vbo voorraad per gemeente per eerste vd maand')
    baglib.aprint(ll+30, '-----------------------------------------------------------')

 
   # #############################################################################
    baglib.printkop(ll, '1. Inlezen van de inputbestanden')
    bd = baglib.read_input_csv(ll, INPUT_FILES_DICT, BAG_TYPE_DICT)
    for bob, vk in KEY_DICT.items():
        (nrec[bob], nkey[bob]) = baglib.df_comp(ll+20, bd[bob], key_lst=vk)
        bd[bob] = bd[bob].astype(dtype = {bob+'id': 'string'})
        # baglib.aprint(ll-10, bd[bob].info())


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
    

    '''    
    # #############################################################################
    baglib.printkop(ll, '2. maak de koppeling vbo vk - gem vk')
    
    # selecteer benodigde kolommen en verwijdere dubbele voorkomens
    baglib.aprint(ll, 'Verwijderen pndvk en daarna ontdubbelen van de vbovk')
    cols = ['vbovkid_org', 'pndid', 'pndvkid', 'oppervlakte', 'vbogmlx', 'vbogmly']
    bd['vbo'] = bd['vbo'].drop(columns=cols).drop_duplicates()
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], key_lst=vbovk, nrec=nrec['vbo'], nkey=nkey['vbo'], u_may_change=False)

    bd['vbo'] = pd.merge(bd['vbo'], bd['num'], how='left', on=numvk)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], key_lst=vbovk, nrec=nrec['vbo'], nkey=nkey['vbo'], u_may_change=False)
    baglib.aprint(ll-10, bd['vbo'].head())
    baglib.aprint(ll-10, bd['vbo'][bd['vbo']['postcode'].isna()].head())
    
    cols = ['numvkid_oud', 'numvkbg', 'numvkeg', 'numstatus', 'huisnr', 'postcode', 'typeao']
    bd['vbo'].drop(columns=cols, inplace=True)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], key_lst=vbovk, nrec=nrec['vbo'], nkey=nkey['vbo'], u_may_change=False)
    
    bd['vbo'] = pd.merge(bd['vbo'], bd['opr'], how='inner', on=oprvk)
    cols = ['numid', 'numvkid', 'oprvkid_oud', 'oprvkbg', 'oprvkeg', 'oprstatus', 'oprnaam', 'oprtype']
    bd['vbo'].drop(columns=cols, inplace=True)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], key_lst=vbovk, nrec=nrec['vbo'], nkey=nkey['vbo'], u_may_change=False)
    
    bd['vbo'] = pd.merge(bd['vbo'], bd['wpl'], how='inner', on=wplvk)
    cols = ['oprid', 'oprvkid', 'wplid', 'wplvkid', 'wplvkbg', 'wplvkeg', 'wplstatus']
    bd['vbo'].drop(columns=cols, inplace=True)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], key_lst=vbovk, nrec=nrec['vbo'], nkey=nkey['vbo'], u_may_change=False)

    # baglib.aprint(ll-10, bd['vbo'].info())
    # print(bd['wpl'].info())

    # #############################################################################
    baglib.printkop(ll, '3. Leidt de voorraad af')
    bd['vbo']['voorraad'] = bd['vbo']['vbostatus'].isin(IN_VOORRAAD)
    bd['vbo'] = bd['vbo'][bd['vbo']['voorraad']]
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], key_lst=vbovk, nrec=nrec['vbo'], nkey=nkey['vbo'], u_may_change=True)

    # #############################################################################
    peildatum = 20221201
    baglib.printkop(ll, '4. Peildatum is ' + str(peildatum))
    bd['vbo']['voorraad'] = bd['vbo']['vbostatus'].isin(IN_VOORRAAD)

    vbo_stand = baglib.select_vk_on_date(bd['vbo'], 'vbovkbg', 'vbovkeg', peildatum)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, vbo_stand, key_lst=vbovk, nrec=nrec['vbo'], nkey=nkey['vbo'], u_may_change=True)

    # #############################################################################
    baglib.printkop(ll, '5. Aggregeer naar gemeenten')
    voorraad = vbo_stand.groupby('gemid').size().to_frame('20221201').reset_index()


    # #############################################################################
    baglib.printkop(ll, '5. Fix om output te vergelijken met statline')
    
    statline_dict = {
         'A045364': 'woon', # 'woonfunctie'; de overige zijn allemaal niet woon
         'A045375': 'over', # 'overige gebruiksfunctie'
         'A045370': 'kant', # 'kantoorfunctie'
         'A045368': 'gezo', # 'gezondheidszorgfunctie'
         'A045366': 'bij1', # 'bijeenkomstfunctie'
         'A045372': 'ondr', # 'onderwijsfunctie'
         'A045374': 'wink', # 'winkelfunctie'
         'A045373': 'sprt', # 'sportfunctie'
         'A045371': 'logi', # 'logiesfunctie'
         'A045369': 'indu', # 'industriefunctie'
         'A045367': 'celf'  # 'celfunctie'
     }
    
    statline_totalen = {
         'T001419': 'tota', # Woning en niet-woning totaal
         'A045376': 'multi', # Niet-woning met meerdere functies
         'A045365': 'niwo', # Niet-woning totaal'
    }
    cols=['Gebruiksfunctie', 'sl20221201', 'RegioS']
    sl_df = sl_df[sl_df['Gebruiksfunctie']=='T001419'][cols]
    
    voorraad['gemid'] = 'GM' + voorraad['gemid']

    # baglib.aprint(ll, voorraad.head(20))
    sl_df = pd.merge(sl_df, voorraad, right_on='gemid', left_on='RegioS', how='inner', sort='RegioS')
    
    # baglib.debugprint(title='Voorraad Anton', df=voorraad, colname='gemid', vals=TEST_D['gemid'], loglevel=ll, sort_on=['gemid'])
    # baglib.debugprint(title='Voorraad SL', df=sl_df, colname='RegioS', vals=TEST_D['gemid'], loglevel=ll, sort_on=['RegioS'])
    # baglib.aprint(ll, sl_df.head())
    
    cols=['gemid', 'sl20221201', '20221201', 'diff']
    sl_df['diff'] = sl_df['sl20221201'] - sl_df['20221201']
    baglib.aprint(ll, sl_df[cols].head(20))
    
    
    # #############################################################################
    baglib.printkop(ll, '6. Bewaren in koppelvlak 4')
    
    outputfile = os.path.join(K4DIR, 'voorraad_verschillen.csv')
    sl_df[cols].to_csv(outputfile, index=False)

    
    
    
    toc = time.perf_counter()
    
    
    baglib.aprint(ll+40, '\n*** Einde bag_vbo_aggr in', (toc - tic)/60, 'min ***\n')

    '''

ll = 40
# ########################################################################
baglib.aprint(ll, 'Start bag_vbo_aggr lokaal')

if __name__ == '__main__':

    baglib.printkop(ll+40, OMGEVING)
    current_month = baglib.get_arg1(sys.argv, DIR03)
    
    bag_vbo_aggr(current_month=current_month,
                    koppelvlak2=DIR02,
                    koppelvlak3=DIR03,
                    koppelvlak4=DIR04,
                    loglevel=ll)


