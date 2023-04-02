#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 17 Dec 2022

Doel:
    Bepaal de voorraad per gemeente op de eerste van de maand t met 5 
    opeenvolgende extracten uit t, t+1, ... t+4
    
    We beginnen 1 januari in t=202211
    
    Stappen:
        0. maak een loop over de laatste 5 extract maanden; 

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
    
        4. selecteer de vbo vk die op de eerste van de maand geldig zijn (t)
        
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
# import cbsodata
import baglib
from config import OMGEVING, DIR02, DIR03, DIR04, BAG_TYPE_DICT, IN_VOORRAAD, FUTURE_DATE

# ############### Define functions ################################

def bag_vgl_extract(current_month='202303',
                    koppelvlak2='../data/02-csv',
                    koppelvlak3='../data/03-bewerkte-data',
                    koppelvlak4='../data/04-aggr',
                    loglevel=20):

    tic = time.perf_counter()
    
    # #############################################################################
    aantal_extract_maanden = 4
    baglib.printkop(ll, 'Start bag_vgl_extract; ' + str(current_month) + '; Aantal extract maanden: ' + str(aantal_extract_maanden))


    extract_month_lst = baglib.make_month_lst(current_month, aantal_extract_maanden)
    
    baglib.aprint(ll+30, '\n\Te onderzoeken extract maanden:')
    baglib.aprint(ll+30, '\t' + str(extract_month_lst))
    
    peildatum = int(extract_month_lst[-1] + '01')
    baglib.aprint(ll+40, '\tPeildatum:', peildatum)    


    
    woning_voorraad = pd.DataFrame()
    TEST_D = {'gemid': ['0668'],
              'vboid': ['0160010000062544']}
    microset_vbo_per_extract = {}


    baglib.aprint(ll, '\n\tStart de loop over de extract maanden')
    for extract_month in extract_month_lst:
    
        baglib.aprint(ll+10, '\n\n\tBezig met maand:', extract_month)
        baglib.printkop(ll+40, 'Bepaal de voorraad op ' + str(peildatum) + ' met extract ' + str(extract_month))
    
        # #############################################################################
        # print('00.............Initializing variables...............................')
        # #############################################################################
        # month and dirs
        # INPUTDIR = koppelvlak3 + extract_month + '/'
        K2DIR = os.path.join(koppelvlak2, extract_month)
        K3DIR = os.path.join(koppelvlak3, extract_month)
        K4DIR = os.path.join(koppelvlak4, extract_month)
        baglib.make_dir(K4DIR)


        # vbo is in voorraad als status is 
        # v3, v4, v6, v8, zie status_dict in bag12_xml2csv.py
        # IN_VOORRAAD = ['v3', 'v4', 'v6', 'v8'] wordt al geimporteerd uit config.py
        pd.set_option('display.max_columns', 20)
    
        INPUT_FILES_DICT = {'vbo': os.path.join(K3DIR, 'vbo.csv'),
                            'num': os.path.join(K3DIR, 'num.csv'),
                            'opr': os.path.join(K3DIR, 'opr.csv'),
                            'wpl': os.path.join(K3DIR, 'wpl.csv')}
                            # 'wpl_naam': os.path.join(K2DIR, 'wpl_naam.csv')}
        
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
    
  
     
       # #############################################################################
        baglib.printkop(ll+40, '1. Inlezen van de inputbestanden')
        bd = baglib.read_input_csv(ll, INPUT_FILES_DICT, BAG_TYPE_DICT)
        for bob, vk in KEY_DICT.items():
            (nrec[bob], nkey[bob]) = baglib.df_comp(ll+20, bd[bob], key_lst=vk)
            bd[bob] = bd[bob].astype(dtype = {bob+'id': 'string'})
            # baglib.aprint(ll-10, bd[bob].info())
    
        baglib.debugprint(title='vbo na 1. Inlezen',
                          df=bd['vbo'],
                          colname='vboid',
                          vals=TEST_D['vboid'],
                          loglevel=-10)
       
        
        # #############################################################################
        baglib.printkop(ll+40, '2. maak de koppeling vbo vk - gem vk')
        
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
        baglib.printkop(ll+40, '3. Leidt de voorraad af')
        
        bd['vbo']['voorraad'] = bd['vbo']['vbostatus'].isin(IN_VOORRAAD)
        bd['vbo'] = bd['vbo'][bd['vbo']['voorraad']]
        (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], key_lst=vbovk, nrec=nrec['vbo'], nkey=nkey['vbo'], u_may_change=True)

        baglib.debugprint(title='vbo na 3. Leidt voorraad af',
                          df=bd['vbo'],
                          colname='vboid',
                          vals=TEST_D['vboid'],
                          loglevel=-10)
        
        
        # #############################################################################
        baglib.printkop(ll+40, '4. Afleiden stand; Peildatum is ' + str(peildatum))
        bd['vbo']['voorraad'] = bd['vbo']['vbostatus'].isin(IN_VOORRAAD)
    
        vbo_stand = baglib.select_vk_on_date(bd['vbo'], 'vbovkbg', 'vbovkeg', peildatum)
        (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, vbo_stand, key_lst=vbovk, nrec=nrec['vbo'], nkey=nkey['vbo'], u_may_change=True)
    
        cols = ['vboid', 'vbovkbg', 'vbovkeg', 'gemid', 'vbostatus']
        # microset_vbo_per_extract[extract_month] = vbo_stand[vbo_stand['gemid'] == TEST_D['gemid'][0]][cols]
        # microset_vbo_per_extract[extract_month] = vbo_stand[vbo_stand['vbovkeg'] == FUTURE_DATE][cols]
        microset_vbo_per_extract[extract_month] = vbo_stand[cols]
        
        baglib.debugprint(title='vbo na 4. Aflleiden stand op peildatum',
                          df=vbo_stand,
                          colname='vboid',
                          vals=TEST_D['vboid'],
                          loglevel=-10)
        
        
        # #############################################################################
        baglib.printkop(ll+40, '5. Aggregeer naar gemeenten')
        
        voorraad_extractmaand = vbo_stand.groupby('gemid').size().to_frame(str(extract_month)).reset_index()
    
        if woning_voorraad.empty:
            woning_voorraad = voorraad_extractmaand
            promiel_diff = voorraad_extractmaand
            next_month = extract_month
        else:
            woning_voorraad = pd.merge(woning_voorraad, voorraad_extractmaand)
            promiel_diff[next_month] = 1000 * (woning_voorraad[next_month] - woning_voorraad[extract_month]) / woning_voorraad[extract_month]
            
            baglib.aprint(ll-10, promiel_diff)
            next_month = extract_month
            

    # #############################################################################
    baglib.printkop(ll+40, '6. Bewaren in koppelvlak 4')
    
    outputfile = os.path.join(K4DIR, 'voorraad.csv')
    woning_voorraad.to_csv(outputfile, index=False)

    outputfile = os.path.join(K4DIR, 'promiel_diff.csv')
    promiel_diff.to_csv(outputfile, index=False)
    

    # #############################################################################
    baglib.printkop(ll+40, '7. Start micro analyse')
    
    def diff_df(df1, df2, subset):
        '''Return tuple: (dfboth, df1not2, df2not1).'''
        # print('\tdiff_idx_df: in beide, in 1 niet 2, in 2 niet 1:')
        _df = pd.concat([df1, df2])
        _dfboth = _df[~_df.duplicated(subset=subset, keep='first')]
        _df = pd.concat([df1, _dfboth])
        _df1not2 = _df[~_df.duplicated(subset=subset, keep=False)]
        _df = pd.concat([df2, _dfboth])
        _df2not1 = _df[~_df.duplicated(subset=subset, keep=False)]
        return (_dfboth, _df1not2, _df2not1)
   
    
    baglib.aprint(ll+40, '*** Voor peildatum', peildatum, '***')
    for extract_month, prev_month in zip(extract_month_lst, extract_month_lst[1:]):
        # baglib.aprint(ll, microset_vbo_per_extract[extract_month].head())
        
        # concateneer twee opeenvolgende extracten met vbo standgegevens op peildatum
        concat_df = pd.concat([microset_vbo_per_extract[prev_month], microset_vbo_per_extract[extract_month]])

        # check of vbo_status kan wijzigen, zonder dat het voorkomen afgesloten wordt
        # bepaal eerst
        vbostatus_df = concat_df.groupby(['vboid', 'vbovkbg', 'vbostatus']).to_frame()
        status_changed = vbostatus_df.groupby('vbo').size().to_frame('Aantal')
        status_changed = status_changed_df[status_changed['Aantal']>1]
        vbo_with_status_changed = pd.merge(status_changed, concat_df)

        baglib.aprint(ll+40, '\n** Van extract', prev_month, 'naar extract', extract_month)
        baglib.aprint(ll+40, status_changed_df[status_changed_df['Aantal']>1])
        
        '''
        (both, df1not2, df2not1) = diff_df(microset_vbo_per_extract[extract_month],
                                           microset_vbo_per_extract[prev_month],
                                           'vboid')


        baglib.aprint(ll+40, '\n** Van extract', prev_month, 'naar extract', extract_month)


        if not df1not2.empty:
            baglib.aprint(ll+40, 'vbo+ wel in extract', prev_month, 'niet in', extract_month)
            baglib.aprint(ll+40, df1not2)
        else:
            baglib.aprint(ll+40, 'vbo+ niet in extract_maand', prev_month, 'en wel in', extract_month, ': geen')
        if not df2not1.empty:
            baglib.aprint(ll+40, 'vbo+ niet in extract', prev_month, 'wel in', extract_month)
            baglib.aprint(ll+40, df2not1)
        else:
            baglib.aprint(ll+40, 'vbo+ wel in extract_maand', prev_month, 'en niet in', extract_month, ': geen')
    
    toc = time.perf_counter()
    baglib.aprint(ll+40, '\n*** Einde bag_vbo_aggr in', (toc - tic)/60, 'min ***\n')


ll = 20
# ########################################################################
baglib.aprint(ll, 'Start bag_vbo_aggr lokaal')

if __name__ == '__main__':

    baglib.printkop(ll+40, OMGEVING)
    current_month = baglib.get_arg1(sys.argv, DIR03)
    
    bag_vgl_extract(current_month=current_month,
                    koppelvlak2=DIR02,
                    koppelvlak3=DIR03,
                    koppelvlak4=DIR04,
                    loglevel=ll)


