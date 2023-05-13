#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 17 Dec 2022

Doel:
    Bepaal de voorraad per gemeente op de eerste van de maand t met n 
    opeenvolgende extracten uit t, t+1, ... t+n-1
    
    We beginnen 1 januari het oudste extract
    
    zie ook:
        https://opendata.cbs.nl/statline/#/CBS/nl/dataset/81955NED/table?fromstatweb
        
    
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
# import numpy as np
import sys
import os
import time
# import cbsodata
import baglib
import logging
from config import OMGEVING, KOPPELVLAK3a, KOPPELVLAK4, BAG_TYPE_DICT, IN_VOORRAAD, LOGFILE

# ############### Define functions ################################

def bag_vgl_extract(logit, maand='202305',
                    aantal_extract_maanden=4):

    tic = time.perf_counter()
    
    # #############################################################################
    logit.info(f'doel: vergelijk de woningvoorraad van {aantal_extract_maanden} opeenvolgende extracten, eindigend met het extract uit {maand}')

    extract_maand_lst = baglib.make_month_lst(maand, aantal_extract_maanden)
    logit.debug(f'te onderzoeken extract maanden {extract_maand_lst}')


    woning_voorraad = pd.DataFrame()
    microset_vbo_per_extract = {}
    vbo_met_impact_op_voorraad = pd.DataFrame()

    # TEST_D is om in te zoomen op een bepaald bagobject voor test en debugging doeleinden     
    # TEST_D = {'gemid': ['1903'],
    #           'vboid': ['1948010040638437']}
    
    # de uiteindelijke peildatum is de eerste van de maand van het oudste extract    
    peildatum = int(extract_maand_lst[0] + '01')
    
    logit.info(f'start de loop over de extract maanden {extract_maand_lst}\n\
               met peildatum {peildatum}')

    for extract_maand in extract_maand_lst:
    
        logit.info(f'bepaal de voorraad op {peildatum} met extract {extract_maand}')
    
        # #############################################################################
        # print('00.............Initializing variables...............................')
        # #############################################################################
        # i/o mappen
        input_dir = os.path.join(KOPPELVLAK3a, extract_maand)
        output_dir = os.path.join(KOPPELVLAK4, extract_maand)
        baglib.make_dirs(output_dir)


        # vbo is in voorraad als status is 
        # IN_VOORRAAD = ['v3', 'v4', 'v6', 'v8'] 
        # dit wordt geimporteerd uit config.py
    
        INPUT_FILES_DICT = {'vbo': os.path.join(input_dir, 'vbo'),
                            'num': os.path.join(input_dir, 'num'),
                            'opr': os.path.join(input_dir, 'opr'),
                            'wpl': os.path.join(input_dir, 'wpl')}
                            # 'wpl_naam': os.path.join(K2DIR, 'wpl_naam.csv')}
        
        vbovk = ['vboid', 'vbovkid']
        numvk = ['numid', 'numvkid']
        oprvk = ['oprid', 'oprvkid']
        wplvk = ['wplid', 'wplvkid']
        
        KEY_DICT = {'vbo': vbovk,
                    'num': numvk,
                    'opr': oprvk,
                    'wpl': wplvk}
        
        bd = {}         # dict with df with bagobject (vbo en pnd in this case)
        nrec = {}
        nkey = {}

     
       # #############################################################################
        logit.info('stap 1. Inlezen van de inputbestanden')
        bd = baglib.read_dict_of_df(file_d=INPUT_FILES_DICT, bag_type_d=BAG_TYPE_DICT, logit=logit)
        for bob, vk in KEY_DICT.items():
            (nrec[bob], nkey[bob]) = baglib.df_compare(df=bd[bob], vk_lst=vk, logit=logit)
    
        
        # #############################################################################
        logit.info('stap 2. Maak de koppeling vbo vk - gem vk')
        
        # selecteer benodigde kolommen en verwijdere dubbele voorkomens
        logit.debug('2a. Verwijderen pndvk en daarna ontdubbelen van de vbovk')
        
        cols = ['pndid', 'pndvkid', 'oppervlakte', 'vbogmlx', 'vbogmly']
        # cols = ['vboid', 'vbovkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'numid']
        # print(bd['vbo'].info())
        bd['vbo'] = bd['vbo'].drop(columns=cols).drop_duplicates()
        # bd['vbo'] = bd['vbo'][cols].drop_duplicates()
        (nrec['vbo'], nkey['vbo']) = baglib.df_compare(df=bd['vbo'], vk_lst=vbovk, nrec=nrec['vbo'], nvk=nkey['vbo'], nvk_marge=0, logit=logit)
    
        bd['vbo'] = pd.merge(bd['vbo'], bd['num'], how='left', on=numvk)
        (nrec['vbo'], nkey['vbo']) = baglib.df_compare(df=bd['vbo'], vk_lst=vbovk, nrec=nrec['vbo'], nvk=nkey['vbo'], nvk_marge=0, logit=logit)
        # print(bd['vbo'].head())
        # logit.debug(f, bd['vbo'][bd['vbo']['postcode'].isna()].head())

        cols = ['numvkbg', 'numvkeg', 'numstatus', 'huisnr', 'postcode', 'typeao']
        bd['vbo'].drop(columns=cols, inplace=True)
        (nrec['vbo'], nkey['vbo']) = baglib.df_compare(df=bd['vbo'], vk_lst=vbovk, nrec=nrec['vbo'], nvk=nkey['vbo'], nvk_marge=0, logit=logit)
        
        bd['vbo'] = pd.merge(bd['vbo'], bd['opr'], how='inner', on=oprvk)
        cols = ['numid', 'numvkid', 'oprvkbg', 'oprvkeg', 'oprstatus', 'oprnaam', 'oprtype']
        bd['vbo'].drop(columns=cols, inplace=True)
        (nrec['vbo'], nkey['vbo']) = baglib.df_compare(df=bd['vbo'], vk_lst=vbovk, nrec=nrec['vbo'], nvk=nkey['vbo'], nvk_marge=0, logit=logit)
        
        bd['vbo'] = pd.merge(bd['vbo'], bd['wpl'], how='inner', on=wplvk)
        cols = ['oprid', 'oprvkid', 'wplid', 'wplvkid', 'wplvkbg', 'wplvkeg', 'wplstatus']
        bd['vbo'].drop(columns=cols, inplace=True)
        (nrec['vbo'], nkey['vbo']) = baglib.df_compare(df=bd['vbo'], vk_lst=vbovk, nrec=nrec['vbo'], nvk=nkey['vbo'], nvk_marge=0, logit=logit)
    
    
        # #############################################################################
        logit.info('stap 3. Leidt de voorraad af')
        
        bd['vbo']['voorraad'] = bd['vbo']['vbostatus'].isin(IN_VOORRAAD)
        (nrec['vbo'], nkey['vbo']) = baglib.df_compare(df=bd['vbo'], vk_lst=vbovk, nrec=nrec['vbo'], nvk=nkey['vbo'], nvk_marge=1.2, logit=logit)

        
        # #############################################################################
        logit.info(f'stap 4. Afleiden stand; Peildatum is {peildatum}')
        # bd['vbo']['voorraad'] = bd['vbo']['vbostatus'].isin(IN_VOORRAAD)
    
        bd['vbo'] = baglib.select_vk_on_date(bd['vbo'], 'vbovkbg', 'vbovkeg', peildatum)
        (nrec['vbo'], nkey['vbo']) = baglib.df_compare(df=bd['vbo'], vk_lst=vbovk, nrec=nrec['vbo'], nvk=nkey['vbo'], nvk_marge=1.2, logit=logit)
        bd['vbo']['extract'] = extract_maand
        bd['vbo']['peildatum'] = peildatum
        

        # #############################################################################
        logit.info('stap 5. Aggregeer de vbo-s-in-voorraad naar gemeenten')
       
        voorraad_extractmaand = bd['vbo'][bd['vbo']['voorraad']].\
            groupby('gemid').size().to_frame(str(extract_maand)).reset_index()
    
        # logit.info('stap 5a. Zoek VBO bij die gemeenten met (grote) verschillen')
        cols = ['vboid', 'vbovkbg', 'gemid', 'vbostatus', 'voorraad', 'peildatum', 'extract']
        microset_vbo_per_extract[extract_maand] = bd['vbo'][cols]

        
        if woning_voorraad.empty: # True als we de loop voor de eerste keer doorlopen
            woning_voorraad = voorraad_extractmaand
            promiel_diff = voorraad_extractmaand
            vorige_maand = extract_maand # vanaf de volgende loop is er een vorige maand
        else: # er is een vorige loop, dus ook een vorige_maand met bijbehorende microset_vbo
            woning_voorraad = pd.merge(woning_voorraad, voorraad_extractmaand)
            promiel_diff[extract_maand] = 1000 * (woning_voorraad[extract_maand] - woning_voorraad[vorige_maand]) / woning_voorraad[vorige_maand]
            # gem_lst_met_wijziging_in_voorraad = list(promiel_diff[promiel_diff[extract_maand]!=0]['gemid'])
            gem_met_wijziging_in_voorraad = promiel_diff[promiel_diff[extract_maand]!=0]['gemid']
            # logit.warning(f'in gemeenten {gem_lst_met_wijziging_in_voorraad} gebeurt het')
            # print(gem_met_wijziging_in_voorraad.info())
            # if gem_lst_met_wijziging_in_voorraad:
            if not gem_met_wijziging_in_voorraad.empty:
                microset_vbo_concat = pd.concat([microset_vbo_per_extract[vorige_maand], microset_vbo_per_extract[extract_maand]])
                changed_records = microset_vbo_concat[~microset_vbo_concat.duplicated(subset=['vboid', 'voorraad'], keep=False)]
                lst_changed_vboid = list(changed_records['vboid'].unique())
                logit.debug(f'in gemeenten {gem_met_wijziging_in_voorraad} wijzigt de voorraad')
                logit.debug(f'dit wordt veroorzaakt door deze vboid {lst_changed_vboid}')
                logit.warning(f'\n{promiel_diff[promiel_diff[extract_maand]!=0]}')
                changed_records = changed_records.sort_values(by=['vboid', 'extract'])
                changed_records = pd.merge(changed_records, gem_met_wijziging_in_voorraad)
                logit.warning(f'\n{changed_records}')
                if vbo_met_impact_op_voorraad.empty:
                    vbo_met_impact_op_voorraad = changed_records
                else:
                    vbo_met_impact_op_voorraad = pd.concat([vbo_met_impact_op_voorraad,
                                                           changed_records])
                # print(pd.merge(changed_records['vboid'].drop_duplicates(), microset_vbo_concat))
                vorige_maand = extract_maand # vanaf de volgende loop is er een vorige maand
            
        logit.info(f'einde loop voor maand {extract_maand}')
        
        
        
    # #############################################################################
    logit.info('stap 6. Bewaren in koppelvlak 4')
    
    outputfile = os.path.join(output_dir, 'voorraad.csv')
    woning_voorraad.to_csv(outputfile, index=False)

    outputfile = os.path.join(output_dir, 'promiel_diff.csv')
    promiel_diff.to_csv(outputfile, index=False)
    
    outputfile = os.path.join(output_dir, 'vbo_met_impact_op_voorraad.csv')
    vbo_met_impact_op_voorraad.to_csv(outputfile, index=False)
    '''
    # #############################################################################
    logit.info('stap 7. Start micro analyse')
    
    def diff_df(df1, df2, subset):
        # Return tuple: (dfboth, df1not2, df2not1).
        # print('\tdiff_idx_df: in beide, in 1 niet 2, in 2 niet 1:')
        _df = pd.concat([df1, df2])
        _dfboth = _df[~_df.duplicated(subset=subset, keep='first')]
        _df = pd.concat([df1, _dfboth])
        _df1not2 = _df[~_df.duplicated(subset=subset, keep=False)]
        _df = pd.concat([df2, _dfboth])
        _df2not1 = _df[~_df.duplicated(subset=subset, keep=False)]
        return (_dfboth, _df1not2, _df2not1)
   
    
    logit.info(f'*** Voor peildatum {peildatum} ***')
    for prev_month, extract_maand in zip(extract_maand_lst, extract_maand_lst[1:]):
        # logit.debug(fmicroset_vbo_per_extract[extract_maand].head())
        
        # concateneer twee opeenvolgende extracten met vbo standgegevens op peildatum
        concat_df = pd.concat([microset_vbo_per_extract[prev_month], microset_vbo_per_extract[extract_maand]])

        # print(concat_df.sort_values(by=['vboid', 'vbovkbg']).head(30))
        
        # print(f'\nvan {prev_month} op {extract_maand}:')
        singles = concat_df[~concat_df.duplicated(subset=['vboid', 'vbovkbg', 'vbostatus'], keep=False)]
        # print('singles:', singles)
        # print()
        # print(concat_df[concat_df['vboid']==TEST_D['vboid'][0]])
        # check of vbo_status kan wijzigen, zonder dat het voorkomen afgesloten wordt
        # bepaal eerst
        
        vbostatus_df = concat_df.groupby(['vboid', 'vbovkbg', 'vbostatus']).to_frame()
        status_changed = vbostatus_df.groupby('vbo').size().to_frame('Aantal')
        status_changed = status_changed[status_changed['Aantal']>1]
        vbo_with_status_changed = pd.merge(status_changed, concat_df)

        logit.info(f'** Van extract {prev_month} naar extract {extract_maand}')
        logit.info(status_changed[status_changed['Aantal']>1])
        

        (both, df1not2, df2not1) = diff_df(microset_vbo_per_extract[extract_maand],
                                           microset_vbo_per_extract[prev_month],
                                           'vboid')


        logit.info(f, '\n** Van extract', prev_month, 'naar extract', extract_maand)


        if not df1not2.empty:
            logit.info(f, 'vbo+ wel in extract', prev_month, 'niet in', extract_maand)
            logit.info(f, df1not2)
        else:
            logit.info(f, 'vbo+ niet in extract_maand', prev_month, 'en wel in', extract_maand, ': geen')
        if not df2not1.empty:
            logit.info(f, 'vbo+ niet in extract', prev_month, 'wel in', extract_maand)
            logit.info(f, df2not1)
        else:
            logit.info(f, 'vbo+ wel in extract_maand', prev_month, 'en niet in', extract_maand, ': geen')
        '''
    
    toc = time.perf_counter()
    logit.info(f'*** Einde bag_vbo_aggr in {(toc - tic)/60} min ***\n')


# ########################################################################

if __name__ == '__main__':


    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOGFILE),
            logging.StreamHandler()])    
    logit = logging.getLogger()
    logit.info('start bag_vlg_extract vanuit main')
    logit.warning(OMGEVING)
    logit.setLevel(logging.INFO)
    
    arg_lst = baglib.get_args(sys.argv, ddir='')
    maand = arg_lst[0]
    aantal_extract_maanden = arg_lst[1]
    bag_vgl_extract(maand=maand,
                    aantal_extract_maanden=aantal_extract_maanden,
                    logit = logit)


