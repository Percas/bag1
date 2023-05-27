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
        0. maak een loop over de laatste n extract maanden; 

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

    # de uiteindelijke peildatum is de eerste van de maand van het oudste extract    
    peildatum = int(extract_maand_lst[0] + '01')
    
    logit.info(f'start de loop over de extract maanden {extract_maand_lst}\n\
               peildatum voor alle extracten is {peildatum}')

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
        
        # #############################################################################
        logit.info('stap 1. Maak de koppeling vbo vk - gem vk')
        vbo_df = koppel_gemvk(file_d=INPUT_FILES_DICT, bag_type_d=BAG_TYPE_DICT, logit=logit)
        nrec, nvk = baglib.df_compare(df=vbo_df, vk_lst=vbovk, nvk_marge=1.2, logit=logit)
        
        # #############################################################################
        logit.info('stap 2. Leidt de voorraad af')
        vbo_df['voorraad'] = vbo_df['vbostatus'].isin(IN_VOORRAAD)
        nrec, nvk = baglib.df_compare(df=vbo_df, vk_lst=vbovk, nrec=nrec, nvk=nvk, nvk_marge=1.2, logit=logit)

        # #############################################################################
        logit.info(f'stap 3. afleiden stand; Peildatum is {peildatum}')
        vbo_df = baglib.select_vk_on_date(vbo_df, 'vbovkbg', 'vbovkeg', peildatum)
        nrec, nvk = baglib.df_compare(df=vbo_df, vk_lst=vbovk, nrec=nrec, nvk=nvk, nvk_marge=1.2, logit=logit)
        vbo_df['peildatum'] = peildatum
        vbo_df['extract'] = extract_maand
        
        # #############################################################################
        logit.info('stap 4. Aggregeer de vbo-s-in-voorraad naar gemeenten')
       
        voorraad_extractmaand = vbo_df[vbo_df['voorraad']].\
            groupby('gemid').size().to_frame(str(extract_maand)).reset_index()
    
        logit.debug('bewaar microdata om de vbo-s te achterhalen die verschil veroorzaken ')
        cols = ['vboid', 'vbovkbg', 'gemid', 'vbostatus', 'voorraad', 'peildatum', 'extract']
        microset_vbo_per_extract[extract_maand] = vbo_df[cols]


        # #############################################################################
        logit.info('stap 5. Vergelijk twee extractmaanden')
        if woning_voorraad.empty: # True als we de loop voor de eerste keer doorlopen
            logit.info('in de eerste loop vergelijken we nog niets')
            woning_voorraad = voorraad_extractmaand
            promiel_diff = voorraad_extractmaand
            vorige_maand = extract_maand # vanaf de volgende loop is er een vorige maand
        else: # er is een vorige loop, dus ook een vorige_maand met bijbehorende microset_vbo
            woning_voorraad = pd.merge(woning_voorraad, voorraad_extractmaand)
            logit.debug(f'{woning_voorraad.head()}')
            promiel_diff[extract_maand] = 1000 * (woning_voorraad[extract_maand] - woning_voorraad[vorige_maand]) / woning_voorraad[vorige_maand]
            # gem_lst_met_wijziging_in_voorraad = list(promiel_diff[promiel_diff[extract_maand]!=0]['gemid'])
            
            # maak een Series met gemid die een wijziging hebben in hun vbo voorraad:
            gem_met_wijziging_in_voorraad = promiel_diff[promiel_diff[extract_maand]!=0]['gemid']

            # logit.warning(f'in gemeenten {gem_lst_met_wijziging_in_voorraad} gebeurt het')
            # print(gem_met_wijziging_in_voorraad.info())
            # if gem_lst_met_wijziging_in_voorraad:
                
            # vind de vbo die de oorzaak zijn van de wijziging in voorraad in een gemeente
            if not gem_met_wijziging_in_voorraad.empty:
                microset_vbo_concat = pd.concat([microset_vbo_per_extract[vorige_maand], microset_vbo_per_extract[extract_maand]])
                changed_records = microset_vbo_concat[~microset_vbo_concat.duplicated(subset=['vboid', 'voorraad'], keep=False)]
                lst_changed_vboid = list(changed_records['vboid'].unique())
                logit.debug(f'in gemeenten {gem_met_wijziging_in_voorraad} wijzigt de voorraad')
                logit.debug(f'dit wordt veroorzaakt door deze vboid {lst_changed_vboid}')
                # logit.warning(f'\n{promiel_diff[promiel_diff[extract_maand]!=0]}')
                changed_records = changed_records.sort_values(by=['vboid', 'extract'])
                changed_records = pd.merge(changed_records, gem_met_wijziging_in_voorraad)
                # logit.warning(f'\n{changed_records}')
                if vbo_met_impact_op_voorraad.empty:
                    vbo_met_impact_op_voorraad = changed_records
                else:
                    vbo_met_impact_op_voorraad = pd.concat([vbo_met_impact_op_voorraad,
                                                           changed_records])
                # print(pd.merge(changed_records['vboid'].drop_duplicates(), microset_vbo_concat))
                vorige_maand = extract_maand # vanaf de volgende loop is er een vorige maand

        logit.info(f'einde loop voor maand {extract_maand}')
        
        
        
    # #############################################################################
    logit.info('stap na de loop: aggregatie en analyse bewaren in koppelvlak 4')
    
    outputfile = os.path.join(output_dir, 'voorraad.csv')
    woning_voorraad.to_csv(outputfile, index=False)

    outputfile = os.path.join(output_dir, 'promiel_diff.csv')
    promiel_diff.to_csv(outputfile, index=False)
    
    outputfile = os.path.join(output_dir, 'vbo_met_impact_op_voorraad.csv')
    vbo_met_impact_op_voorraad.to_csv(outputfile, index=False)
    
    toc = time.perf_counter()
    logit.info(f'*** Einde bag_vbl_extract in {(toc - tic)/60} min ***\n')


# def koppel_gemvk(vbo_df, num_df, opr_df, wpl_df):
def koppel_gemvk(file_d, bag_type_d, logit):
    '''Koppel aan elk vbovk een gemvk.
    Dit loopt via vbo - num - opr - wpl - gem.
    Voorwaarde is dat de namen van de kolommen kloppen'''
    
    logit.info('maak de koppeling vbo vk - gem vk')
    
    _numvk = ['numid', 'numvkid']
    _oprvk = ['oprid', 'oprvkid']
    _wplvk = ['wplid', 'wplvkid']
          

    _vbo_df = baglib.read_input(input_file=file_d['vbo'], bag_type_d=bag_type_d, logit=logit)
    _num_df = baglib.read_input(input_file=file_d['num'], bag_type_d=bag_type_d, logit=logit)
    _opr_df = baglib.read_input(input_file=file_d['opr'], bag_type_d=bag_type_d, logit=logit)
    _wpl_df = baglib.read_input(input_file=file_d['wpl'], bag_type_d=bag_type_d, logit=logit)

    # selecteer benodigde kolommen en verwijdere dubbele voorkomens
    logit.debug('verwijderen pndvk en daarna ontdubbelen van de vbovk')
    
    cols = ['pndid', 'pndvkid', 'oppervlakte', 'vbogmlx', 'vbogmly']
    _vbo_df = _vbo_df.drop(columns=cols).drop_duplicates()
    _vbo_df = pd.merge(_vbo_df, _num_df, how='left', on=_numvk)
   
    cols = ['numvkbg', 'numvkeg', 'numstatus', 'huisnr', 'postcode', 'typeao']
    _vbo_df.drop(columns=cols, inplace=True)
    _vbo_df = pd.merge(_vbo_df, _opr_df, how='inner', on=_oprvk)
 
    cols = ['numid', 'numvkid', 'oprvkbg', 'oprvkeg', 'oprstatus', 'oprnaam', 'oprtype']
    _vbo_df.drop(columns=cols, inplace=True)
    _vbo_df = pd.merge(_vbo_df, _wpl_df, how='inner', on=_wplvk)

    cols = ['oprid', 'oprvkid', 'wplid', 'wplvkid', 'wplvkbg', 'wplvkeg', 'wplstatus']
    _vbo_df.drop(columns=cols, inplace=True)
    
    return _vbo_df

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


