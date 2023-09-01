#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''

Onderzoek voorkomens overgangen (gebeurtenissen) van vbo:
    
vbovk        1            2            3            4
vbo1  o-----------oo-----------oo-----------oo---------->

Hoe: door twee opeenvolgende voorkomens aan elkaar te koppelen (1 - 2, 2 - 3 etc.) en de verschillen
tussen deze twee op te slaan als een gebeurtenis.

Stappen:

    * verrijk vbo_df met gemeente en pandstatus + bouwjaar (via de hoofdpna vk koppeling)
    * koppel twee vbovk_df met elkaar waarbij je per vbo de einddatum
    van het linker vbovk_df koppelt met de begindatum van het rechter.
    * noem daarbij de te vergelijken velden (zoals status, opp, pndid en numid)
     van het linker vbovk_df _oud en die van het rechter _nieuw.
    * cast deze velden naar str, zodat je ze in dezelfde kolom kunt opslaan.
    * voor elk van de te vergelijken velden als oud != nieuw, maak dan een vbo-gebeurtenis-
    record aan met velden: 
        vboid
        datum_gebeurtenis
        soort_gebeurtenis ['statuswijziging', ' opp-wijziging', ...]
        waarde_oud
        waarde_nieuw
    
    *. Dit overzicht van gebeurtenissen kan inzicht geven in het wat, het waar en
     wanneer van gebeurtenissen per VBO of ander BAG object. Hierbij kun je denken aan:

    - Wat is het percentage per type gebeurtenis? Ofwel: wat gebeurt er "het meest"
    - Hoeveel gebeurtenissen zijn er gemiddeld per soort per gemeente? Ofwel: gebeurt er in bepaalde gemeenten meer
      dan in andere?
    - Hoe zijn de gebeurtenissen verdeeld over de tijd, per type gebeurtenis, zowel absoluut als t.o.v. de levenscyclus van het vbo
    - Wat is de werkelijke betekenis van een bepaalde gebeurtenis, bijvoorbeeld als oppervlakte of pndid van een vbo wijzigt.
      we willen hierbij vooral in staat zijn om adnministratieve correcties te negeren


    * Resultaten
    - analyse leert dat in Winterswijk relatief de meeste wijzigingen in oppervlakte van vbo's zijn.
    Laten we eens een paar oppervlakte rijtjes gaan maken
'''

import logging
import os.path
# ################ import libraries ###############################
# import numpy as np
import sys
import time

import pandas as pd

import baglib
from config import OMGEVING, KOPPELVLAK0, KOPPELVLAK3a, KOPPELVLAK2, FILE_EXT, BAG_TYPE_DICT, LOGFILE
from k3_hoofdpnd import k3_hoofdpnd

# from k02_bag import k02_bag
# from k1_xml import k1_xmlgem, k1_xml


# ############### Define functions ################################


def k3_gebeurtenis(maand, logit) -> pd.DataFrame:
    '''maak het vbo-gebeurtenisbestand voor maand.'''

    pd.set_option('display.max_columns', 15)
    pd.set_option('display.width', 400)

    tic = time.perf_counter()
    logit.info(f'start k3_gebeurtenis voor maand {maand}')

    # check of de benodigde input er is
    if not os.path.exists(os.path.join(KOPPELVLAK3a, maand, 'vbo.' + FILE_EXT)):
        logit.info(f'vbo.{FILE_EXT} niet gevonden, probeer af te leiden')
        k3_hoofdpnd(maand, logit)

    # inlezen vd het hoofd df: vbo voorkomens
    cols = ['vboid', 'vbovkid', 'vbovkbg', 'vbovkeg', 'pndid', 'pndvkid', 'numid', 'numvkid', 'oppervlakte', 'vbostatus']
    vbo_df = baglib.read_input(input_file=os.path.join(KOPPELVLAK3a, maand, 'vbo'),
                              bag_type_d=BAG_TYPE_DICT,
                              output_file_type='pandas',
                              logit=logit)[cols]

    # verrijken van het vbo_df, eerst gemeenten (dit gaat in 4 stappen via num -> opr -> wpl -> gem)
    cols = ['numid', 'numvkid', 'oprid', 'oprvkid']
    num_df = baglib.read_input(input_file=os.path.join(KOPPELVLAK3a, maand, 'num'),
                               bag_type_d=BAG_TYPE_DICT,
                               output_file_type='pandas',
                               logit=logit)[cols]
    vbo_df = pd.merge(vbo_df, num_df).drop(['numid', 'numvkid'], axis=1)
    del num_df

    cols = ['oprid', 'oprvkid', 'wplid', 'wplvkid']
    opr_df = baglib.read_input(input_file=os.path.join(KOPPELVLAK3a, maand, 'opr'),
                               bag_type_d=BAG_TYPE_DICT,
                               output_file_type='pandas',
                               logit=logit)[cols]
    vbo_df = pd.merge(vbo_df, opr_df).drop(['oprid', 'oprvkid'], axis=1)
    del opr_df

    cols = ['wplid', 'wplvkid', 'gemid']
    wpl_df = baglib.read_input(input_file=os.path.join(KOPPELVLAK3a, maand, 'wpl'),
                               bag_type_d=BAG_TYPE_DICT,
                               output_file_type='pandas',
                               logit=logit)[cols]
    vbo_df = pd.merge(vbo_df, wpl_df).drop(['wplid', 'wplvkid'], axis=1)
    del wpl_df


    # verrijken van het vbo_df met het hoofdpand om bouwjaar en pand status te krijgen:
    cols = ['pndid', 'pndvkid', 'bouwjaar', 'pndstatus']
    pnd_df = baglib.read_input(input_file=os.path.join(KOPPELVLAK3a, maand, 'pnd'),
                               bag_type_d=BAG_TYPE_DICT,
                               output_file_type='pandas',
                               logit=logit)[cols]
    vbo_df = pd.merge(vbo_df, pnd_df).drop(['pndid', 'pndvkid'], axis=1)
    del pnd_df


    # converteer die kolommen die we willen vergelijken naar string, zodat we ze alle in twee kolommen
    # (oud en nieuw) kunnen opslaan samen met het soort gebeurtenis (dat de naam van de kolom krijgt met
    # daarachter _geb)
    cols_to_compare = ['vbostatus', 'oppervlakte', 'gemid', 'bouwjaar', 'pndstatus']
    vbo_df[cols_to_compare] = vbo_df[cols_to_compare].astype('string')
    compare_cols_oud = [s + '_oud' for s in cols_to_compare]
    compare_cols_nieuw = [s + '_nieuw' for s in cols_to_compare]

    # hernoem de kolommen van het linker dataframe naar oud en het rechter naar nieuw en koppel de twee (dezelfde)
    # dataframes vbo_df. Dit noemen we de "ruwe_gebeurtenissen"
    oud_vk_vbo_df = vbo_df.rename(columns=dict(zip(cols_to_compare, compare_cols_oud)))
    nieuw_vk_vbo_df = vbo_df.rename(columns=dict(zip(cols_to_compare, compare_cols_nieuw)))
    nieuw_vk_vbo_df['gemid'] = nieuw_vk_vbo_df['gemid_nieuw'] # om te kunnen aggregeren over gemeentecode

    ruwe_gebeurtenissen = pd.merge(oud_vk_vbo_df[['vboid', 'vbovkeg'] + compare_cols_oud],
                                   nieuw_vk_vbo_df[['vboid', 'vbovkbg', 'gemid'] + compare_cols_nieuw],
                                   left_on =['vboid', 'vbovkeg'],
                                   right_on=['vboid', 'vbovkbg']).drop_duplicates()


    # vul een nieuw geubeurtenis dataframe met de paarsgewijze verschillen voor elk van de mogelijke gebeurtenissen
    # die we willen vergelijken (in cols_to_compare)
    gebeurtenis = pd.DataFrame()
    for compare_col in cols_to_compare:
        col_oud = compare_col + '_oud'
        col_nieuw = compare_col + '_nieuw'
        verschil_per_type_gebeurtenis = ruwe_gebeurtenissen[ruwe_gebeurtenissen[col_oud] != ruwe_gebeurtenissen[col_nieuw]][['vboid', 'vbovkbg', 'gemid', col_oud, col_nieuw]]
        verschil_per_type_gebeurtenis['gebeurtenis'] = compare_col + '_geb'
        verschil_per_type_gebeurtenis = verschil_per_type_gebeurtenis.rename(columns={col_oud: 'oud', col_nieuw: 'nieuw', 'vbovkbg': 'datum'})
        gebeurtenis = pd.concat([gebeurtenis, verschil_per_type_gebeurtenis.copy()])
    gebeurtenis['gebeurtenis'] = gebeurtenis['gebeurtenis'].astype('category')
    geb_cols = [x + '_geb' for x in cols_to_compare]

    # Analyseer de gebeurtenissen, maak een draaitabel met aantal gebeurtenissen per gemeente per type gebeurtenis
    pivot = pd.pivot_table(data=gebeurtenis, values='nieuw', index='gemid', columns='gebeurtenis', aggfunc='count').reset_index()
    # print(pivot.sample(n=10))

    # het geeft meer inzicht als we het aantal gebeurtenissen bekijken t.o.v. het aantal vbo in een gemeente. Dit noemen
    # we de gemeente grootte (gem_grootte)
    # gem_grootte = ruwe_gebeurtenissen.groupby('gemid').size().reset_index(name='gem_grootte')
    gem_grootte = vbo_df[['vboid', 'gemid']].groupby('gemid').size().reset_index(name='gem_grootte')
    # print(gem_grootte.info())
    pivot = pd.merge(pivot, gem_grootte)
    for col in geb_cols:
        col_perc = col + '_p'
        pivot[col_perc] = round(100 * pivot[col] / pivot['gem_grootte'], 1)

    # gem_naam.csv van de CBS site downloaden en in mapje plaatsen.
    # we doen alles obv de gemeenten van 2023. Handmatige repair voor die gemeente met een komma in de naam
    gem_naam_file = os.path.join(KOPPELVLAK3a, maand, 'gem_naam.csv')
    gem_naam = pd.DataFrame()
    if os.path.exists(gem_naam_file):
        gem_naam = pd.read_csv(gem_naam_file, sep=',')
    pivot['gemid'] = 'GM' + pivot['gemid']

    if not gem_naam.empty:
        # print(gem_naam.info())
        # print(gem_naam)
        pivot = pd.merge(pivot, gem_naam[['GemeentecodeGM', 'Gemeentenaam']], left_on='gemid', right_on='GemeentecodeGM').drop(columns=['GemeentecodeGM'])

    # print(pivot.info())
    print(pivot.sort_values(by='oppervlakte_geb_p', ascending=False).head())


    # print(pivot.sort_values(by='oppervlakte_geb_p', ascending=False).head(40))
    baglib.save_df2file(df=pivot.sort_values(by='oppervlakte_geb_p', ascending=False),
                        outputfile=os.path.join(KOPPELVLAK3a, maand, 'vbo_gebeurtenis'), file_ext='csv',
                        includeindex=False, logit=logit)

    '''
    # maak rijtje oppervlakten per vbo:
    # haal opeenvolgende gelijke eruit
    opp_df = vbo_df[['vboid', 'oppervlakte']].sort_values(by='vboid').drop_duplicates(keep='first')
    # voeg een tellertje toe dat het aantal oppervlaktewijzigingen per vbo telt (aantal_wijzig)
    opp_df['aantal_wijzig'] = opp_df.groupby('vboid')['vboid'].transform('size')
    # vbo's waarvan de oppervlakte nooit wijzigt gooien we eruit
    opp_df = opp_df[opp_df['aantal_wijzig'] > 1]
    # maak het oppervlakte rijtje
    opp_df = opp_df.groupby(['vboid', 'aantal_wijzig'])['oppervlakte'].apply('-'.join).reset_index()
    # opp_df.sort_values(by=['gemid', 'aantal_wijzig'], ascending=False, inplace=True)
    print(opp_df.head())
    #bewaren
    # baglib.save_df2file(df=opp_df, outputfile=os.path.join(KOPPELVLAK3a, maand, 'vbo_opp_wijzigt'), file_ext='csv',
    #                                                        includeindex=False, logit=logit)

    
    # maak rijtjes per vbo:

    vbo_df.sort_values(by=['vboid', 'vbovkid'], inplace=True)
    # print(vbo_df.info())
    alle_rijtjes = vbo_df['vboid'].drop_duplicates()
    # print(alle_rijtjes.head())
    for col in cols_to_compare:
        logit.debug(f'comparing {col}')
        # haal opeenvolgende eruit als ze gelijk zijn
        rijtje_df = vbo_df[['vboid', col]].drop_duplicates(subset=['vboid', col], keep='first')
        # voeg een tellertje toe dat het aantal col-wijzigingen per vbo telt (aantal_col)
        rijtje_df['aantal_'+col] = rijtje_df.groupby('vboid')['vboid'].transform('size')
        # maak het rijtje
        rijtje_df = rijtje_df.groupby(['vboid', 'aantal_'+col])[col].apply('-'.join).reset_index()
        alle_rijtjes = pd.merge(alle_rijtjes, rijtje_df)
    print(alle_rijtjes.sample(n=30))
    '''

    vbo_df = baglib.read_input(input_file=os.path.join(KOPPELVLAK2, maand, 'vbo'), file_ext='parquet')
    print(vbo_df.info())
    alle_rijtjes = vbo_df['vboid'].drop_duplicates()
    # print(alle_rijtjes.head())
    for col in cols_to_compare:
        logit.debug(f'comparing {col}')
        # haal opeenvolgende eruit als ze gelijk zijn
        rijtje_df = vbo_df[['vboid', col]].drop_duplicates(subset=['vboid', col], keep='first')
        # voeg een tellertje toe dat het aantal col-wijzigingen per vbo telt (aantal_col)
        rijtje_df['aantal_' + col] = rijtje_df.groupby('vboid')['vboid'].transform('size')
        # maak het rijtje
        rijtje_df = rijtje_df.groupby(['vboid', 'aantal_' + col])[col].apply('-'.join).reset_index()
        alle_rijtjes = pd.merge(alle_rijtjes, rijtje_df)
    print(alle_rijtjes.sample(n=30))
# ########################################################################
# ########################################################################


if __name__ == '__main__':
   
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOGFILE),
            logging.StreamHandler()])    
    logit = logging.getLogger()
    logit.info('start k3_gebeurtenis vanuit main')
    logit.warning(OMGEVING)
    logit.setLevel(logging.DEBUG)
    
    maand_lst = baglib.get_args(sys.argv, KOPPELVLAK0)

    for maand in maand_lst:
        k3_gebeurtenis(maand, logit)


