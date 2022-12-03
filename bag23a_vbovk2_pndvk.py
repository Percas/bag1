#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 23 april 2022

Doel: selecteer bij elke VBO voorkomen (vbovk) een hoofdpand voorkomen (pndvk).
Het kadaster koppelt 1 of meer pnd aan een vbovk. Dit is om twee redenen niet
eenduidig:
    1. er kan meer dan 1 pnd aan een vbovk gekoppeld zijn (0,44%)
    2. bij het pnd kan iets, zoals het bouwjaar veranderen in een volgend vk

Input: vbovk en pndvk
Output: vbovk - hoofdpndvk

subdoel: we willen eigenschappen van het hoofdpand vk (zoals bouwjaar) op 
vbovk niveau gebruiken. Daarvoor is het nodig om een koppeling tussen vbovk
en pandvk te hebben. Een vbovk kan aan een pndvk worden gekoppeld als de 
looptijd van dit vbovk binnen de looptijd van al zijn pndvk valt. 

Dit hoeft niet zo te zijn. Daarom maken we eerst extra vbovk aan:
    maak een nieuw vbovk als tijdens de looptijd van een vbovk een 
    nieuw pndvk ontstaat van een van de pndn van dit vbo

    stappen bij subdoel1:
    1. maak de tabel pnd_vkbg: [pndid, pndvkbg]
    2. koppel vboid, vbovk, vbovkbg, pndid met [pndid, pndvkbg]
    3. laat vbovkbg en pndid weg in deze tabel er rename pndvkbg naar vbovkbg
    4. haal de dubbele eruit (als die er al zijn)
    5. concateer het resultaat met vboid, vbovkbg
    6. sorteer, haal dubbele eruit
    8. doe een ffill om de lege vbovk te vullen (propagate)
    9. maak een nieuwe oplopende vbovk: vbovk2

resultaat subdoel 1: vbo, vbovk, vbovk2, vbovkbg

        

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


# ############### Define functions ################################

def bag_vbovk_pndvk(current_month='testdata',
                    koppelvlak2='../data/02-csv',
                    koppelvlak3='../data/03-bewerkte-data',
                    loglevel=True):

    tic = time.perf_counter()
    
    print('-------------------------------------------')
    print('------------- Start bag_vbovk_pndvk -------')
    print('-------------------------------------------')

    # #############################################################################
    # print('00.............Initializing variables...............................')
    # #############################################################################
    # month and dirs
    INPUTDIR = koppelvlak2 + current_month + '/'
    K2DIR = INPUTDIR
    OUTPUTDIR = koppelvlak3 + current_month + '/'
    baglib.make_dir(OUTPUTDIR)
    
    if current_month == 'testdata':
        current_year = 2000
    else:
        current_month = int(current_month)
        current_year = int(current_month/100)
    
    # The tuning variables to determine pand prio
    IN_VOORRAAD = ['inge', 'inni', 'verb', 'buig']
    IN_VOORRAAD_P = 10000         # if pndstatus is in IN_VOORRAAD
    BOUWJAAR_P = 5000             # if YEAR_LOW < bouwjaar < current_year + 1
    BOUWJAAR_DIV = 1              # divide bouwjaar with 2000 (small around 1)
    YEAR_LOW = 1000
    # PND_DIV = 10 ** 16
    # vbovk_geen_pndvk_df = pd.DataFrame()
    # n_vbovk_geen_pndvk = 0
    # result_dict = {}
    
    pd.set_option('display.max_columns', 20)
    
    INPUT_FILS_DICT = {'vbo': K2DIR + 'vbo.csv',
                       'pnd': K2DIR + 'pnd.csv'}
    
    
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk}
    
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    nrec = {}       # aantal records
    nkey = {}       # aantal keyrecords
    
    # if printit = True bepaal-hoofdpnd prints extra info
    # printit = True
    
    FUTURE_DATE = 20321231
   
    '''    
    print('\n---------------DOELSTELLING--------------------------------')
    print('----1. Bepaal voor elk VBO voorkomen een hoofdpand voorkomen')
    print('----op basis van de vbo vkeg (voorkomen einddatum geldigheid)')
    print('----2. Bepaal voor elk vbovk de waarde voor "inliggend" ')
    print('-----------------------------------------------------------')
    '''

    # #############################################################################
    print('\n----1. Inlezen van vbo.csv en pnd.csv -----------------------\n')
    # #############################################################################
    
    # print('huidige maand (verslagmaand + 1):', current_month)
    
    bd = baglib.read_input_csv(INPUT_FILS_DICT, BAG_TYPE_DICT)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], key_lst=vbovk)
    (nrec['pnd'], nkey['pnd']) = baglib.df_comp(bd['pnd'], key_lst=pndvk)
    nkey1 = nkey['vbo']
    print('\t\tAantal records en sleutelrecords in vbo:', nrec['vbo'], nkey['vbo'])
    print('\t\tAantal records en sleutelrecords in pnd:', nrec['pnd'], nkey['pnd'])
    
    # #############################################################################
    print('\n----2. Aanmaken nieuwe vbo vk -----------------------\n')
    # #############################################################################

    print('\t\t2.1a Bepaal de begindatum van een vbo:')
    vbo_startdd = bd['vbo'].groupby('vboid')['vbovkbg'].min().to_frame().\
        rename(columns={'vbovkbg': 'startdd'}).reset_index()
    # print(vbo_startdd)

    print('\t\t2.1b Neem de unieke vbo-pnd uit vbo df...')
    vbo_df = bd['vbo'][['vboid', 'pndid']].drop_duplicates()
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbo_df, key_lst=['vboid'])
    print('\t\tAantal records en aantal unieke vbo:', nrec['vbo'], nkey['vbo'])
    
    print('\t\t2.1c Koppel deze')
    vbo_df = pd.merge(vbo_df, vbo_startdd)

    print('\n\t\t2.2 Voeg aan de vbo, pnd de pndvkbg (vk begindatum) toe van pndvk df...')
    print('\t\t\tEenheid id is hier vbo + pndvkbg!')
    # print(vbo_df.info())
    # print(bd['pnd'].info())
    vbo_df = pd.merge(vbo_df,
                      bd['pnd'][['pndid', 'pndvkbg']])[['vboid', 'pndvkbg', 'startdd']]
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbo_df, key_lst=['vboid', 'pndvkbg'],
                                                nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=True)
    
    # print(vbo_df)
    print('\n\t\t2.2b Verwijder de data waar het pnd eerder begint dan t vbo')
    vbo_df = vbo_df[vbo_df['pndvkbg'] >= vbo_df['startdd']][['vboid', 'pndvkbg']]
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbo_df, key_lst=['vboid', 'pndvkbg'],
                                                nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=True)
    # print(vbo_df)

    print('\n\t\t2.3 Hernoem de kolom pndvkbg naar vbovkbg...')
    vbo_df.rename({'pndvkbg': 'vbovkbg'}, axis='columns', inplace=True)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbo_df, key_lst=['vboid', 'vbovkbg'],
                                                nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)

    # concat so that all vbovkbg are in one frame
    print('\n\t\t2.4 Concateer de begindatum van de vbovk met de begindatum\n',
          '\t\t\tvan de pndvk...')
    vbo_df = pd.concat([bd['vbo'][['vboid', 'vbovkid', 'vbovkbg']], vbo_df])
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbo_df, key_lst=['vboid', 'vbovkbg'],
                                                nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=True)
    # print(vbo_df.head(30))
 
    print('\n\t\t2.5 verwijder dubbele vkbg bij een vboid, sorteer en zorg\n',
          '\t\t\tdat de lege (nieuwe) vbovk gevuld worden met het dichtst\n',
          '\t\t\tbijzijnde kleinere vk nummer tbv latere opvulling van dit\n',
          '\t\t\tnieuwe vk. Dit gaat met pandas.ffill')
    vbo_df = vbo_df.drop_duplicates(subset=['vboid', 'vbovkbg']).\
        sort_values(by=['vboid', 'vbovkbg']).\
            ffill().dropna().astype({'vbovkid': int})
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbo_df, key_lst=['vboid', 'vbovkbg'],
                                                nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    # print(vbo_df.head(30))

    print('\n\t\t2.6 Maak een nieuwe teller voor vbovk: vbovk2...')
    vbo_df = baglib.makecounter(vbo_df, grouper='vboid', newname='vbovkid2')
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbo_df, key_lst=['vboid', 'vbovkid2'],
                                                nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    # print(vbo_df.head(30))
  
    print('\n\t\t2.7 Voeg vbovkeg toe in twee stappen:')
    print('\t\t\t1.7.1 neem de vbovkbg van het volgende record')
    vbo_df['vbovkeg'] = vbo_df['vbovkbg'].shift(periods=-1)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbo_df, key_lst=['vboid', 'vbovkid2'],
                                                nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
  
    
    print('\t\t\t2.7.2 corrigeer de vbovkeg van het meest recente voorkomen')
    print('\t\t\tDit krijgt een datum in de verre toekomst...')
    idx = vbo_df.groupby(['vboid'])['vbovkid2'].transform(max) == vbo_df['vbovkid2']
    vbo_df.loc[idx, 'vbovkeg'] = FUTURE_DATE
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbo_df, key_lst=['vboid', 'vbovkid2'],
                                                nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
  

    print('\n\t\t2.8 Terugcasten naar integer van vbovkeg...')    
    vbo_df = vbo_df.astype({'vbovkeg': np.uintc})
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbo_df, key_lst=['vboid', 'vbovkid2'],
                                                nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    # print(vbo_df.head(30))
 

    print('\n\t\t ----- Perc toegenomen voorkomens:', 
          round(100 * (nkey['vbo']/nkey1 - 1), 1), '%')
    
    # print(bd['vbo'].head(30))
    # print('\n\n\t\t --------- aantal records in bd[vbo]: ', bd['vbo'].shape[0])
    # print(vbo_df.head(30))

    print('\n\t\t2.9 Imputeren nieuwe vbovk nog zonder pndid... ')
    cols = ['vboid', 'vbovkid', 'vbostatus', 'vbogmlx', 'vbogmly']
    vbo_df = pd.merge(vbo_df,
                      bd['vbo'][cols].drop_duplicates(),
                      how='inner',
                      left_on=vbovk, right_on=vbovk)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbo_df, key_lst=['vboid', 'vbovkid2'],
                                                nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    # print(bd['vbo'].head(30))
    
    print('\t\t2.10 Voeg pndid nog toe')
    print('\t\t Hierna is vboid, vbovk2 niet meer uniek vanwege dubbele pndn',
    'bij 1 vbo')
    bd['vbo'] = pd.merge(vbo_df,
                         bd['vbo'][['vboid', 'vbovkid', 'pndid']],
                         how='inner',
                         left_on=vbovk, right_on=vbovk)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], key_lst=['vboid', 'vbovkid2'],
                                                nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=True)
    # print(bd['vbo'].head(30))
    
    print('\n\t\t2.11 Schakel over identificatie van vbovk obv vbovkid2')
    vbovk = ['vboid', 'vbovkid2']
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk}


    # print(bd['vbo'].info())

    # #############################################################################
    print('\n----3. Verwijder eendagsvliegen -----------------------\n')
    # #############################################################################
    for bob, vk in KEY_DICT.items():
        # controle
        # print('DEBUG:', bd[bob].info())
        # print('DEBUG: controle op', bob, 'met', vk )
        # (nrec[bob], nkey[bob]) = baglib.df_comp(bd[bob], key_lst=vk)
        
        print('\tVerwijder eendagsvliegen bij', bob)
        bd[bob] = baglib.fix_eendagsvlieg(bd[bob], bob+'vkbg', bob+'vkeg')
        print('\tCheck na verwijderen', bob, 'met voorkomens', vk, ':')
        (nrec[bob], nkey[bob]) = baglib.df_comp(bd[bob], key_lst=vk, 
                                                nrec=nrec[bob], nkey=nkey[bob])
    
    # print(bd['vbo'].head(30))
    
    doel_vbovk_u = nkey['vbo']
    n_vbovk = nrec['vbo']
    print('\tConcreet doel:', doel_vbovk_u, 'vbovk van een pndvk voorzien.')
    
    
    # #############################################################################
    print('\n----4. Voeg de informatie van de pndvk toe aan de vbovk----')
    # #############################################################################
    print('\tDOEL reminder: aantal unieke vbovk:', doel_vbovk_u)
    print('\tStart met de', nrec['vbo'], '(niet unieke) vbovk. Elk vbovk heeft 1 of',
          '\n\tmeer pndid (dubbele bij de zelf aangemaakte vbovk).\n',
          '\tDus\n',
          '\t\tvbovk1, pndid1\n',
          '\t\tvbovk1, pndid2 (dit vbovk is dan zelf aangemaakt (en dubbel))\n',
          '\t\tvbovk2, pndid3\n',
          '\tKoppel nu aan pndid1, pndid2, pndid3 alle voorkomens van het\n',
          '\tbetreffende pnd. Het aantal records wordt hiermee ruim verdubbeld')

    bd['vbo'] = pd.merge(bd['vbo'],
                         bd['pnd'],
                         how='left',
                         on='pndid')
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], key_lst=vbovk,
                                                nrec=nrec['vbo'], nkey=nkey['vbo'], 
                                                u_may_change=False)
    bd['vbo'].set_index(vbovk, inplace=True)
    
    # zuinig met geheugen: Sommige types kunnen teruggecast worden.
    bd['vbo'] = baglib.recast_df_floats(bd['vbo'], BAG_TYPE_DICT)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    
    # toc = time.perf_counter()
    # baglib.print_time(toc - tic, 'gebruikte tijd:', printit)
    
  
    # #############################################################################
    print('\n----5. Selecteer pndvk waarin  het midden vh vbovk valt---\n')
    # #############################################################################
    
    bd['vbo']['midden'] = bd['vbo']['vbovkbg']/2 + bd['vbo']['vbovkeg']/2
    msk = (bd['vbo']['pndvkbg'] < bd['vbo']['midden']) & \
        (bd['vbo']['midden'] < bd['vbo']['pndvkeg'])
    bd['vbo'] = bd['vbo'][msk]
    
    # controle
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=True)
    
    doel2_vbovk_u = nkey['vbo']
    verschil_u =  doel_vbovk_u - doel2_vbovk_u
    
    print('\t\tAantal vbovk zonder pndvk:', verschil_u)
    
    
    print('\n\tVoor gegeven vbovk zou je gegeven een pnd, precies 1 pndvk moeten\n',
          '\tvinden op dag vbovkeg. Hiermee zou het aantal op', n_vbovk,
          '\n\tmoeten uitkomen, waarin', doel_vbovk_u,
          'unieke vbovk zitten. In de\n',
          '\tpraktijk vinden we echter twee andere situaties:\n',
          '\t\t Situatie 5a: meer dan 1 pndvk koppelt met de vbovk\n',
          '\t\t Situatie 5b: geen enkel pndvk koppelt met de vbovk\n',
          '\tWe zullen deze apart beschouwen:')
    print('\t\t3a, >1 is niet zo erg. Dat lossen we op met de prio functie\n',
          '\t\t3b  is een datafout. Bewaar deze vbovk in vbovk_geen_pndvk')
    
    # welke unieke vbovk houden we over:
    print('\n\t---5a: meer dan 1 pndvk koppelt met een vbovk')
    print('\tStartpunt (reminder) aantal records vbovk:', nrec['vbo'])
    print('\tvbovk na het koppelen met pndvk obv vbovkeg: ')
    if bd['vbo'].shape[0] == 0:
        sys.exit('Fout: in pnd.csv staat geen enkel pand dat koppelt met\
                 vbo.csv. Verder gaan heeft geen zin. Programma stopt...')
    
    print('\t\tDe reden dat 5a optreedt is technisch van aard::\n',
          '\t\tIn bepaald-hoofdpand zoeken we pndvk bij vbovk met formule\n',
          '\t\t\tpndvkbg <= vbovkeg <= pndvkeg\n',
          '\t\tOmdat hier twee keer <= staat hou je nog enkele dubbele\n',
          '\t\tvbovk. Als je echter links of rechts < zou doen, dan blijkt\n',
          '\t\tdat je koppelingen met pndvk gaat missen')
    
    
    if verschil_u != 0:  # er zijn vbovk met 0 pndvk
        print('\tbij deze vbovk is er dus op datum vkeg geen pndvk te vinden:',
              '\n\t\tAantal unieke vbovk in: ', doel_vbovk_u,
              # '\n\t\tAantal unieke vbovk uit:', result_dict['3_vbovk_pndvk_uniek'],
              '\n\t\tVerschil van de unieke: ', verschil_u),
        perc_doel = round(100 * (doel_vbovk_u - verschil_u) / doel_vbovk_u, 3)
        print('\tDoor deze', verschil_u, 'vbovk die geen pndvk hebben halen we\n',
              '\tons bovengenoemd DOEL voor', perc_doel, '%.')
        
        print('\tNieuwe doel:', doel2_vbovk_u)
    '''

        vbovk_u = bd['vbo'][['vboid', 'vbovkid']].drop_duplicates()
        missing_vbovk_df = pd.concat([vbovk_pndvk_u,
                                      vbovk_u]).drop_duplicates(keep=False)
        vbovk_geen_pndvk_df = pd.merge(missing_vbovk_df, vbovk_df, how='left')
        n_vbovk_geen_pndvk = vbovk_geen_pndvk_df.shape[0]
    else:
        print('\tSituatie 3b komt niet voor: aantal unieke vbovk is (DOEL):',
                doel_vbovk_u)
    
    # toc = time.perf_counter()
    # baglib.print_time(toc - tic, 'countin vbovk in', printit)
    
    '''
   
    # #############################################################################
    print('\n----6. Bepaal prio voor pndvk: welke is het best om te koppelen--')
    # #############################################################################
    
    print('\tWe voegen een kolom prio toe aan vbovk_pndvk...')
    
    bd['vbo'] = prio_pnd(bd['vbo'],
                         IN_VOORRAAD_P, IN_VOORRAAD,
                         BOUWJAAR_P, YEAR_LOW, current_year + 1,
                         BOUWJAAR_DIV) # , PND_DIV)
    #controle
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    
    bd['vbo']['prio2'] = bd['vbo']['prio']\
        - abs(bd['vbo']['vbogmlx'] - bd['vbo']['pndgmlx'])\
        - abs(bd['vbo']['vbogmly'] - bd['vbo']['pndgmly'])
    
    print(bd['vbo'][['prio', 'prio2']].head(10))
    

    print('\tSelecteer nu het pand met de hoogste prio. Alle pndvk krijgen een\n',
          '\tprio, maar de prio is alleen belangrijk bij de',
          'extra aangemaakte vbovk\n',
          '\tdie we in stap 3a geconstateerd hebben.')
    
    # how to remove non unique vbovk:
    # https://stackoverflow.com/questions/13035764/remove-pandas-rows-with-duplicate-indices/13036848#13036848
    bd['vbo'] = bd['vbo'].sort_values('prio', ascending=False)
    bd['vbo'] = bd['vbo'][~bd['vbo'].index.duplicated(keep='first')]
    
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    
    # toc = time.perf_counter()
    # print('\t\ttictoc - prio, sort and drop duplicates', toc - tic, 'seconds')
    
    print('\tBewaar de pnd prios in pndvk_prio.csv')
    outputfile = OUTPUTDIR + 'pndvk_prio.csv'
    bd['vbo'][['pndid', 'pndvkid', 'prio']]\
        .drop_duplicates().to_csv(outputfile, index=False)
    
    # toc = time.perf_counter()
    # print('\t\ttictoc - saving pnd-prio file in', toc - tic, 'seconds')
    
    
    # #############################################################################
    print('\n----7. Bewaren in koppelvlak3: vbovk -> hoofdpndvk met',
          doel2_vbovk_u, 'records...')
    # #############################################################################
    # tic = time.perf_counter()
    
    
    outputfile = OUTPUTDIR + 'vbovk_pndvk.csv'
    
    # vbovk_hoofdpndvk_df = vbovk_prio_df[['vboid', 'vbovkid', 'pndid', 'pndvkid']]
    # vbovk_hoofdpndvk_df.sort_values(['vboid', 'vbovkid']).to_csv(outputfile,
    #                                                            index=False)
    
    bd['vbo'][['vbovkid', 'pndid', 'pndvkid']].sort_index().to_csv(outputfile, index=True)
    
    toc = time.perf_counter()
    print('\t\ttictoc - saving vbo-pnd file in', toc - tic, 'seconds')
    
    '''
    if vbovk_geen_pndvk_df.shape[0] != 0:
        print('\tWe verrijken vbovk_geen_pndvk met "het laagste pndvk dat geen\n',
              '\teendagsvlieg is". Dat is meestal het vk dat het dichtst in de\n',
              '\tbuurt ligt van de vkeg van het vbovk...')
        vbovk_geen_pndvk_df = pd.merge(vbovk_geen_pndvk_df,
                                       pnd_laagstevk_df, how='left')
        if vbovk_geen_pndvk_df.shape[0] != n_vbovk_geen_pndvk:
            print('ERROR: aantal vk gewijzigd:', n_vbovk_geen_pndvk, '=>',
                  vbovk_geen_pndvk_df.shape[0])
        else:
            print('\tBewaren van vbovk_geen_pndvk.csv met', n_vbovk_geen_pndvk,
                  'records...')
            outputfile = OUTPUTDIR + 'vbovk_geen_pndvk.csv'
            vbovk_geen_pndvk_df.to_csv(outputfile)
    else:
        print('\tEr is geen bestand aangemaakt met vbovk zonder pndvk')
    '''
    '''
    
    # #############################################################################
    print('\n----6. Bepalen n_vbo_in_pndvk, gerelateerd aan vbo.typeinliggend\n')
    # #############################################################################
    print('\tHet aantal vbo-s dat in een pand ligt, n_vbo_in_pndvk is een\n',
          '\teigenschap van pndvk. We tellen dan het aantal\n',
          '\tunieke vbo bij een pndvk, waarbij geldt dat zo een vbo pas meetelt,\n',
          '\tals 1 van zijn vk in IN_VOORRAAD zit.\n')
    print('\tStappen:\n',
          '\t\t1. Bepaal voor elke vbovk of deze in IN_VOORRAAD zit\n',
          '\t\t2. Bepaal voor elke pndvk de vbovk die erin zitten en in\n',
          '\t\tIN_VOORRAAD zijn.Dit gebeurt in drie substappen:\n',
          '\t\t\t2a. zoek bij pndvk alle vbovk (eigenschap voorraad included)\n',
          '\t\t\t2b. verwijder records met voorraad == False\n',
          '\t\t\t2c. laat vkid weg en drop dubbele. tel nu het aantal keren dat\n',
          '\t\t\t\t voorraad==TRUE per pndvk.\n'
          '\t\t3. Dit aantal heet: "inliggend". inliggend is 0 of meer. \n',
          '\t\t4. Dit aantal is n_vbo_in_pndvk. Ken dit aantal ook toe aan de\n',
          '\t\t\tvbovk die bij het betreffende pndvk horen')
    print('\tDe situaties die voorkomen zijn:\n',
          '\t\tA. pndvk heeft geen vbo. Schuurtje: pndvk_zonder_vbo.csv\n',
          '\t\t\tMaakt in dit geval typeinliggend = False\n'
          '\t\tB. pndvk heeft 1 vbo: vbovk typeinliggend = False: woonhuis\n',
          '\t\tC. pndvk heeft >1 vbo: vbovk typeinliggend = True: flat')
    
    
    print('\n\t\t--- 6.1:  vbovk: leid de variabele voorraad af')
    
    bd['vbo']['voorraad'] = bd['vbo']['vbostatus'].isin(IN_VOORRAAD)
    nodige_cols = ['vboid', 'vbovkid', 'voorraad', 'pndid', 'pndvkid']
    bd['vbo'].reset_index(inplace=True)
    bd['vbo'] = bd['vbo'][nodige_cols]
    
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    
    vbovk_df2 = bd['vbo'][bd['vbo']['voorraad']==True]
    
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbovk_df2, nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=True)
    
    # print(vbovk_df2.head())
    
    print('\n\t\t--- 6.1b. verwijdere eventuele dubbele voorkomens')
    vbovk_df2 = baglib.ontdubbel_idx_maxcol(vbovk_df2, ['voorraad'])
    
    
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(vbovk_df2, nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=True)
    
    
    
    print('\n\t\t---6.2a pak alle pndvk. voeg vbovk toe via vbovk-hoofdpndvk ')
    # vbovk-hoofdpndvk zit in het dataframe bd['vbo']
    # dit combineren we dus met bd['pnd']. Deze laatste bepaalt de populatie
    
    bd['pnd'].set_index(pndvk, inplace=True)
    # print('DEBUG1:', bd['pnd'].info())
    nodige_cols = []
    bd['pnd'] = bd['pnd'][nodige_cols]
    # print('\nDEBUB2-')
    # print('DEBUG2:', bd['pnd'].info())
    # print('DEBUB2+\n')
    
    
    (nrec['pnd'], nkey['pnd']) = baglib.df_comp(bd['pnd'])
    bd['pnd'] = pd.merge(bd['pnd'], bd['vbo'], how='left',
                         left_on=pndvk, right_on=pndvk)
    # print('DEBUG3:', bd['pnd'].info())
    bd['pnd'].set_index(pndvk, inplace=True)
    # print('DEBUG4:', bd['pnd'].info())
    
    # print('DEBUG5: controle achteraf op aantallen:')
    
    (nrec['pnd'], nkey['pnd']) = baglib.df_comp(bd['pnd'],
                                                nrec=nrec['pnd'], nkey=nkey['pnd'],
                                                u_may_change=True)
    
    
    print('\n\t\t6.2b Verwijder pndvk vbovk die niet in voorraad zitten')
    bd['pnd'] = bd['pnd'][bd['pnd']['voorraad'] == True]
    (nrec['pnd'], nkey['pnd']) = baglib.df_comp(bd['pnd'],
                                                nrec=nrec['pnd'], nkey=nkey['pnd'],
                                                u_may_change=True)
    
    print('\n\t\t6.2c laat vkid weg en ontdubbel: we willen unieke vbo')
    bd['pnd'] = bd['pnd']['vboid'].drop_duplicates()
    (nrec['pnd'], nkey['pnd']) = baglib.df_comp(bd['pnd'],
                                                nrec=nrec['pnd'], nkey=nkey['pnd'],
                                                u_may_change=True)
    
    
    print('\t\t6.3 tel aantal vbo per pndvk')
    bd['pnd'] = bd['pnd'].groupby(pndvk).size().to_frame('inliggend')
    # print(bd['pnd'].info())
    # print(bd['pnd'].head())
    
    print('\t\tGemiddeld aantal vbo-s per pndvk:', bd['pnd'].mean())
    
    
    
    print('\n\t\t6.4 dit aantal toewijzen aan vbovk')
    print('\t\t\tkoppel hiervoor weer vbovk weer aan pndvk, nu vanuit vbovk')
    
    
    
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                key_lst=vbovk,
                                                u_may_change=False)
    
    # print('DEBUG1')
    # print(bd['vbo'].info())
    bd['vbo'] = pd.merge(bd['vbo'], bd['pnd'], how='left', left_on=pndvk,
                         right_on=pndvk)
    bd['vbo'].set_index(vbovk, inplace=True)
    bd['vbo'].fillna(0, inplace=True)
    bd['vbo'] = baglib.recast_df_floats(bd['vbo'], BAG_TYPE_DICT)
    
    bd['vbo'].drop(axis=1, columns=pndvk, inplace=True)
    
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    
    # print('DEBUG2')
    # print(bd['vbo'].info())
    # print(bd['vbo'].head())
    
    
    print('\n\t\t----- 7. bewaren in pndvk-nvbo.csv vbovk_nvbo.csv...')
    outputfile = OUTPUTDIR + 'pndvk_nvbo.csv'
    bd['pnd'].to_csv(outputfile, index=True)
    outputfile = OUTPUTDIR + 'vbovk_nvbo.csv'
    bd['vbo'].to_csv(outputfile, index=True)
     
    toc = time.perf_counter()
    baglib.print_time(toc - tic, '\n------------- Einde bag_vbovk_pndvk in',
                      loglevel)

    '''  
    
def prio_pnd(pnd1_df,
             in_voorraad_points, in_voorraad_statuslst,
             bouwjaar_points, bouwjaar_low, bouwjaar_high, bouwjaar_divider):
             # pndid_divider):
    """
    Bereken een prioriteit voor elk pndid in pnd1_df.

    In detail:
        1. Als bouwjaar vh pand logisch is:     +bouwjaar_points
           logisch betekent tussen de bouwjaar_low en bouwjaar_high
        2. Als het pand in de voorraad is:      +in_voorraad_points
           in de voorraad betekend status in in_voorraad_statuslist:
               {inge, inni, verb}
        3. We trekken het vkid er nog af om het switchen naar een
           nieuw voorkomen te ontmoedigen
        4. Oudere panden krijgen (ietsje) minder aftrekpunten:
            -bouwjaar / bouwjaar_divider
        5. Om het uniek te maken trekken we er een klein getal van af obv
        pnd_idPND_DIV:       -pndid/pndid_divider

    Returns
    -------
    Hetzelfde input dataframe pnd1_df met een extra kolom prio.

    """
    print('\tDe prio van een pnd is de som van onderstaande punten:')
    print('\t\tPunten als het pand in voorraad is:', in_voorraad_points)
    _in_voorraad_check = pnd1_df['pndstatus'].isin(in_voorraad_statuslst)
    pnd1_df['prio'] = np.where(_in_voorraad_check, in_voorraad_points, 0)

    print('\t\tPunten als het bouwjaar logisch is:', bouwjaar_points)
    _logisch_bouwjaar_check = pnd1_df['bouwjaar'].\
        between(bouwjaar_low, bouwjaar_high)
    pnd1_df['prio'] += np.where(_logisch_bouwjaar_check, bouwjaar_points, 0)
    
    print('\t\tMinpunten om switchen iets te ontmoedigen:', '-vkid')
    pnd1_df['prio'] -= pnd1_df['pndvkid']
 
    print('\t\tMinpunten voor het bouwjaar zelf (ca -1):',
            '-bouwjaar/' + str(bouwjaar_divider))
    pnd1_df['prio'] -= pnd1_df['bouwjaar'] / bouwjaar_divider

    # print('\t\tMinpunten voor pandid (tbv herhaalbaarheid):',
    #         '-pndid /' + str(pndid_divider))
    # pnd1_df['prio'] -= pnd1_df['pndid'].astype(int) / pndid_divider

    # print(active_pnd_df[['pndid', 'pndstatus', 'bouwjaar', 'prio']].head(30))
    # print('\tnumber of records in activ_pnd_df:', active_pnd_df.shape[0])
    # if pnd1_df.shape[0] != pnd1_df['pndid'].unique().shape[0]:
    #     print('Error of niet he;
    #           in functie prio_pnd: unieke panden versus actief')

    return pnd1_df
       
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

    bag_vbovk_pndvk(current_month=current_month,
                    koppelvlak2=DIR02,
                    koppelvlak3=DIR03,
                    loglevel=printit)


