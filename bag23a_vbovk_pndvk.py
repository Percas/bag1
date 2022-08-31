#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 23 april 2022

Doel: selecteer bij elke VBO voorkomen (vbovk) een hoofdpand voorkomen (pndvk).
Het kadaster koppelt 1 of meer pnd aan een vbovk. Dit is om twee redenen niet
eenduidig:
    1. er kan meer dan 1 pnd aan een vbovk gekoppeld zijn (0,44%)
    2. bij het pnd kan het bouwjaar veranderen in een volgend vk

Voor elk pndvk bepalen we een prioriteit. Het pndvk met de hoogste prio wordt
het hoofdpnd vk. Hoe je de prioriteit berekent staat in de functie prio_pnd

0.2: Clean up code. Make one outputfile with all vbo vk:
    vboid, vbovkid, vbovkbg, vbovkeg, pndid, pndvkid, pndvkbg, pndvkeg, prio
    where that pnd vk is linked with pndvkbg < vbovkeg <= pndvkeg
    prio is only calculated if there is more than one pnd linked the the vbo
    vk. So if prio is empty (nan), it means that there is only one pnd vk
    linked to this vbo vk.
0.3: Koppel een hoofd-pndvk aan een vbovk: pndvk ipv een pnd dus!
0.3.2: Documentation and string formatting
0.3.3b: tel vbovk totaal en vbovk uniek (vku) in elke stap
0.4.1 koppel laagste pndvk aan vbovk_geen_pndvk
0.4.2 samenvatting toevoegen aan het eind met sleutelgetallen
0.5.1 aanpassing id naar vboid and pndid in koppelvlak 2 (waar het hoort ;-),
idem voor status naar vbostatus, vkid naar vbovkid etc.
0.6 functies verhuisd naar bag_functions.py
0.6.1 cleaning up
0.6.2 writes file bepaal_hoofdpand_kerngetallen to compare months
0.6.3 debugging error: in pndvk_prio staat in 202205 bij pnd 0003100000118116 
dubbele pndvkid met ook nog verschillende prio. Fixed
0.7 determine n_vbo_in_pndvk. For every pndvk determine the number of unique
vbo in this pndvk, where the vbo is in IN_VOORRAAD. The vbo is in IN_VOORRAAD
if at least one of its vbovk connected to pndvk is in IN_VOORRAAD
0.8 introducing indices and as small as possible datatypes
1.0 update met afstand tussen pnd en vbo. Verder standaard inlezen
1.1 fix inliggend
"""

# ################ import libraries ###############################
import pandas as pd
import numpy as np
import sys
import os
import time
import baglib
from baglib import BAG_TYPE_DICT

# ############### Define functions ################################

def bag_vbovk_pndvk(current_month='202208',
                    koppelvlak2='../data/02-csv',
                    koppelvlak3='../data/03-bewerkte-data',
                    loglevel=True):

    # ########################################################################
    print('------------- Start bag_vbovk_pndvk ------------- ')
    # ########################################################################
    # #############################################################################
    # print('00.............Initializing variables...............................')
    # #############################################################################
    # month and dirs
    INPUTDIR = koppelvlak2 + current_month + '/'
    K2DIR = INPUTDIR
    OUTPUTDIR = koppelvlak3 + current_month + '/'
    baglib.make_dir(OUTPUTDIR)
    
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
    
    # #####################################################
    # ----------- Legenda ----- ---------------------------
    # #####################################################
    print(f'{"Legenda":~^80}')
    print(f'~\t{"vbo:  verblijfsobject":<38}', f'{"pnd:  pand":<38}')
    print(f'~\t{"vk:   voorkomen":<38}', f'{"pndvk:  pand voorkomen":<38}')
    print(f'~\t{"vkbg: voorkomen begindatum geldigheid":<38}',
          f'{"vkeg: voorkomen einddatum geldigheid":<38}')
    print(f'~\t{"n...:  aantal records in df":<38}',
          f'{"bob: bagobject":<38}')
    print(f'{"~":~>80}')
    
    
    print('\n---------------DOELSTELLING--------------------------------')
    print('----Bepaal voor elk VBO voorkomen een hoofdpand voorkomen')
    print('----op basis van de vbo vkeg (voorkomen einddatum geldigheid)')
    print('-----------------------------------------------------------')
    
    # #############################################################################
    print('\n----1. Inlezen van vbo.csv en pnd.csv -----------------------\n')
    # #############################################################################
    
    tic = time.perf_counter()
    
    print('huidige maand (verslagmaand + 1):', current_month)
    
    bd = baglib.read_input_csv(INPUT_FILS_DICT, BAG_TYPE_DICT)
    
    # fix eendagsvliegen plus controle bob=bagobject: vbo of pnd
    for bob, vk in KEY_DICT.items():
        # controle
        # print('DEBUG:', bd[bob].info())
        # print('DEBUG: controle op', bob, 'met', vk )
        (nrec[bob], nkey[bob]) = baglib.df_comp(bd[bob], key_lst=vk)
        
        print('\tVerwijder eendagsvliegen bij', bob)
        bd[bob] = baglib.fix_eendagsvlieg(bd[bob], bob+'vkbg', bob+'vkeg')
        print('\tCheck na verwijderen', bob, 'met voorkomens', vk, ':')
        (nrec[bob], nkey[bob]) = baglib.df_comp(bd[bob], key_lst=vk, 
                                                nrec=nrec[bob], nkey=nkey[bob])
    
    doel_vbovk_u = nkey['vbo']
    n_vbovk = nrec['vbo']
    print('\tConcreter doel:', doel_vbovk_u, 'vbovk van een pndvk voorzien.')
    
    
    # #############################################################################
    print('\n----2. Voeg de informatie van de pndvk toe aan de vbovk----')
    # #############################################################################
    # print('\tDOEL reminder: aantal unieke vbovk:', doel_vbovk_u)
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
    print('\n----3. Selecteer nu het pndvk waarin de vkeg valt van het vbovk---\n')
    # #############################################################################
    
    msk = (bd['vbo']['pndvkbg'] <= bd['vbo']['vbovkeg']) & \
        (bd['vbo']['vbovkeg'] <= bd['vbo']['pndvkeg'])
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
          '\t\t Situatie 3a: meer dan 1 pndvk koppelt met de vbovk\n',
          '\t\t Situatie 3b: geen enkel pndvk koppelt met de vbovk\n',
          '\tWe zullen deze apart beschouwen:')
    print('\t\t3a, >1 is niet zo erg. Dat lossen we op met de prio functie\n',
          '\t\t3b  is een datafout. Bewaar deze vbovk in vbovk_geen_pndvk')
    
    # welke unieke vbovk houden we over:
    print('\n\t---3a: meer dan 1 pndvk koppelt met een vbovk')
    print('\tStartpunt (reminder) aantal records vbovk:', nrec['vbo'])
    print('\tvbovk na het koppelen met pndvk obv vbovkeg: ')
    if bd['vbo'].shape[0] == 0:
        sys.exit('Fout: in pnd.csv staat geen enkel pand dat koppelt met\
                 vbo.csv. Verder gaan heeft geen zin. Programma stopt...')
    
    print('\t\tDe reden dat 3a optreedt is technisch van aard::\n',
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
        '''
    
    else:
        print('\tSituatie 3b komt niet voor: aantal unieke vbovk is (DOEL):',
                doel_vbovk_u)
    
    # toc = time.perf_counter()
    # baglib.print_time(toc - tic, 'countin vbovk in', printit)
    
    
    # #############################################################################
    print('\n----4. Bepaal prio voor pndvk: welke is het best om te koppelen--')
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
    
    # print(bd['vbo'][['prio', 'prio2']].head(10))
    
    
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
    print('\n----5. Bewaren in koppelvlak3: vbovk -> hoofdpndvk met',
          doel2_vbovk_u, 'records...')
    # #############################################################################
    tic = time.perf_counter()
    
    
    outputfile = OUTPUTDIR + 'vbovk-pndvk.csv'
    
    # vbovk_hoofdpndvk_df = vbovk_prio_df[['vboid', 'vbovkid', 'pndid', 'pndvkid']]
    # vbovk_hoofdpndvk_df.sort_values(['vboid', 'vbovkid']).to_csv(outputfile,
    #                                                            index=False)
    
    bd['vbo'][['pndid', 'pndvkid']].sort_index().to_csv(outputfile, index=True)
    
    # toc = time.perf_counter()
    # print('\t\ttictoc - saving vbo-pnd file in', toc - tic, 'seconds')
    
    
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
    
    # #############################################################################
    print('\n----6. Bepalen n_vbo_in_pndvk, gerelateerd aan vbo.typeinliggend\n')
    # #############################################################################
    print('\tHet aantal vbo-s dat in een pand ligt, n_vbo_in_pndvk is een',
          '\teigenschap van pndvk. We tellen dan het aantal\n',
          '\tunieke vbo bij een pndvk, waarbij geldt dat zo een vbo pas meetelt,',
          '\tals 1 van zijn vk in IN_VOORRAAD zit.\n')
    print('\tStappen\n:\n',
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
          '\t\t\tMaakt in dit geval typeinliggend = False'
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
    outputfile = OUTPUTDIR + 'vbovk-nvbo.csv'
    bd['vbo'].to_csv(outputfile, index=True)
     
    toc = time.perf_counter()
    baglib.print_time(toc - tic, '\n------------- Einde bag_vbovk_pndvk in',
                      loglevel)

  
    
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

'''             
# ########################################################################
print('------------- Start bag_vbovk_pndvk lokaal ------------- \n')
# ########################################################################
'''

if __name__ == '__main__':

    os.chdir('..')
    BASEDIR = os.getcwd() + '/'
    baglib.print_omgeving(BASEDIR)
    DATADIR = BASEDIR + 'data/'
    DIR00 = DATADIR + '00-zip/'
    DIR01 = DATADIR + '01-xml/'
    DIR02 = DATADIR + '02-csv/'
    DIR03 = DATADIR + '03-bewerktedata/'
    current_month = baglib.get_arg1(sys.argv, DIR02)

    printit=True

    bag_vbovk_pndvk(current_month=current_month,
                    koppelvlak2=DIR02,
                    koppelvlak3=DIR03,
                    loglevel=printit)


'''

# month and dirs
os.chdir('..')
BASEDIR = os.getcwd() + '/'
baglib.print_omgeving(BASEDIR)
DATADIR = BASEDIR + 'data/'
DIR02 = DATADIR + '02-csv/'
DIR03 = DATADIR + '03-bewerktedata/'
current_month = baglib.get_arg1(sys.argv, DIR02)
INPUTDIR = DIR02 + current_month + '/'
K2DIR = INPUTDIR
OUTPUTDIR = DIR03 + current_month + '/'
baglib.make_dir(OUTPUTDIR)

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
result_dict = {}

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
printit = True

# #####################################################
# ----------- Legenda ----- ---------------------------
# #####################################################
print(f'{"Legenda":~^80}')
print(f'~\t{"vbo:  verblijfsobject":<38}', f'{"pnd:  pand":<38}')
print(f'~\t{"vk:   voorkomen":<38}', f'{"pndvk:  pand voorkomen":<38}')
print(f'~\t{"vkbg: voorkomen begindatum geldigheid":<38}',
      f'{"vkeg: voorkomen einddatum geldigheid":<38}')
print(f'~\t{"n...:  aantal records in df":<38}',
      f'{"bob: bagobject":<38}')
print(f'{"~":~>80}')


print('\n---------------DOELSTELLING--------------------------------')
print('----Bepaal voor elk VBO voorkomen een hoofdpand voorkomen')
print('----op basis van de vbo vkeg (voorkomen einddatum geldigheid)')
print('-----------------------------------------------------------')

# #############################################################################
print('\n----1. Inlezen van vbo.csv en pnd.csv -----------------------\n')
# #############################################################################

tic = time.perf_counter()

print('huidige maand (verslagmaand + 1):', current_month)

bd = baglib.read_input_csv(INPUT_FILS_DICT, BAG_TYPE_DICT)

# fix eendagsvliegen plus controle bob=bagobject: vbo of pnd
for bob, vk in KEY_DICT.items():
    # controle
    # print('DEBUG:', bd[bob].info())
    # print('DEBUG: controle op', bob, 'met', vk )
    (nrec[bob], nkey[bob]) = baglib.df_comp(bd[bob], key_lst=vk)
    
    print('\tVerwijder eendagsvliegen bij', bob)
    bd[bob] = baglib.fix_eendagsvlieg(bd[bob], bob+'vkbg', bob+'vkeg')
    print('\tCheck na verwijderen', bob, 'met voorkomens', vk, ':')
    (nrec[bob], nkey[bob]) = baglib.df_comp(bd[bob], key_lst=vk, 
                                            nrec=nrec[bob], nkey=nkey[bob])

doel_vbovk_u = nkey['vbo']
n_vbovk = nrec['vbo']
print('\tConcreter doel:', doel_vbovk_u, 'vbovk van een pndvk voorzien.')


# #############################################################################
print('\n----2. Voeg de informatie van de pndvk toe aan de vbovk----')
# #############################################################################
# print('\tDOEL reminder: aantal unieke vbovk:', doel_vbovk_u)
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

toc = time.perf_counter()
baglib.print_time(toc - tic, 'gebruikte tijd:', printit)


# #############################################################################
print('\n----3. Selecteer nu het pndvk waarin de vkeg valt van het vbovk---\n')
# #############################################################################

msk = (bd['vbo']['pndvkbg'] <= bd['vbo']['vbovkeg']) & \
    (bd['vbo']['vbovkeg'] <= bd['vbo']['pndvkeg'])
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
      '\t\t Situatie 3a: meer dan 1 pndvk koppelt met de vbovk\n',
      '\t\t Situatie 3b: geen enkel pndvk koppelt met de vbovk\n',
      '\tWe zullen deze apart beschouwen:')
print('\t\t3a, >1 is niet zo erg. Dat lossen we op met de prio functie\n',
      '\t\t3b  is een datafout. Bewaar deze vbovk in vbovk_geen_pndvk')

# welke unieke vbovk houden we over:
print('\n\t---3a: meer dan 1 pndvk koppelt met een vbovk')
print('\tStartpunt (reminder) aantal records vbovk:', nrec['vbo'])
print('\tvbovk na het koppelen met pndvk obv vbovkeg: ')
if bd['vbo'].shape[0] == 0:
    sys.exit('Fout: in pnd.csv staat geen enkel pand dat koppelt met\
             vbo.csv. Verder gaan heeft geen zin. Programma stopt...')

print('\t\tDe reden dat 3a optreedt is technisch van aard::\n',
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
'''
    vbovk_u = bd['vbo'][['vboid', 'vbovkid']].drop_duplicates()
    missing_vbovk_df = pd.concat([vbovk_pndvk_u,
                                  vbovk_u]).drop_duplicates(keep=False)
    vbovk_geen_pndvk_df = pd.merge(missing_vbovk_df, vbovk_df, how='left')
    n_vbovk_geen_pndvk = vbovk_geen_pndvk_df.shape[0]
'''
'''
else:
    print('\tSituatie 3b komt niet voor: aantal unieke vbovk is (DOEL):',
            doel_vbovk_u)

toc = time.perf_counter()
baglib.print_time(toc - tic, 'countin vbovk in', printit)


# #############################################################################
print('\n----4. Bepaal prio voor pndvk: welke is het best om te koppelen--')
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

# print(bd['vbo'][['prio', 'prio2']].head(10))


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

toc = time.perf_counter()
print('\t\ttictoc - prio, sort and drop duplicates', toc - tic, 'seconds')

print('\tBewaar de pnd prios in pndvk_prio.csv')
outputfile = OUTPUTDIR + 'pndvk_prio.csv'
bd['vbo'][['pndid', 'pndvkid', 'prio']]\
    .drop_duplicates().to_csv(outputfile, index=False)

toc = time.perf_counter()
print('\t\ttictoc - saving pnd-prio file in', toc - tic, 'seconds')


# #############################################################################
print('\n----5. Bewaren in koppelvlak3: vbovk -> hoofdpndvk met',
      doel2_vbovk_u, 'records...')
# #############################################################################
tic = time.perf_counter()


outputfile = OUTPUTDIR + 'vbovk-pndvk.csv'

# vbovk_hoofdpndvk_df = vbovk_prio_df[['vboid', 'vbovkid', 'pndid', 'pndvkid']]
# vbovk_hoofdpndvk_df.sort_values(['vboid', 'vbovkid']).to_csv(outputfile,
#                                                            index=False)

bd['vbo'][['pndid', 'pndvkid']].sort_index().to_csv(outputfile, index=True)

toc = time.perf_counter()
print('\t\ttictoc - saving vbo-pnd file in', toc - tic, 'seconds')

'''
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
print('\tHet aantal vbo-s dat in een pand ligt, n_vbo_in_pndvk is een',
      '\teigenschap van pndvk. We tellen dan het aantal\n',
      '\tunieke vbo bij een pndvk, waarbij geldt dat zo een vbo pas meetelt,',
      '\tals 1 van zijn vk in IN_VOORRAAD zit.\n')
print('\tStappen\n:\n',
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
      '\t\t\tMaakt in dit geval typeinliggend = False'
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

print('DEBUG2')
print(bd['vbo'].info())
print(bd['vbo'].head())


print('\n\t\t----- 7. bewaren in pndvk-nvbo.csv vbovk_nvbo.csv...')
outputfile = OUTPUTDIR + 'pndvk_nvbo.csv'
bd['pnd'].to_csv(outputfile, index=True)
outputfile = OUTPUTDIR + 'vbovk-nvbo.csv'
bd['vbo'].to_csv(outputfile, index=True)

'''
'''
# #############################################################################
print('\n----7. Samenvatting: bewaren van de belangrijkste kerngetallen------')
# #############################################################################
bepaal_hoofdpand_kerngetallen_file = DIR03 +\
    'bepaal_hoofdpand_kerngetallen.csv'
current_month = str(current_month)
try:
    result_df = pd.read_csv(bepaal_hoofdpand_kerngetallen_file)
except:
    result_df = pd.DataFrame(list(result_dict.items()),
                              columns=['Maand', current_month])
else:
    print('Aanmaken nieuw bestand bepaal_hoofdpand_kerngetallen...')
    current_df = pd.DataFrame(list(result_dict.items()),
                              columns=['Maand', current_month])
    result_df[current_month] = current_df[current_month]

outputfile = DIR03 + 'bepaal_hoofdpand_kerngetallen.csv'
result_df.to_csv(outputfile, index=False)
print(result_df)
'''