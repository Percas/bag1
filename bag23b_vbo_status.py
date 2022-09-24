#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 27 aug 2022

Doel: Analyseer de verschillende fase overgangen bij vbo's en panden'
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

def bag_vbo_status(current_month='202208',
                   koppelvlak2='../data/02-csv',
                   koppelvlak3='../data/03-bewerkte-data',
                   loglevel=True):

    tic = time.perf_counter()
    
    print('-------------------------------------------')
    print('------------- Start bag_vbo_status -------')
    print('-------------------------------------------')

    # #############################################################################
    # print('00.............Initializing variables...............................')
    # #############################################################################
    # month and dirs
    k2dir = koppelvlak2 + current_month + '/'
    k3dir = koppelvlak3 + current_month + '/'
    baglib.make_dir(k3dir)
    
    # current_month = int(current_month)
    # current_year = int(current_month/100)
    
    pd.set_option('display.max_columns', 20)
    
    INPUT_FILS_DICT = {'vbo': k2dir + 'vbo.csv',
                       'pnd': k2dir + 'pnd.csv',
                       'vbovk-pndvk': k3dir + 'vbovk_pndvk.csv'}
    
    
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk,
                'vbovk_pndvk': vbovk}
    
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    nrec = {}       # aantal records
    nkey = {}       # aantal keyrecords
    
    # if printit = True bepaal-hoofdpnd prints extra info
    # printit = True
    
    
    
    print('\n---------------DOELSTELLING--------------------------------')
    print('----1. Bepaal voor elke vbo zijn rijtje met statussen plus de')
    print('----begindatum van de overgang')
    print('----2. Voeg daar ook de pandovergang aan toe')
    print('-----------------------------------------------------------')
    
    # #############################################################################
    print('\n----1. Inlezen van de inputbestandensv -----------------------\n')
    # #############################################################################
    
    # print('huidige maand (verslagmaand + 1):', current_month)
    
    bd = baglib.read_input_csv(INPUT_FILS_DICT, BAG_TYPE_DICT)

    cols = ['vboid', 'vbovkid', 'vbovkbg', 'vbostatus']
    cols2 = ['vboid', 'vbostatus']
    cols3 = ['vboid', 'vbovkid']
    stats = bd['vbo'][cols].sort_values(axis=0, by=cols3)
    # stat_df = stats[cols].loc[(stats[cols2].shift() != stats[cols2]).any(axis=1)] 
    # if vboid, vbostatus occurs multiple times consecutively, only keep one instance
    vbo_df = stats.loc[(stats[cols2].shift() != stats[cols2]).any(axis=1)] 
    
    # print(dedup(1000)[dedup['vbostatus'] != 'inge'])
    
    # print('ontdubbelde statussen:')
    # print(stat_df.head(30))
    
    
    cols = ['pndid', 'pndvkid', 'pndvkbg', 'pndstatus']
    cols2 = ['pndid', 'pndstatus']
    cols3 = ['pndid', 'pndvkid']
    stats = bd['pnd'][cols].sort_values(axis=0, by=cols3)
    # if pndid, pndstatus occurs multiple times consecutively, only keep one instance
    pnd_df = stats.loc[(stats[cols2].shift() != stats[cols2]).any(axis=1)] 
    
    # print(dedup(1000)[dedup['vbostatus'] != 'inge'])
    
    print('ontdubbelde statussen:')
    print(pnd_df.head(30))
    
    stat_df = pnd_df
    
    
    print('\tMaak statusrij per id. duurt even ...')
    # stat_df['vbostatusrij'] = stat_df.groupby('vboid')['vbostatus'].transform(lambda x: ''.join(x))
    stat_df['vbostatusrij'] = stat_df.groupby('pndid')['pndstatus'].transform(lambda x: ''.join(x))
    # print(stat_df.head(30))
    # print(stat_df.info())
    
    print('\tTel aantal keren dat een vbo statusrij voorkomt')
    stat_df = stat_df.groupby('vbostatusrij').size().sort_values(ascending=False)
    # piep.columns = ['aantal']

    # print(piep.sort_values('aantal', axis=0, ascending=False).head(30))
    print(stat_df.head(30))
    
    # ggbyp = dedup.groupby('vbostatus').transform(lambda x: x/sum(x))
    # print(ggbyp.head(30))

    print('\tBewaren van', stat_df.shape[0], 'statusrijen met hun aantal...')
    outputfile = k3dir + 'vbo_statusrij.csv'
    stat_df.to_csv(outputfile, index=True)


    toc = time.perf_counter()
    baglib.print_time(toc - tic, 'countin vbovk in', printit)
    

    
    '''
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
    outputfile = OUTPUTDIR + 'vbovk-nvbo.csv'
    bd['vbo'].to_csv(outputfile, index=True)
     
    toc = time.perf_counter()
    baglib.print_time(toc - tic, '\n------------- Einde bag_vbovk_pndvk in',
                      loglevel)
    '''
  
    

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

    bag_vbo_status(current_month=current_month,
                   koppelvlak2=DIR02,
                   koppelvlak3=DIR03,
                   loglevel=printit)

