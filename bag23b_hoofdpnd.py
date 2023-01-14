#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Woensdag 11 Jan 2023

We willen eigenschappen van een pand (zoals bouwjaar en pand status op vbovk 
niveau gebruiken. Daarvoor is het nodig om een koppeling tussen vbovk
en precies 1 hoofdpandvk te hebben. Dat gaat in ruim 99% van de gevallen 
direct goed, maar een half procent van de vbo's heeft twee of meer panden.

Doel: bepaal elke VBO voorkomen (vbovk) precies 1 hoofdpand voorkomen (pndvk).

Input: vbovk en pndvk
Output: vbovk - hoofdpndvk

Omdat de module fix_vk ervoor heeft gezorgd dat een
vbovk altijd binnen de looptijd van een pndvk valt, nemen we dit als uitgangspunt.

Stappen:
    0. Lees vbo.csv van koppelvlak 3 in (hierin zit de koppeling vbovk-pndvk) en
    lees pnd.csv in uit koppelvlak 2
    1. Koppel deze en bepaal met de prioriteits functie het hoofdpand.
        De functie prio_pnd berekent voor elk pnd een prioriteit op basis
        van bouwjaar (moet bestaan), pand status en afstand tot het vbo.
        

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
from config import FUTURE_DATE


# ############### Define functions ################################

def bag_hoofdpnd(current_month='testdata',
                 koppelvlak2='../data/02-csv',
                 koppelvlak3='../data/03-bewerkte-data',
                 loglevel=10,
                 future_date=FUTURE_DATE):

    tic = time.perf_counter()
    ll = loglevel
    
    baglib.aprint(ll+40, '-------------------------------------------')
    baglib.aprint(ll+40, '--- Start hoofdpnd;', current_month, ' -----')
    baglib.aprint(ll+40, '-------------------------------------------')

    # #############################################################################
    # print('00.............Initializing variables...............................')
    # #############################################################################
    # month and dirs

    K2DIR = koppelvlak2 + current_month + '/'
    K3DIR = koppelvlak3 + current_month + '/'
    
    if (current_month == 'testdata') or (current_month == 'backup_testdata'):
        current_year = 2000
    else:
        current_month = int(current_month)
        current_year = int(current_month/100)
    
    pd.set_option('display.max_columns', 20)
    
    INPUT_FILS_DICT = {'vbo': K3DIR + 'vbo.csv',
                       'pnd': K3DIR + 'pnd.csv'}
    
    
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk}
    
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    nrec = {}       # aantal records
    nkey = {}       # aantal keyrecords
    
    # The tuning variables to determine pand prio
    IN_VOORRAAD = ['inge', 'inni', 'verb', 'buig']
    IN_VOORRAAD_P = 10000         # if pndstatus is in IN_VOORRAAD
    BOUWJAAR_P = 5000             # if YEAR_LOW < bouwjaar < current_year + 1
    BOUWJAAR_DIV = 1              # divide bouwjaar with 2000 (small around 1)
    YEAR_LOW = 1000
   
    TEST_D = {'vboid': ['0388010000212290'],
              'pndid': ['0388100000202416', '0388100000231732', '0003100000117987']}

    baglib.aprint(ll+40, '\n---------------DOELSTELLING--------------------------------')
    baglib.aprint(ll+40, '---- Bepaal voor elk VBO voorkomen een hoofdpand voorkomen')
    baglib.aprint(ll+40, '-----------------------------------------------------------')

    # #############################################################################
    baglib.aprint(ll+40, '\n----0. Inlezen van vbo.csv uit K3 en pnd.csv uit K2------------\n')
    # #############################################################################
    
    # print('huidige maand (verslagmaand + 1):', current_month)
    
    bd = baglib.read_input_csv(ll, INPUT_FILS_DICT, BAG_TYPE_DICT)
    cols = ['pndid', 'pndvkid', 'pndvkbg', 'pndvkeg', 'bouwjaar', 'pndstatus', 'pndgmlx', 'pndgmly']
    bd['pnd'] = bd['pnd'][cols]
    cols =  ['vboid', 'vbovkid',  'pndid', 'pndvkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'vbogmlx', 'vbogmly']
    bd['vbo'] = bd['vbo'][cols]
    
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], key_lst=vbovk)
    (nrec['pnd'], nkey['pnd']) = baglib.df_comp(ll, bd['pnd'], key_lst=pndvk)
    # nkey1 = nkey['vbo']
    baglib.aprint(ll+20, '\t\tAantal records en', vbovk, 'in vbo:', nrec['vbo'], nkey['vbo'])
    baglib.aprint(ll+20, '\t\tAantal records en', pndvk, 'in pnd:', nrec['pnd'], nkey['pnd'])
    
    baglib.debugprint(loglevel=ll+10, title='ingelezen pnd na stap 0', df=bd['pnd'], colname='pndid', vals=TEST_D['pndid'])
    
    # #############################################################################
    print('\n----1. Verwijder eendagsvliegen -----------------------\n')
    # #############################################################################
    for bob, vk in KEY_DICT.items():
        # controle
        # print('DEBUG:', bd[bob].info())
        # print('DEBUG: controle op', bob, 'met', vk )
        # (nrec[bob], nkey[bob]) = baglib.df_comp(bd[bob], key_lst=vk)
        
        print('\tVerwijder eendagsvliegen bij', bob)
        bd[bob] = baglib.fix_eendagsvlieg(bd[bob], bob+'vkbg', bob+'vkeg')
        print('\tCheck na verwijderen', bob, 'met voorkomens', vk, ':')
        (nrec[bob], nkey[bob]) = baglib.df_comp(ll, bd[bob], key_lst=vk, 
                                                nrec=nrec[bob], nkey=nkey[bob])
    
    doel_vbovk_u = nkey['vbo']
    baglib.debugprint(loglevel=ll+10, title='pnd eendagsvliegen verwijderd na stap 1', df=bd['pnd'], colname='pndid', vals=TEST_D['pndid'])
    
    # #############################################################################
    print('\n----2. Voeg de informatie van de pndvk toe aan de vbovk----')
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
                         on=pndvk)
    del bd['pnd']
    
    print(bd['vbo'].info())
    # (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], key_lst=vbovk,
    #                                             nrec=nrec['vbo'], nkey=nkey['vbo'], 
    #                                             u_may_change=False)
    
    
    bd['vbo'].set_index(vbovk, inplace=True)
    
    # zuinig met geheugen: Sommige types kunnen teruggecast worden.
    bd['vbo'] = baglib.recast_df_floats(bd['vbo'], BAG_TYPE_DICT)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    
    # toc = time.perf_counter()
    # baglib.print_time(toc - tic, 'gebruikte tijd:', printit)

    doel2_vbovk_u = nkey['vbo']

   
    # #############################################################################
    print('\n----3. Bepaal prio voor pndvk: welke is het best om te koppelen--')
    # #############################################################################
    
    print('\tWe voegen een kolom prio toe')
    
    bd['vbo'] = prio_pnd(bd['vbo'],
                         IN_VOORRAAD_P, IN_VOORRAAD,
                         BOUWJAAR_P, YEAR_LOW, current_year + 1,
                         BOUWJAAR_DIV) # , PND_DIV)
    #controle
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                # key_lst=vbovk,
                                                u_may_change=False)

    print('\tSelecteer nu het pand met de hoogste prio. Alle pndvk krijgen een\n',
          '\tprio, maar de prio is alleen belangrijk als er meer dan 1 pnd koppelt')
    
    # how to remove non unique vbovk:
    # https://stackoverflow.com/questions/13035764/remove-pandas-rows-with-duplicate-indices/13036848#13036848
    bd['vbo'] = bd['vbo'].sort_values('prio', ascending=False)
    bd['vbo'] = bd['vbo'][~bd['vbo'].index.duplicated(keep='first')]
    
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    
    # toc = time.perf_counter()
    # print('\t\ttictoc - prio, sort and drop duplicates', toc - tic, 'seconds')
    
    # print('\tBewaar de pnd prios in pndvk_prio.csv')
    # outputfile = OUTPUTDIR + 'pndvk_prio.csv'
    # bd['vbo'][['pndid', 'pndvkid', 'prio']]\
    #     .drop_duplicates().to_csv(outputfile, index=False)
    
    # toc = time.perf_counter()
    # print('\t\ttictoc - saving pnd-prio file in', toc - tic, 'seconds')
    
    
    # #############################################################################
    print('\n----7. Bewaren in koppelvlak3: vbovk -> hoofdpndvk met',
          doel2_vbovk_u, 'records...')
    # #############################################################################
    # tic = time.perf_counter()
    
    if doel2_vbovk_u > 20000000:
        print('Dit gaat even duren...')
        
    outputfile = K3DIR + 'vbovk_hoofdpndvk.csv'
    
    # vbovk_hoofdpndvk_df = vbovk_prio_df[['vboid', 'vbovkid', 'pndid', 'pndvkid']]
    # vbovk_hoofdpndvk_df.sort_values(['vboid', 'vbovkid']).to_csv(outputfile,
    #                                                            index=False)
    cols = ['vbovkbg', 'vbovkeg', 'vbostatus', 'pndid', 'pndvkid', 'pndstatus']
    bd['vbo'][cols].sort_index().to_csv(outputfile, index=True)
    
    toc = time.perf_counter()
    # print('\t\ttictoc - saving vbo-pnd file in', toc - tic, 'seconds')
    baglib.aprint(ll, 'Bepalen hoofdpand vk duurde', toc - tic)

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

    print('\t\tMeer minpunten als de afstand tussen vbo en pnd groter wordt')
    pnd1_df['prio'] += \
        - abs(pnd1_df['vbogmlx'] - pnd1_df['pndgmlx'])\
        - abs(pnd1_df['vbogmly'] - pnd1_df['pndgmly'])


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
print('------------- Start bag_vbovk2_pndvk lokaal ------------- \n')
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

    ll = 40
    
    bag_hoofdpnd(current_month=current_month,
                 koppelvlak2=DIR02,
                 koppelvlak3=DIR03,
                 loglevel=ll)


