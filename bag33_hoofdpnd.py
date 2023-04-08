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
    0. Lees vbo van koppelvlak 3 in (hierin zit de koppeling vbovk-pndvk) en
    lees pnd in uit koppelvlak 3
    1. Koppel deze en bepaal met de prioriteits functie het hoofdpand.
        De functie prio_pnd berekent voor elk pnd een prioriteit op basis
        van bouwjaar (moet bestaan), pand status (pnd moet niet gesloopt zijn 
                                                  e.d.)
        en afstand tot het vbo.
        

"""

# ################ import libraries ###############################
import pandas as pd
import numpy as np
import sys
import os
import time
import baglib
from config import OMGEVING, DIR03, IN_VOORRAAD, BAG_TYPE_DICT


# ############### Define functions ################################

def bag_hoofdpnd(current_month='testdata23',
                 koppelvlak3=os.path.join('..', 'data', '03-bewerkte-data'),
                 file_ext='parquet',
                 loglevel=10):

    tic = time.perf_counter()
    ll = loglevel
    
    baglib.printkop(ll+40, 'Start bag_hoofdpnd; ' + str(current_month))

    # #############################################################################
    # print('00.............Initializing variables...............................')
    # #############################################################################
    # month and dirs

    # K2DIR = koppelvlak2 + current_month + '/'
    K3DIR = os.path.join(koppelvlak3, current_month)
    
    if current_month == 'testdata23':
        current_year = 2000
    else:
        current_month = int(current_month)
        current_year = int(current_month/100)
    
    pd.set_option('display.max_columns', 20)
    
    INPUT_FILS_DICT = {'vbo': os.path.join(K3DIR, 'vbo'),
                       'pnd': os.path.join(K3DIR, 'pnd')}
    
    baglib.aprint(ll-10, INPUT_FILS_DICT)
    
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk}
    
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    nrec = {}       # aantal records
    nkey = {}       # aantal keyrecords
    
    # The tuning variables to determine pand prio
    # IN_VOORRAAD = ['inge', 'inni', 'verb', 'buig']

    IN_VOORRAAD_P = 10000         # if pndstatus is in IN_VOORRAAD
    BOUWJAAR_P = 5000             # if YEAR_LOW < bouwjaar < current_year + 1
    BOUWJAAR_DIV = 1              # divide bouwjaar with 2000 (small around 1)
    YEAR_LOW = 1000
   
    TEST_D = {'vboid': ['0388010000212290'],
              # 'pndid': ['0518100001754356', '0003100000117620']}
              'pndid': ['0003100000117620']}
    
    
    baglib.aprint(ll+40, '\n---------------DOELSTELLING------------------------------')
    baglib.aprint(ll+40, '--- Aan elk vbovk precies 1 hoofdpnd vk koppelen (stap 0 t/m 4)')
    baglib.aprint(ll+40, '--- "Inliggend"  bepalen: aantal vbo in 1 pnd (stap 5 t/m 6)')
    baglib.aprint(ll+30, '-----------------------------------------------------------')


    # ######################################################################
    baglib.printkop(ll+30, '0. Inlezen van vbo.' +file_ext+ ' en pnd.' +file_ext+ ' uit K3')
    baglib.aprint(ll+20, 'huidige maand (verslagmaand + 1):', current_month)
    
    bd = baglib.read_input(loglevel=ll, file_d=INPUT_FILS_DICT, bag_type_d=BAG_TYPE_DICT)

    cols = ['pndid', 'pndvkid', 'pndvkbg', 'pndvkeg', 'bouwjaar', 'pndstatus', 'pndgmlx', 'pndgmly']
    bd['pnd'] = bd['pnd'][cols] # .astype(dtype=BAG_TYPE_DICT)
    cols =  ['vboid', 'vbovkid',  'pndid', 'pndvkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'vbogmlx', 'vbogmly']
    bd['vbo'] = bd['vbo'][cols] # .astype(dtype=BAG_TYPE_DICT)
    

    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], key_lst=vbovk)
    (nrec['pnd'], nkey['pnd']) = baglib.df_comp(ll, bd['pnd'], key_lst=pndvk)

    baglib.aprint(ll-10, '\t\tAantal records en unieke', vbovk, 'in vbo:', nrec['vbo'], nkey['vbo'])
    baglib.aprint(ll-10, '\t\tAantal records en unieke', pndvk, 'in pnd:', nrec['pnd'], nkey['pnd'])

    # baglib.aprint(ll-10, 'DEBUG:\n', bd['pnd'].info())
    # baglib.aprint(ll-10, 'DEBUG:\n', bd['vbo'].info())
    
    baglib.debugprint(loglevel=ll-10, title='voorbeeld pnd na stap 0', df=bd['pnd'], colname='pndid', vals=TEST_D['pndid'])
    baglib.debugprint(loglevel=ll-10, title='voorbeeld vbo na 0. inlezen', 
                      df=bd['vbo'], colname='vboid', vals=TEST_D['vboid'], sort_on=vbovk)
    


    # ######################################################################
    baglib.aprint(ll, '1. Verwijder eendagsvliegen')
    for bob, vk in KEY_DICT.items():
        baglib.aprint(ll+10,'\tVerwijder eendagsvliegen bij', bob)
        bd[bob] = baglib.fix_eendagsvlieg(bd[bob], bob+'vkbg', bob+'vkeg')
        baglib.aprint(ll+10, '\tCheck na verwijderen', bob, 'met voorkomens', vk, ':')
        (nrec[bob], nkey[bob]) = baglib.df_comp(ll, bd[bob], key_lst=vk, 
                                                nrec=nrec[bob], nkey=nkey[bob])
    
    doel_vbovk_u = nkey['vbo']
    baglib.debugprint(loglevel=ll-10, title='pnd eendagsvliegen verwijderd na stap 1', 
                      df=bd['pnd'], colname='pndid', vals=TEST_D['pndid'])
    baglib.debugprint(loglevel=ll-10, title='voorbeeld vbo na 1. verwijder eendagsvliegen', 
                      df=bd['vbo'], colname='vboid', vals=TEST_D['vboid'], sort_on=vbovk)
    
    


    # ######################################################################
    baglib.printkop(ll+30, '2. koppelen vbo df met pnd df op pndvk')
    baglib.aprint(ll+10, '\tDOEL reminder: aantal unieke vbovk:', doel_vbovk_u)
    baglib.aprint(ll+10, '\tStart met de', nrec['vbo'], 'vbovk. Elk vbovk heeft 1 of',
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
    
    # bd['vbo'].set_index(vbovk, inplace=True)
    
    # zuinig met geheugen: Sommige types kunnen teruggecast worden.
    bd['vbo'] = baglib.recast_df_floats(bd['vbo'], BAG_TYPE_DICT, loglevel=ll)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'], key_lst=vbovk,
                                                u_may_change=False)
    
    doel2_vbovk_u = nkey['vbo']
    # print(bd['vbo'].info())
    baglib.debugprint(loglevel=ll-10, title='voorbeeld vbo na stap 2: koppelen met pnd inf', 
                      df=bd['vbo'], colname='vboid', vals=TEST_D['vboid'], sort_on=vbovk)

   



    # ######################################################################
    baglib.printkop(ll+30, '3. Bepaal prio voor pndvk: welke is het best om te koppelen')
    baglib.aprint(ll+10, '\t3a. We voegen een kolom prio toe')
    
    bd['vbo'] = prio_pnd(bd['vbo'],
                         IN_VOORRAAD_P, IN_VOORRAAD,
                         BOUWJAAR_P, YEAR_LOW, current_year + 1,
                         BOUWJAAR_DIV, loglevel=ll) # , PND_DIV)
    #controle
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                key_lst=vbovk,
                                                u_may_change=False)

    baglib.aprint(ll+10, '\t3b. Selecteer nu het pand met de hoogste prio. Alle pndvk krijgen een\n',
          '\tprio, maar de prio is alleen belangrijk als er meer dan 1 pnd koppelt')
    
    # how to remove non unique vbovk:
    # https://stackoverflow.com/questions/13035764/remove-pandas-rows-with-duplicate-indices/13036848#13036848
    bd['vbo'] = bd['vbo'].sort_values('prio', ascending=False)
    # bd['vbo'] = bd['vbo'][~bd['vbo'].index.duplicated(keep='first')]
    bd['vbo'] = bd['vbo'][~bd['vbo'].duplicated(subset=vbovk, keep='first')]
    
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'], key_lst=vbovk,
                                                u_may_change=False)
    baglib.debugprint(loglevel=ll-10, title='voorbeeld vbo na stap 3: pnd met hoogste prio bepaald', 
                      df=bd['vbo'], colname='vboid', vals=TEST_D['vboid'], sort_on=vbovk)
    



    # ######################################################################
    baglib.printkop(ll+30, '4. Bewaren in koppelvlak3: vbovk -> hoofdpndvk met '+str(doel2_vbovk_u)+' records')
    if doel2_vbovk_u > 20000000:
        baglib.aprint(ll+30, 'Dit gaat even duren...')

    cols = ['vboid', 'vbovkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'pndid', 'pndvkid', 'pndstatus']

    baglib.save_df2file(df=bd['vbo'],
                        outputfile=os.path.join(K3DIR, 'vbovk_hoofdpndvk'), 
                        file_ext=file_ext, append=False, loglevel=ll)

    
    # baglib.save_df2file(bd['vbo'][cols].sort_values(by=vbovk),
    #                     os.path.join(K3DIR, 'vbovk_hoofdpndvk'), file_ext, False)
    
    toc = time.perf_counter()
    baglib.aprint(ll+40, '\n*** bepalen hoofdpnd in', (toc - tic)/60, 'min **\n\n')
    mid = time.perf_counter()


    # ######################################################################
    baglib.printkop(ll+30, '---5. Bepalen inliggend: aantal vbo in pnd -------------')
    baglib.aprint(ll+10, '\tHet aantal vbo-s dat in een pand ligt, n_vbo_in_pndvk is een\n',
          '\teigenschap van pndvk. We tellen dan het aantal\n',
          '\tunieke vbo bij een pndvk, waarbij geldt dat zo een vbo pas meetelt,\n',
          '\tals 1 van zijn vk in IN_VOORRAAD zit.\n')
    baglib.aprint(ll+10, '\tStappen:\n',
                  '\t\t. Leidt de variabele voorraad af\n',
                  '\t\t. Verwijder vbovk per pndvk niet in voorraad\n',
                  '\t\t. Verwijder dubbele vbo per pndvk\n',
                  '\t\t. Tel het aantal vbo per pndvk\n'
                  '\t\t. Sommige pndvk hebben 0 vbo in voorraad en zijn nu weg,\n',
                  '\t\t. daarom left_merge met alle pndvk\n')
    baglib.aprint(ll+10, '\tDe situaties die voorkomen zijn:\n',
                  '\t\tA. pndvk heeft geen vbo. Schuurtje: pndvk_zonder_vbo\n',
                  '\t\t\tMaakt in dit geval typeinliggend = False\n'
                  '\t\tB. pndvk heeft 1 vbo: vbovk typeinliggend = False: woonhuis\n',
                  '\t\tC. pndvk heeft >1 vbo: vbovk typeinliggend = True: flat')
    
    

    baglib.aprint(ll+10, '\n\t\t--- 5.1:  vbovk: leid de variabele voorraad af')
    bd['vbo']['voorraad'] = bd['vbo']['vbostatus'].isin(IN_VOORRAAD)
    cols = ['vboid', 'vbovkid', 'pndid', 'pndvkid', 'voorraad']
    # vbo_df = bd['vbo'][cols].reset_index()
    vbo_df = bd['vbo'][cols]
    # print(vbo_df.info())
    baglib.debugprint(loglevel=ll-10, title='variabele voorraad net afgeleid', 
                      df=vbo_df, colname='pndid', vals=TEST_D['pndid']) 





    baglib.aprint(ll+10, '\n\t\t5.2 Verwijder vbovk die niet in voorraad zitten')
    vbo_df = vbo_df[vbo_df['voorraad']==True].drop(columns='voorraad')
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, vbo_df, nrec=nrec['vbo'],
                                                nkey=nkey['vbo'], key_lst=vbovk,
                                                u_may_change=True)
    baglib.debugprint(loglevel=ll-10, title='voorraad is TRUE', 
                      df=vbo_df, colname='pndid', vals=TEST_D['pndid'], sort_on=vbovk)
    




    baglib.aprint(ll+10, '\n\t\t5.3 laat vbovkid weg en ontdubbel: we willen unieke vbo per pndvk')
    pndvk_df = vbo_df.drop(columns ='vbovkid').drop_duplicates()
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, pndvk_df, nrec=nrec['vbo'],
                                                nkey=nkey['vbo'], key_lst=pndvk,
                                                u_may_change=True)
    baglib.debugprint(loglevel=ll-10, title='alleen nog vboid bij pndvk', 
                      df=pndvk_df, colname='pndid', vals=TEST_D['pndid'], sort_on='vboid')

    



    baglib.aprint(ll+10, '\n\t\t5.4 tel aantal vbo per pndvk')
    pndvk_df = pndvk_df.groupby(pndvk).size().to_frame('inliggend').reset_index()
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, pndvk_df, nrec=nrec['vbo'],
                                                nkey=nkey['vbo'], key_lst=pndvk,
                                                u_may_change=True)
    baglib.debugprint(loglevel=ll-10, title='groupby pndvk, tel vboid, noem dit inliggend', 
                      df=pndvk_df, colname='pndid', vals=TEST_D['pndid'], sort_on=pndvk)

    baglib.aprint(ll+20, '\tGemiddeld aantal vbo-s per pndvk:', pndvk_df['inliggend'].mean())
  




    baglib.aprint(ll+10, '\n\t\t5.5 left_merge met pndvk')
    pndvk2_df = bd['vbo'].reset_index()
    pndvk2_df = pndvk2_df[pndvk].drop_duplicates()
    pndvk2_df = pd.merge(pndvk2_df, pndvk_df, how='left', on=pndvk)
    pndvk2_df.fillna(0, inplace=True)
    pndvk2_df = baglib.recast_df_floats(pndvk2_df, BAG_TYPE_DICT)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, pndvk2_df, nrec=nrec['vbo'],
                                                nkey=nkey['vbo'], key_lst=pndvk,
                                                u_may_change=True)
    baglib.debugprint(loglevel=ll-10, title='nu ook met de pandvk zonder vbo in voorraad', 
                      df=pndvk2_df, colname='pndid', vals=TEST_D['pndid'], sort_on=pndvk)
    
    baglib.aprint(ll+20, '\tGem aantal vbo-s per pndvk (incl lege pndvk):', pndvk2_df['inliggend'].mean())

    # switch this on to find interesting data for debugprint
    # baglib.aprint(ll, pndvk2_df.sort_values(by='inliggend', ascending=False).head(50))



    
    baglib.aprint(ll+10, '\n\t\t5.5 dit aantal toewijzen aan vbovk')
    baglib.aprint(ll+10, '\t\t\tkoppel hiervoor weer vbovk weer aan pndvk, nu vanuit vbovk')
    vbo_df = bd['vbo'].reset_index()
    vbo_df = pd.merge(vbo_df, pndvk2_df, how='inner', on=pndvk)
    cols = ['vboid', 'vbovkid', 'pndid', 'pndvkid', 'inliggend']
    vbo_df = vbo_df[cols]
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, vbo_df, nrec=nrec['vbo'],
                                                nkey=nkey['vbo'], key_lst=vbovk,
                                                u_may_change=True)
    baglib.debugprint(loglevel=ll-10, title='inliggend toegekend aan alle vbovk bij het pndvk', 
                      df=vbo_df, colname='pndid', vals=TEST_D['pndid'], sort_on=vbovk)


    baglib.aprint(ll+20, '\tGemiddeld inliggend per vbovk:', vbo_df['inliggend'].mean())

    
    # ######################################################################
    baglib.printkop(ll+30, '6. bewaren in pndvk_nvbo vbovk_nvbo')

    baglib.save_df2file(df=pndvk2_df,
                        outputfile=os.path.join(K3DIR, 'pndvk_nvbo'), 
                        file_ext=file_ext, append=False, loglevel=ll)

    baglib.save_df2file(df=vbo_df, outputfile=os.path.join(K3DIR, 'vbovk_nvbo'), 
                        file_ext=file_ext, append=False, loglevel=ll)


     
    toc = time.perf_counter()
    baglib.aprint(ll+40, '\n*** bepalen inliggend in', (toc - mid)/60, 'min ***')
    baglib.aprint(ll+40, '\n*** Einde bag_hoofdpnd in', (toc - tic)/60, 'min ***\n')



    
   
    
def prio_pnd(pnd1_df,
             in_voorraad_points, in_voorraad_statuslst,
             bouwjaar_points, bouwjaar_low, bouwjaar_high, bouwjaar_divider,
             loglevel=10):
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
    
    ll = loglevel
    
    baglib.aprint(ll+10, '\tDe prio van een pnd is de som van onderstaande punten:')
    baglib.aprint(ll+10, '\t\tPunten als het pand in voorraad is:', in_voorraad_points)
    _in_voorraad_check = pnd1_df['pndstatus'].isin(in_voorraad_statuslst)
    pnd1_df['prio'] = np.where(_in_voorraad_check, in_voorraad_points, 0)

    baglib.aprint(ll+10, '\t\tPunten als het bouwjaar logisch is:', bouwjaar_points)
    _logisch_bouwjaar_check = pnd1_df['bouwjaar'].\
        between(bouwjaar_low, bouwjaar_high)
    pnd1_df['prio'] += np.where(_logisch_bouwjaar_check, bouwjaar_points, 0)
    
    baglib.aprint(ll+10, '\t\tMinpunten om switchen iets te ontmoedigen:', '-vkid')
    pnd1_df['prio'] -= pnd1_df['pndvkid']
 
    baglib.aprint(ll+10, '\t\tMinpunten voor het bouwjaar zelf (ca -1):',
            '-bouwjaar/' + str(bouwjaar_divider))
    pnd1_df['prio'] -= pnd1_df['bouwjaar'] / bouwjaar_divider

    baglib.aprint(ll+10, '\t\tMeer minpunten als de afstand tussen vbo en pnd groter wordt')
    pnd1_df['prio'] += \
        - abs(pnd1_df['vbogmlx'] - pnd1_df['pndgmlx'])\
        - abs(pnd1_df['vbogmly'] - pnd1_df['pndgmly'])


    # baglib.aprint(ll+10, '\t\tMinpunten voor pandid (tbv herhaalbaarheid):',
    #         '-pndid /' + str(pndid_divider))
    # pnd1_df['prio'] -= pnd1_df['pndid'].astype(int) / pndid_divider

    # baglib.aprint(ll+10, active_pnd_df[['pndid', 'pndstatus', 'bouwjaar', 'prio']].head(30))
    # baglib.aprint(ll+10, '\tnumber of records in activ_pnd_df:', active_pnd_df.shape[0])
    # if pnd1_df.shape[0] != pnd1_df['pndid'].unique().shape[0]:
    #     baglib.aprint(ll+10, 'Error of niet he;
    #           in functie prio_pnd: unieke panden versus actief')

    return pnd1_df
       
# ########################################################################
# ########################################################################


if __name__ == '__main__':

    ll = 40
    file_ext = 'parquet'
    # file_ext = 'csv'
    
    baglib.printkop(ll+40, OMGEVING)
    current_month = baglib.get_arg1(sys.argv, DIR03)

    
    baglib.aprint(ll+10, '------------- Start bag_hoofdpnd lokaal ------------- \n')
    bag_hoofdpnd(current_month=current_month,
                 koppelvlak3=DIR03,
                 loglevel=ll, file_ext=file_ext)


