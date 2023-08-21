#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Woensdag 11 Jan 2023

We willen eigenschappen van een pand (zoals bouwjaar en pand status op vbovk 
niveau gebruiken. Daarvoor is het nodig om een koppeling tussen vbovk
en precies 1 hoofdpandvk te hebben. Dat gaat in ca 99,5% van de gevallen 
direct goed, maar een half procent van de vbo's heeft twee of meer panden.

Doel: bepaal elke VBO voorkomen (vbovk) precies 1 hoofdpand voorkomen (pndvk).

Input: vbovk en pndvk
Output: vbovk - hoofdpndvk

Omdat de module fix_vk ervoor heeft gezorgd dat een
vbovk altijd binnen de looptijd van een pndvk valt, nemen we dit als uitgangspunt.

"""

# ################ import libraries ###############################
import pandas as pd
import numpy as np
import sys
import os
import time
import baglib
from config import OMGEVING, KOPPELVLAK0, KOPPELVLAK3a, BAG_TYPE_DICT,  IN_VOORRAAD, FILE_EXT, LOGFILE
from k2_fixvk import k2_fixvk
import logging
# ############### Define functions ################################

def k3_hoofdpnd(maand, logit):

    tic = time.perf_counter()
    logit.info(f'start k3_hoofdpnd voor maand {maand}')

    # #############################################################################
    # Initializing variables
    
    # zowel input als output in/naar koppelvlak 3a
    dir_k3a_maand = os.path.join(KOPPELVLAK3a, maand)

    if not os.path.exists(os.path.join(KOPPELVLAK3a, maand, 'vbo.'+FILE_EXT)):
        logit.info(f'vbo.{FILE_EXT} in koppelvlak 3a niet gevonden. Probeer af te leiden...')
        k2_fixvk(maand, logit)
    
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    
    # doelen: 
        # aan elk vbovk precies 1 hoofdpnd vk koppelen (stap 0 t/m 4)')
        # "inliggend"  bepalen: aantal vbo in 1 pnd (stap 5 t/m 6)')

    # ######################################################################
    logit.debug('stap 0: inlezen')    
    vbo_df = baglib.read_input(input_file=os.path.join(dir_k3a_maand, 'vbo'),
                              bag_type_d=BAG_TYPE_DICT,
                              output_file_type='pandas',
                              logit=logit)
    pnd_df = baglib.read_input(input_file=os.path.join(dir_k3a_maand, 'pnd'),
                              bag_type_d=BAG_TYPE_DICT,
                              output_file_type='pandas',
                              logit=logit)

    cols = ['pndid', 'pndvkid', 'pndvkbg', 'pndvkeg', 'bouwjaar', 'pndstatus', 'pndgmlx', 'pndgmly']
    pnd_df = pnd_df[cols] # .astype(dtype=BAG_TYPE_DICT)
    cols =  ['vboid', 'vbovkid',  'pndid', 'pndvkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'vbogmlx', 'vbogmly']
    vbo_df = vbo_df[cols] # .astype(dtype=BAG_TYPE_DICT)
    nrec, nkey = baglib.df_compare(vbo_df, vk_lst=vbovk, nrec_nvk_ratio_is_1=False, logit=logit)


    # ######################################################################
    logit.debug('stap 1. verwijder eendagsvliegen')
    logit.warning('stap 1. verwijder eendagsvliegen staat uit. Is onnodig na fix_vk.')
    '''
    vbo_df = baglib.fix_eendagsvlieg(vbo_df, 'vbovkbg', 'vbovkeg', logit)
    logit.warning('check of deze stap uitgezet kan worden:')
    nrec, nkey = baglib.df_compare(df=vbo_df, vk_lst=vbovk, nrec=nrec, nvk=nkey, 
                                   nrec_nvk_ratio_is_1=False,
                                   nvk_marge=0, logit=logit)
    pnd_df = baglib.fix_eendagsvlieg(pnd_df, 'pndvkbg', 'pndvkeg', logit)
    '''
    # ######################################################################
    logit.debug('stap 2. koppelen vbo df met pnd df op pndvk')
    # start met de vbovk. Elk vbovk heeft 1 of meer pndid (dubbele bij de zelf
    # aangemaakte vbovk). Dus 
    #       vbovk1, pndid1
    #       vbovk1, pndid2 (dit vbovk is dan zelf aangemaakt (en dubbel))
    #       vbovk2, pndid3
    #       Koppel nu aan pndid1, pndid2, pndid3 alle voorkomens van het
    #       betreffende pnd. Het aantal records wordt hiermee ruim verdubbeld

    vbo_df = pd.merge(vbo_df, pnd_df, how='left', on=pndvk)
    del pnd_df
    
    vbo_df = baglib.recast_df_floats(vbo_df, BAG_TYPE_DICT, logit=logit)
    nrec, nkey = baglib.df_compare(df=vbo_df, vk_lst=vbovk, nrec=nrec, nvk=nkey, 
                                   nrec_nvk_ratio_is_1=False,
                                   nvk_marge=0.1, logit=logit)

    # ######################################################################
    logit.debug('stap 3. bepaal prio voor pndvk: welke is het best om te koppelen')
    
    # We voegen een kolom prio toe
    IN_VOORRAAD_POINTS = 10000         # if pndstatus is in IN_VOORRAAD
    BOUWJAAR_POINTS = 5000             # if YEAR_LOW < bouwjaar < current_year + 1
    BOUWJAAR_DIV = 1              # divide bouwjaar with 2000 (small around 1)
    BOUWJAAR_LOW = 1000
    BOUWJAAR_HIGH = 2030

    vbo_df = prio_pnd(df=vbo_df,
                      in_voorraad_points=IN_VOORRAAD_POINTS, 
                      in_voorraad=IN_VOORRAAD,
                      bouwjaar_points=BOUWJAAR_POINTS,
                      bouwjaar_low=BOUWJAAR_LOW,
                      bouwjaar_high=BOUWJAAR_HIGH,
                      bouwjaar_divider=BOUWJAAR_DIV, logit=logit) # , PND_DIV)
    #controle
    nrec, nkey = baglib.df_compare(df=vbo_df, vk_lst=vbovk, nrec=nrec, nvk=nkey, 
                                   nrec_nvk_ratio_is_1=False,
                                   nvk_marge=0, logit=logit)

    vbo_df = vbo_df.sort_values('prio', ascending=False)
    vbo_df = vbo_df[~vbo_df.duplicated(subset=vbovk, keep='first')]
    
    nrec, nkey = baglib.df_compare(df=vbo_df, vk_lst=vbovk, nrec=nrec, nvk=nkey, 
                                   nrec_nvk_ratio_is_1=True, # hier was het om te doen!
                                   nvk_marge=0, logit=logit)



    # ######################################################################
    logit.debug('stap 4. bewaren in koppelvlak3: vbovk -> hoofdpndvk met {str(doel2_vbovk_u)} records')
    # if doel2_vbovk_u > 20000000:
    #     logit.debug(f'+30, 'Dit gaat even duren...')

    cols = ['vboid', 'vbovkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'pndid', 'pndvkid', 'pndstatus']

    baglib.save_df2file(df=vbo_df[cols],
                        outputfile=os.path.join(dir_k3a_maand, 'vbovk_hoofdpndvk'), 
                        file_ext=FILE_EXT, append=False, logit=logit)

    
    # baglib.save_df2file(vbo_df[cols].sort_values(by=vbovk),
    #                     os.path.join(K3DIR, 'vbovk_hoofdpndvk'), file_ext, False)
    
    toc = time.perf_counter()
    logit.info(f'bepalen hoofdpnd klaar in {(toc - tic)/60} min')


def K3_inliggend(maand, logit):
    
    
    # ######################################################################
    logit.debug('start functie K3_inliggend: het aantal vbo in dat in een pnd ligt')
    # we bepalen het aantal vbo-s dat in een pnd voorkomen ligt!
    # het aantal vbo-s dat in een pndvk ligt, n_vbo_in_pndvk is een
    # eigenschap van pndvk. We tellen het aantal
    # unieke vbo bij een pndvk, waarbij geldt dat zo een vbo pas meetelt
    # als 1 van zijn vk in IN_VOORRAAD zit
    
    '''
    logit.debug(f'+10, '\tStappen:\n',
                  '\t\t. Leidt de variabele voorraad af\n',
                  '\t\t. Verwijder vbovk per pndvk niet in voorraad\n',
                  '\t\t. Verwijder dubbele vbo per pndvk\n',
                  '\t\t. Tel het aantal vbo per pndvk\n'
                  '\t\t. Sommige pndvk hebben 0 vbo in voorraad en zijn nu weg,\n',
                  '\t\t. daarom left_merge met alle pndvk\n')
    logit.debug(f'+10, '\tDe situaties die voorkomen zijn:\n',
                  '\t\tA. pndvk heeft geen vbo. Schuurtje: pndvk_zonder_vbo\n',
                  '\t\t\tMaakt in dit geval typeinliggend = False\n'
                  '\t\tB. pndvk heeft 1 vbo: vbovk typeinliggend = False: woonhuis\n',
                  '\t\tC. pndvk heeft >1 vbo: vbovk typeinliggend = True: flat')
    '''
    '''

    logit.debug(f'stap 1. leid de variabele voorraad af')
    vbo_input_df['voorraad'] = vbo_input_df['vbostatus'].isin(IN_VOORRAAD)
    cols = ['vboid', 'vbovkid', 'pndid', 'pndvkid', 'voorraad']
    # vbo_df = vbo_input_df[cols].reset_index()
    vbo_df = vbo_input_df[cols]
    # print(vbo_df.info())
    baglib.debugprint(loglevel=ll-10, title='variabele voorraad net afgeleid', 
                      df=vbo_df, colname='pndid', vals=TEST_D['pndid']) 



    '''
    '''
    logit.debug(f'+10, '\n\t\t5.2 Verwijder vbovk die niet in voorraad zitten')
    vbo_df = vbo_df[vbo_df['voorraad']==True].drop(columns='voorraad')
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, vbo_df, nrec=nrec['vbo'],
                                                nvk=nkey['vbo'], key_lst=vbovk,
                                                u_may_change=True)
    baglib.debugprint(loglevel=ll-10, title='voorraad is TRUE', 
                      df=vbo_df, colname='pndid', vals=TEST_D['pndid'], sort_on=vbovk)
    




    logit.debug(f'+10, '\n\t\t5.3 laat vbovkid weg en ontdubbel: we willen unieke vbo per pndvk')
    pndvk_df = vbo_df.drop(columns ='vbovkid').drop_duplicates()
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, pndvk_df, nrec=nrec['vbo'],
                                                nvk=nkey['vbo'], key_lst=pndvk,
                                                u_may_change=True)
    baglib.debugprint(loglevel=ll-10, title='alleen nog vboid bij pndvk', 
                      df=pndvk_df, colname='pndid', vals=TEST_D['pndid'], sort_on='vboid')

    



    logit.debug(f'+10, '\n\t\t5.4 tel aantal vbo per pndvk')
    pndvk_df = pndvk_df.groupby(pndvk).size().to_frame('inliggend').reset_index()
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, pndvk_df, nrec=nrec['vbo'],
                                                nvk=nkey['vbo'], key_lst=pndvk,
                                                u_may_change=True)
    baglib.debugprint(loglevel=ll-10, title='groupby pndvk, tel vboid, noem dit inliggend', 
                      df=pndvk_df, colname='pndid', vals=TEST_D['pndid'], sort_on=pndvk)

    logit.debug(f'+20, '\tGemiddeld aantal vbo-s per pndvk:', pndvk_df['inliggend'].mean())
  




    logit.debug(f'+10, '\n\t\t5.5 left_merge met pndvk')
    pndvk2_df = vbo_input_df.reset_index()
    pndvk2_df = pndvk2_df[pndvk].drop_duplicates()
    pndvk2_df = pd.merge(pndvk2_df, pndvk_df, how='left', on=pndvk)
    pndvk2_df.fillna(0, inplace=True)
    pndvk2_df = baglib.recast_df_floats(pndvk2_df, BAG_TYPE_DICT)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, pndvk2_df, nrec=nrec['vbo'],
                                                nvk=nkey['vbo'], key_lst=pndvk,
                                                u_may_change=True)
    baglib.debugprint(loglevel=ll-10, title='nu ook met de pandvk zonder vbo in voorraad', 
                      df=pndvk2_df, colname='pndid', vals=TEST_D['pndid'], sort_on=pndvk)
    
    logit.debug(f'+20, '\tGem aantal vbo-s per pndvk (incl lege pndvk):', pndvk2_df['inliggend'].mean())

    # switch this on to find interesting data for debugprint
    # logit.debug(f', pndvk2_df.sort_values(by='inliggend', ascending=False).head(50))



    
    logit.debug(f'+10, '\n\t\t5.5 dit aantal toewijzen aan vbovk')
    logit.debug(f'+10, '\t\t\tkoppel hiervoor weer vbovk weer aan pndvk, nu vanuit vbovk')
    vbo_df = vbo_input_df.reset_index()
    vbo_df = pd.merge(vbo_df, pndvk2_df, how='inner', on=pndvk)
    cols = ['vboid', 'vbovkid', 'pndid', 'pndvkid', 'inliggend']
    vbo_df = vbo_df[cols]
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(ll, vbo_df, nrec=nrec['vbo'],
                                                nvk=nkey['vbo'], key_lst=vbovk,
                                                u_may_change=True)
    baglib.debugprint(loglevel=ll-10, title='inliggend toegekend aan alle vbovk bij het pndvk', 
                      df=vbo_df, colname='pndid', vals=TEST_D['pndid'], sort_on=vbovk)


    logit.debug(f'+20, '\tGemiddeld inliggend per vbovk:', vbo_df['inliggend'].mean())

    
    # ######################################################################
    logit.debug('+30, '6. bewaren in pndvk_nvbo vbovk_nvbo')

    baglib.save_df2file(df=pndvk2_df,
                        outputfile=os.path.join(K3DIR, 'pndvk_nvbo'), 
                        file_ext=file_ext, append=False, loglevel=ll)

    baglib.save_df2file(df=vbo_df, outputfile=os.path.join(K3DIR, 'vbovk_nvbo'), 
                        file_ext=file_ext, append=False, loglevel=ll)


     
    toc = time.perf_counter()
    logit.debug(f'+40, '\n*** bepalen inliggend in', (toc - mid)/60, 'min ***')
    logit.debug(f'+40, '\n*** Einde bag_hoofdpnd in', (toc - tic)/60, 'min ***\n')
    '''

def prio_pnd(df=pd.DataFrame(),
             in_voorraad_points=10000,
             in_voorraad=['v3', 'v4', 'v8', 'v6'],
             bouwjaar_points=5000,
             bouwjaar_low=1000,
             bouwjaar_high=2030,
             bouwjaar_divider=1,
             logit=logging.DEBUG):
             # pndid_divider):
    """
    Bereken een prioriteit voor elk pndid in df.
    Voorwaarden: df heeft de velden:
        * pndstatus
        * bouwjaar
        * pndvkid
        * vbogmlx vbogmly pndgmlx pndgmly
        
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
    Hetzelfde input dataframe pnd_df met een extra kolom prio.

    """
    
    logit.debug('de prio van een pnd is de som van onderstaande punten:')
    logit.debug(f'punten als het pand in voorraad is: {in_voorraad_points}')
    _in_voorraad_check = df['pndstatus'].isin(in_voorraad)
    df['prio'] = np.where(_in_voorraad_check, in_voorraad_points, 0)

    logit.debug(f'punten als het bouwjaar logisch is {bouwjaar_points}')
    _logisch_bouwjaar_check = df['bouwjaar'].\
        between(bouwjaar_low, bouwjaar_high)
    df['prio'] += np.where(_logisch_bouwjaar_check, bouwjaar_points, 0)
    
    logit.debug('minpunten om switchen iets te ontmoedigen: -vkid')
    df['prio'] -= df['pndvkid']
 
    logit.debug('minpunten voor het bouwjaar zelf (ca -1) -bouwjaar/bouwjaar_divider')
    df['prio'] -= df['bouwjaar'] / bouwjaar_divider

    logit.debug('meer minpunten als de afstand tussen vbo en pnd groter wordt')
    df['prio'] += \
        - abs(df['vbogmlx'] - df['pndgmlx'])\
        - abs(df['vbogmly'] - df['pndgmly'])


    # logit.debug(f'+10, '\t\tMinpunten voor pandid (tbv herhaalbaarheid):',
    #         '-pndid /' + str(pndid_divider))
    # pnd_df['prio'] -= pnd_df['pndid'].astype(int) / pndid_divider

    # logit.debug(f'+10, active_pnd_df[['pndid', 'pndstatus', 'bouwjaar', 'prio']].head(30))
    # logit.debug(f'+10, '\tnumber of records in activ_pnd_df:', active_pnd_df.shape[0])
    # if pnd_df.shape[0] != pnd_df['pndid'].unique().shape[0]:
    #     logit.debug(f'+10, 'Error of niet he;
    #           in functie prio_pnd: unieke panden versus actief')

    return df
       
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
    logit.info('start k3_hoofdpnd vanuit main')
    logit.warning(OMGEVING)
    logit.setLevel(logging.INFO)
    
    maand_lst = baglib.get_args(sys.argv, KOPPELVLAK0)

    for maand in maand_lst:
        k3_hoofdpnd(maand, logit)
        # k3_inliggend(maand, logit)

