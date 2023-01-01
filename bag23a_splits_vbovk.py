#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 23 dec 2022

In de BAG wordt een vbovk (vbo voorkomen) aan een of meerdere pnd (pand) of 
num (nummeraanduiding) gelinkt. Dat is 
feitelijk onzorgvuldig omdat tijdens de looptijd van een vbovk zaken
kunnen veranderen.

Doel: creeer nieuwe vbo voorkomens (vk) als er iets wijzigt in "het wezen" van 
het vbo: iets dat impact heeft op de output.

We onderscheiden 5 soorten wijzigingen:
    
    1. Een pand (pnd) van dat vbo krijgt een nieuw vk
    2. Een pnd van dat vbo wordt afgesloten en krijgt geen nieuw vk
    3. De nummeraanduiding (num) van dat vbo krijgt een nieuw vk
    3. De openbare ruimte (opr) van die num  krijgt een nieuw vk
    4. De woonplaats (wpl) van die opr krijgt een nieuw vk
    5. De gemeente (gem) van die wpl krijgt een nieuw vk

Als voorbeeld van 5. Als de een vbo in een nieuwe gemeente komt te liggen, 
heeft dat impact op de statistische output. Om een consistent bus bestand van
vbo te krijgen moet je dan een nieuw vbovk aanmaken (tenzij op die datum
al een nieuw vk begint)
    
We gaan nieuwe vbovk maken zodat elk vbovk precies binnen de looptijd van een 
pndvk en numvk valt:

    Input:  vbovk, pnd,   num
    Output: vbovk, pndvk, numvk

Keuzen
1. een vbo kan niet zonder pnd bestaan en een pnd niet zonder vbo
2. een vbo kan niet zonder num, kan niet zonder opr kan niet zonder
    wpl kan niet zonder gemeente bestaan.

Aanpak:
1. Bepaal voor wpl-gem of er nieuwe wplvk moeten worden aangemaakt. Een nieuw
wplvk wordt alleen aangemaakt er een nieuw gemvk is
...

1. Bepaal voor elke vbo-pnd combinatie het interval waarop beide bestaan:
    vbopnd_bg en vbopnd_eg
    bg = begindatum geldigheid
    eg = einddatum geldigheid
2. Verzamel de vkbg van vbo en pnd in vbopnd en maak hiermee nieuwe vbovk
3. Zorg dat het eerste vbovk start op vbopnd_bg 
4. Als een vbopnd_eg < future_date, beeindig dan het vbovk in vbopnd
    future_date is een vaste dag in de verre toekomst
Resultaat: vbovk-pndvk waarbij elk vbovk binnen een pndvk valt,
aan de voorkant gelijktijdig startend met het eerste pnd en aan de achterkant
eventueel afgekapt door daar een nieuw voorkomen te beginnen (waar in dit 
voorbeeld pnd2 eindigt):
    

Situatie omschrijvingen

Ad1: voeg pndvkbg (pand voorkomen begindatum 
geldigheid) toe aan vbovkbg.
Dit levert de startdatum van een nieuw vbovk. Dit nieuwe vbovk heeft een
verwijzing naar het pndvk met die pndvkbg en krijgt verder alle eigenschappen
van het voorafgaande vbovk

vbovk        1            2            3            4
vbo1  o-----------oo-----------oo-----------oo---------->
pnd1       o-----------oo------------------------------->
pnd2                       o-----------o

wordt

vbovk2         1     2   3  4      5     6      7
vbo1       o------oo---oo-oo---oo-----------oo---------->
pnd1       o-----------oo------------------------------->
pnd2                       o-----------o



Ad2. Met deze gebeurtenis wordt de vbopnd relatie beeindigd. vbopnd_eg is de
vbopnd einddatum geldigheid. De vbopnd_eg wordt toegevoegd aan de vbovkbg en
hiermee creeren we een nieuw vbovk. Dit nieuwe vbovk heeft een verwijzing naar
alle andere panden waarna het vbovk verwees. In het voorbeeld hierboven is dat
alleen pnd1
            1            2            3            4
vbo1  o-----------oo-----------oo-----------oo---------->
pnd2                       o-----------o

vbovk nummer 3 wordt gesplitst:
    
vbovk2      1            2          3     4       5
vbo1  o-----------oo-----------oo------oo---oo---------->
pnd2                       o-----------o

    

Ad3. 
            1            2            3            4
vbo1  o-----------oo-----------oo-----------oo---------->
pnd1       o-----------oo------------------------------->
pnd2                       o-----------o
num1    o-------------------------oo-------------------->

Splits het voorkomen 3

Ad4.

num1    o-------------------------oo-------------------->
opr1    o---------oo------------------------------------>

de straat krijgt een nieuwe naam:
num1    o---------oo--------------oo-------------------->
opr1    o---------oo------------------------------------>


"""

# ################ import libraries ###############################
import pandas as pd
# import numpy as np
import sys
# import os
import time
import baglib
from baglib import BAG_TYPE_DICT
from config import LOCATION


# ############### Define functions ################################

def bag_splits_vbovk(current_month='testdata',
                     koppelvlak2='../data/02-csv',
                     koppelvlak3='../data/03-bewerkte-data',
                     loglevel=True):

    tic = time.perf_counter()
    
    print('-------------------------------------------')
    print('--- Start bag_splits_vbovk', current_month, ' -----')
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
        # current_year = int(current_month/100)
    
    pd.set_option('display.max_columns', 20)
    
    INPUT_FILES_DICT = {'vbo': K2DIR + 'vbo.csv',
                        'pnd': K2DIR + 'pnd.csv',
                        'num': K2DIR + 'num.csv',
                        'opr': K2DIR + 'opr.csv',
                        'wpl': K2DIR + 'wpl.csv'}
                        # 'wplgem': K2DIR + 'wplgem.csv'}
        
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    numvk = ['numid', 'numvkid']
    oprvk = ['oprid', 'oprvkid']
    wplvk = ['wplid', 'wplvkid']
    # wplgemvk = ['wplid', 'wplgemvkid']        
    
    KEY_DICT = {# 'vbo': vbovk,
                # 'pnd': pndvk,
                # 'num': numvk,
                'opr': oprvk,
                'wpl': wplvk}
                # 'wplgem': wplgemvk}
    
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    nrec = {}
    nkey = {}    
    # if printit = True bepaal-hoofdpnd prints extra info
    # printit = True
    
    FUTURE_DATE = 20321231
    DEBUGLEVEL = 0
    
    TEST_D = {'wplid': ['3386', '1012', '3631'],
              'oprid': ['0457300000000259', '0457300000000260', '0003300000116985'],
              'numid': ['1979200000000546', '0457200000521759', '0457200000521256'],
              'vboid': ['0457010000060735', '0457010000061531', '1979010000000545'],
              'pndid': ['0457100000064572', '0457100000065899', '1979100000000651']}

    print('\n---------------DOELSTELLING--------------------------------')
    print('----splits VBO voorkomens (vbovk) als er tijdens de loooptijd')
    print('----van dat vbovk een van 5 gebeurtenissen optreedt')
    print('-----------------------------------------------------------')

    # #########################################################################
    print('\n----0a. Inlezen van de inputbestandenv -----------------------\n')
    # #########################################################################
    
    # print('huidige maand (verslagmaand + 1):', current_month)
    
    bd = baglib.read_input_csv(INPUT_FILES_DICT, BAG_TYPE_DICT)

    # #############################################################################
    print('\n----0b. Verwijder eendagsvliegen -----------------------\n')
    # #############################################################################
    for bob, vk in KEY_DICT.items():
        (nrec[bob], nkey[bob]) = baglib.df_comp(bd[bob], key_lst=vk) 
        print('\tVerwijder eendagsvliegen bij', bob)
        bd[bob] = baglib.fix_eendagsvlieg(bd[bob], bob+'vkbg', bob+'vkeg')
        (nrec[bob], nkey[bob]) = baglib.df_comp(bd[bob], key_lst=vk, 
                                                nrec=nrec[bob], nkey=nkey[bob])
    
    baglib.debugprint(df=bd['opr'], 
                      colname='oprid', vals=['0457300000000260'], 
                      sort_on=[], debuglevel=DEBUGLEVEL)

    baglib.debugprint(df=bd['wpl'], colname='wplid',
                   vals=['1012'], sort_on=[], debuglevel=DEBUGLEVEL)

    # #############################################################################
    print('\n----1. Roep vksplitter aan -----------------------\n')
    # #############################################################################
    
    bd['opr'] = baglib.vksplitter(df=bd['opr'], gf=bd['wpl'],
                                  fijntype='opr', groftype='wpl',
                                  future_date=FUTURE_DATE,
                                  test_d=TEST_D)

    cols = ['oprid', 'oprvkid2']
    baglib.debugprint(title='Testje na vksplitter',
                      df=bd['opr'], colname='oprid', vals=TEST_D['oprid'], sort_on=cols, 
                      debuglevel=DEBUGLEVEL)

    bd['num'] = baglib.vksplitter(df=bd['num'], gf=bd['opr'],
                                  fijntype='num', groftype='opr',
                                  future_date=FUTURE_DATE,
                                  test_d=TEST_D)
    
    bd['vbo'] = baglib.vksplitter(df=bd['vbo'], gf=bd['num'],
                                  fijntype='vbo', groftype='num',
                                  future_date=FUTURE_DATE,
                                  test_d=TEST_D)
    
    # bd['vbo'].drop(labels='vbovkid2', axis=1, inplace=True)
    
    # bd['vbo'] = bd['vbo'].rename(columns={'vbovkid': 'vbovkid_oud'})
    # bd['vbo'] = bd['vbo'].rename(columns={'vbovkid2': 'vbovkid'})
    
    bd['vbo'].rename(columns={'vbovkid': 'vbovkid_oud', 'vbovkid2': 'vbovkid'}, inplace=True)

    # print(bd['vbo'].info())
    # print(bd['vbo'][['vboid', 'vbovkid']].head())
    
    bd['vbo'] = baglib.vksplitter(df=bd['vbo'], gf=bd['pnd'],
                                  fijntype='vbo', groftype='pnd',
                                  future_date=FUTURE_DATE,
                                  test_d=TEST_D)

    '''
    # #############################################################################

    # #############################################################################
    print('\n----1. Bepaal voor elke vbo-pnd combinatie het interval waarop',
          ' beide bestaan ---\n')
    # #############################################################################

    # vk = voorkomen 
    # bg = begindatum geldigheid
    # eg = einddatum geldigheid
    vbopnd_bg_eg = bd['vbo'].groupby(['vboid', 'pndid']).agg({'vbovkbg': 'min', 
                                                              'vbovkeg': 'max'}).reset_index()
    pnd_bg_eg = bd['pnd'].groupby('pndid').agg({'pndvkbg': 'min', 'pndvkeg' : 'max'}).reset_index()
    (nrec, nkey) = baglib.df_comp(vbopnd_bg_eg, key_lst=['vboid', 'pndid'])
    # print(vbopnd_bg_eg.head())
    # print(pnd_bg_eg.head())
    
    vbopnd_bg_eg = pd.merge(vbopnd_bg_eg, pnd_bg_eg, how='left', on='pndid')
    del pnd_bg_eg
    
    vbopnd_bg_eg['vbopnd_bg'] = vbopnd_bg_eg[['vbovkbg', 'pndvkbg']].max(axis=1)
    vbopnd_bg_eg['vbopnd_eg'] = vbopnd_bg_eg[['vbovkeg', 'pndvkeg']].min(axis=1)

    baglib.debugprint(df=vbopnd_bg_eg,
                      colname='vboid', val='0762010000011444', 
                      sortlist=['vboid', 'pndid', 'vbovkbg'], debuglevel=DEBUGLEVEL)


    vbopnd_bg_eg.drop(['vbovkbg', 'vbovkeg', 'pndvkbg', 'pndvkeg'], axis=1, inplace=True)
    (nrec, nkey) = baglib.df_comp(vbopnd_bg_eg, key_lst=['vboid', 'pndid'],
                                  nrec=nrec, nkey=nkey, u_may_change=True)
    # print(vbopnd_bg_eg.head())


    # #############################################################################
    print('\n----2. Verzamel de vkbg van vbo en pnd in vbopnd en maak hiermee',
          'nieuwe vbovk ---\n')
    # #############################################################################

    print('\t\t2a. Begin met pndid, pndvkid en pndvkbg')
    print('\t\tUniek in het record is vboid, pndid, pndvkbg')
    (nrec, nkey) = baglib.df_comp(vbopnd_bg_eg, key_lst=['vboid', 'pndid'])
    pnd_df = pd.merge(vbopnd_bg_eg, bd['pnd'][['pndid', 'pndvkid', 'pndvkbg']],
                      how='inner', on='pndid')
    print('\t\tAantal records moet grofweg verdubbelen: "van pnd naar pandvk" ')
    (nrec, nkey) = baglib.df_comp(pnd_df, key_lst=['vboid', 'pndid', 'pndvkbg'],
                                  nrec=nrec, nkey=nkey, u_may_change=True)
    

    print('\n\t\t2b. Hernoem de kolom pndvkbg naar vbovkbg zodat we de',
          '\n\t\tkolommen van vkbg van vbo en pnd kunnen concateneren')
    pnd_df.rename({'pndvkbg': 'vbovkbg'}, axis='columns', inplace=True)


    print('\n\t\t2c. Parallel hieraan: neem vboid, pndid en vbovkbg')
    print('\t\tSleutel van dit record is vboid, pndid, vbovkbg')
    (nrec, nkey) = baglib.df_comp(vbopnd_bg_eg, key_lst=['vboid', 'pndid'])


    vbopnd_df = pd.merge(vbopnd_bg_eg, bd['vbo'][['vboid', 'vbovkid', 'vbovkbg']],
                         how='inner', on='vboid')
    print('\t\tAantal records moet grofweg verdubbelen: "van vbo naar vbovk" ')
    (nrec, nkey) = baglib.df_comp(vbopnd_df, key_lst=['vboid', 'pndid', 'vbovkbg'],
                                  nrec=nrec, nkey=nkey, u_may_change=True)
    

    print('\n\t\t2d. Concateneer de beide kolommen met pd.concat')
    print('\t\tHet aantal records is de som van 2a en 2c')
    vbopnd_df = pd.concat([vbopnd_df, pnd_df])
    (nrec, nkey) = baglib.df_comp(vbopnd_df, key_lst=['vboid', 'pndid', 'vbovkbg'],
                                  nrec=nrec, nkey=nkey, u_may_change=True)
    # print('\t\tVanaf nu moet het aantal vk constant blijven:', nkey)
    
    '''
    '''
    print('\n\t\t2b. Verwijder de records waar het pndvkbg eerder begint dan',
          'de vbopnd_bg')
    vbopnd_df = vbopnd_df[vbopnd_df['pndvkbg'] >= vbopnd_df['vbopnd_bg']].drop(['vbopnd_bg', 'vbopnd_eg'], axis=1)
    # .drop('vbopnd_bg', axis=1)
    (nrec, nkey) = baglib.df_comp(vbopnd_df, key_lst=['vboid', 'pndid', 'pndvkbg'],
                                  nrec=nrec, nkey=nkey, u_may_change=True)
    '''
    '''
    # print(vbopnd_df.sort_values(by=['vboid', 'pndid', 'vbovkbg']).head(30))

    print('\n\t\t2f. We gaan in eerst de nans van de pndvkid opvullen en daarna\n',
          '\t\tde nans van de vbovkid. Beide met ffill')
    # print('\n\t\t\tDe sortering luistert nauw:')
    # https://stackoverflow.com/questions/27012151/forward-fill-specific-columns-in-pandas-dataframe

    baglib.debugprint(df=vbopnd_df,
                      cols=['pndid', 'vbovkid', 'pndvkid', 'vbovkbg'],
                      colname='vboid', val='0762010000011444', 
                      sortlist=['vboid', 'pndid', 'vbovkbg'], debuglevel=DEBUGLEVEL)

    
    print('\n\t\t\t2f1. Sorteer op pndid, vbovkbg, pndvkid (nan last) waarna',
          '\n\t\t\tje met ffill de nans kunt opvullen van de pndvkid')
    cols = ['vboid', 'pndid', 'vbovkbg', 'pndvkid']
    vbopnd_df = vbopnd_df.sort_values(by=cols, na_position='last')
    vbopnd_df.loc[:,'pndvkid'] = vbopnd_df.loc[:,'pndvkid'].ffill().astype({'pndvkid':int})

    # print(vbopnd_df.head(30))
    baglib.debugprint(df=vbopnd_df,
                      cols=['pndid', 'vbovkid', 'pndvkid', 'vbovkbg'],
                      colname='vboid', val='0762010000011444', 
                      sortlist=['vboid', 'pndid', 'vbovkbg'], debuglevel=DEBUGLEVEL)

    print('\n\t\t\t2f2. Sorteer op vboid, vbovkbg, vbovkid, nan last, waarna',
          '\n\t\t\tje met ffill de nans kunt opvullen.\n',
          '\t\t\tVerwijder daarna de dubbele records')
    cols = ['vboid', 'pndid', 'vbovkbg', 'vbovkid']
    vbopnd_df = vbopnd_df.sort_values(by=cols, na_position='last')
    vbopnd_df.loc[:,'vbovkid'] = vbopnd_df.loc[:,'vbovkid'].ffill().astype({'vbovkid':int})
    vbopnd_df.drop_duplicates(inplace=True)
    (nrec, nkey) = baglib.df_comp(vbopnd_df, key_lst=['vboid', 'pndid', 'vbovkbg'],
                                  nrec=nrec, nkey=nkey, u_may_change=True)

    # print(vbopnd_df.head(30))
    baglib.debugprint(df=vbopnd_df,
                      cols=['pndid', 'vbovkid', 'pndvkid', 'vbovkbg'],
                      colname='vboid', val='0762010000011444', 
                      sortlist=['vboid', 'pndid', 'vbovkbg'], debuglevel=DEBUGLEVEL)
 
    print('\n\t\t2g. Verwijder de records waar het vbovkbg eerder begint dan',
          'de vbopnd_bg')
    vbopnd_df = vbopnd_df[vbopnd_df['vbovkbg'] >= vbopnd_df['vbopnd_bg']].drop(['vbopnd_bg', 'vbopnd_eg'], axis=1)
    print('\t\tAantal records neemt beetje af als pnd of vbo eerder begint dan de combinatie vbopnd')
    (nrec, nkey) = baglib.df_comp(vbopnd_df, key_lst=['vboid', 'vboid', 'vbovkbg'],
                                  nrec=nrec, nkey=nkey, u_may_change=True)

    baglib.debugprint(df=vbopnd_df,
                      cols=['pndid', 'vbovkid', 'pndvkid', 'vbovkbg'],
                      colname='vboid', val='0762010000011444', 
                      sortlist=['vboid', 'pndid', 'vbovkbg'], debuglevel=DEBUGLEVEL+10)









    print('\n\t\t6 Maak een nieuwe teller voor vbopnd om deze te kunnen\n',
          '\t\tonderscheiden van de bestaande vbovkid')
    
    vbopnd_df = vbopnd_df.sort_values(by=['vboid', 'vbovkbg'])
    vbopnd_df = baglib.makecounter(vbopnd_df[['vboid', 'vbovkbg']].drop_duplicates(),
                                   grouper='vboid', 
                                   newname='vbovkid2', sortlist=['vboid', 'vbovkbg'])
    (nrec, nkey) = baglib.df_comp(vbopnd_df, key_lst=['vboid', 'vbovkid2'],
                                  nrec=nrec, nkey=nkey,
                                  u_may_change=False)


    

    baglib.debugprint(df=vbopnd_df,
                      cols=[],
                      colname='vboid', val='0762010000011444', 
                      sortlist=['vbovkbg', 'vboid'], debuglevel=DEBUGLEVEL+10)

    '''
    '''
    # print(_df.head(50))
    print('DEBUG')
    tmp_df = _df.groupby([dfid, dfvkbg]).size().to_frame('aa')
    print(tmp_df.sort_values(by='aa', ascending=False).head(30))

    print('DEBUG')
    tmp2_df = _df.groupby([dfid, dfvkid2]).size().to_frame('aa')
    print(tmp2_df.sort_values(by='aa', ascending=False).head(30))

    
    print('\n\t\t7 Voeg', dfvkeg, 'toe in twee stappen:')
    print('\t\t\t7a. neem de', dfvkbg, 'van het volgende record')
    _df[dfvkeg] = _df[dfvkbg].shift(periods=-1)
    (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkid2],
                             nrec=_nrec, nkey=_nkey,
                             u_may_change=False)

    print('\t\t\t7b. corrigeer de vbovkeg van het meest recente voorkomen')
    print('\t\t\tDit krijgt een datum in de toekomst:', future_date)
    idx = _df.groupby([dfid])[dfvkid2].transform(max) == _df[dfvkid2]
    _df.loc[idx, dfvkeg] = future_date
    (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkid2],
                             nrec=_nrec, nkey=_nkey,
                             u_may_change=False)

    print('\n\t\t8 Terugcasten naar integer van', dfvkeg)    
    _df = _df.astype({dfvkeg: np.uintc})
    (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkid2],
                             nrec=_nrec, nkey=_nkey,
                             u_may_change=False)
    # print(vbo_df.head(30))
 
    print('\n\t\t ----- Perc toegenomen voorkomens:', 
          round(100 * (_nkey/_nkey1 - 1), 1), '%')
    
    # print(bd['vbo'].head(30))
    # print('\n\n\t\t --------- aantal records in bd[vbo]: ', bd['vbo'].shape[0])
    # print(vbo_df.head(30))

    '''










    '''    

4. Als een vbopnd_eg < future_date, beeindig dan het vbovk in vbopnd
    future_date is een vaste dag in de verre toekomst


    # #############################################################################
    print('\n--2. Aanmaken vbovk zodat een vbovk volledig binnen pndvk past----\n')
    # #############################################################################
    
    '''    
        
    '''
        print('\t\t------- Start vksplitter; fijn:', fijntype, 'grof:', groftype)   
        # de volgende kolommen moeten bestaan voor fijntype en groftype:
        dfid = fijntype + 'id'
        dfvkbg = fijntype + 'vkbg'
        dfvkeg = fijntype + 'vkeg'
        dfvkid = fijntype + 'vkid'
        gfid = groftype + 'id'
        gfvkbg = groftype + 'vkbg'
        gfvkid = groftype + 'vkid'
        
        # deze kolom gaan we maken om de nieuwe vk te identificeren:
        dfvkid2 = fijntype + 'vkid2'
        # aantallen in de uitgangssituatie     
        (_nrec1, _nkey1) = df_comp(df, key_lst=[dfid, dfvkid])

        print('\t\t1a. We willen geen nieuwe vk voor de\n',
              '\t\tbegindatum van het eerste vk van een df. Daarom bepalen we de\n',
              '\t\tstartdd van elk', dfid)
        dfid_startdd = df.groupby(dfid)[dfvkbg].min().to_frame().\
            rename(columns={dfvkbg: 'startdd'}).reset_index()
        # print(dfid_startdd)

        print('\n\t\t1b Neem daarna de unieke', dfid,'-', gfid, 'uit',
              fijntype +'_df')
        _df = df[[dfid, gfid]].drop_duplicates()
        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid])
        
        print('\t\tAantal records en aantal unieke', dfid, ':', _nrec, _nkey)
        
        
        print('\n\t\t1c Voeg de startdd uit 1a. aan het', dfvkid, 'toe')
        _df = pd.merge(_df, dfid_startdd, left_on=dfid, right_on=dfid)

        print('\n\t\t2a. Voeg daarna de', gfvkbg, 'aan het', dfvkid, 'toe')
        print('\t\t\tEenheid van zo een record is nu', dfid,'+', gfvkbg,'!')
        # print(vbo_df.info())
        # print(bd['pnd'].info())
        _df = pd.merge(_df,
                       # gf[[gfid, gfvkbg]])[[dfid, gfvkbg, 'startdd']]
                       gf[[gfid, gfvkid, gfvkbg]])[[dfid, gfvkbg, gfvkid, 'startdd']]
        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, gfvkbg],
                                 nrec=_nrec,
                                 nkey=_nkey,
                                 u_may_change=True)


        print('\n\t\t2b. Verwijder cf. het uitgangspunt de records waar het', gfid,
              '\n\t\teerder begint dan de startdd van het', dfid)
        # _df = _df[_df[gfvkbg] >= _df['startdd']][[dfid, gfvkbg]]
        _df = _df[_df[gfvkbg] >= _df['startdd']].drop('startdd', axis=1)
        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, gfvkbg],
                                 nrec=_nrec,
                                 nkey=_nkey,
                                 u_may_change=True)
        # print(_df.head(40))
        # print(_df.info())
        print('\n\t\t3 Hernoem de kolom', gfvkbg, 'naar', dfvkbg, 'zodat we de',
              '\n\t\tkolommen van', dfid, 'en', gfid, 'kunnen concateneren')
        _df.rename({gfvkbg: dfvkbg}, axis='columns', inplace=True)
        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkbg],
                                 nrec=_nrec,
                                 nkey=_nkey,
                                 u_may_change=True)
        # print(_df.head(40))
        # concat so that all dfvkbg are in one frame
        print('\n\t\t4 Concateneer de beide kolommen met pd.concat')
        print('\t\tHet aantal records wordt meer dan verdubbeld want er\n',
              '\t\tzijn meer', fijntype, 'vk dan', groftype, 'vk')
        _df = pd.concat([df[[dfid, dfvkid, dfvkbg]], _df])
        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkbg],
                                 nrec=_nrec,
                                 nkey=_nkey,
                                 u_may_change=True)
        print('\t\tVanaf nu moet het aantal vk constant blijven:', _nkey)


        # print(_df.sort_values(by=[dfid, dfvkbg]).head(30))
        
        print('\n\t\t5. We gaan in eerst de nans van de', dfvkid, 'en daarna\n',
              '\t\tde nans van de', gfvkid, 'opvullen. Beide met ffill')
        print('\n\t\t\tDe sortering luistert nauw:')
        # https://stackoverflow.com/questions/27012151/forward-fill-specific-columns-in-pandas-dataframe
        print('DEBUG')
        print(_df[_df[dfid]=='0762010000011444'].sort_values(by=dfvkbg))
        
        print('\n\t\t\t5a. Sorteer op', dfid, dfvkid, '(nan last), waarna je met\n',
              '\t\t\tffill de nans kunt opvullen van de', dfvkid)
        cols = [dfid, dfvkbg, dfvkid]
        _df = _df.sort_values(by=cols, na_position='last')
        _df.loc[:,dfvkid] = _df.loc[:,dfvkid].ffill().astype({dfvkid:int})

        # print(_df.head(30))
        print('DEBUG')
        print(_df[_df[dfid]=='0762010000011444'].sort_values(by=dfvkbg))

        print('\n\t\t\t5b. Sorteer op', dfid, gfvkid, 'nan last, waarna je met\n',
              '\t\t\tffill de nans kunt opvullen van het', gfvkid, '.\n',
              '\t\t\tVerwijder daarna de dubbele records')
        print('Issue: als het eerste vbo afgesloten is voor het begin van het eerste pndvk, kun je de nan niet ffill en... ')
        cols = [dfid, dfvkbg, gfvkid]
        _df = _df.sort_values(by=cols, na_position='last')
        _df.loc[:,gfvkid] = _df.loc[:,gfvkid].ffill().astype({gfvkid:int})
        _df.drop_duplicates(inplace=True)

        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkbg],
                                 nrec=_nrec, nkey=_nkey,
                                 u_may_change=False)
        # print(vbo_df.head(30))
        # print(_df.head(30))
        print('DEBUG')
        print(_df[_df[dfid]=='0762010000011444'].sort_values(by=dfvkbg))
        
        
        print('\n\t\t6 Maak een nieuwe teller voor', dfvkid2, 
              '\n\t\tom deze te kunnen onderscheiden van de bestaande', dfvkid)
        _df = makecounter(_df, grouper=dfid, newname=dfvkid2)
        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkid2],
        # (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkbg],
                                 nrec=_nrec, nkey=_nkey,
                                 u_may_change=False)

        print('DEBUG')
        print(_df[_df[dfid]=='0762010000011444'].sort_values(by=dfvkbg))

        # print(_df.head(50))
        print('DEBUG')
        tmp_df = _df.groupby([dfid, dfvkbg]).size().to_frame('aa')
        print(tmp_df.sort_values(by='aa', ascending=False).head(30))

        print('DEBUG')
        tmp2_df = _df.groupby([dfid, dfvkid2]).size().to_frame('aa')
        print(tmp2_df.sort_values(by='aa', ascending=False).head(30))

        
        print('\n\t\t7 Voeg', dfvkeg, 'toe in twee stappen:')
        print('\t\t\t7a. neem de', dfvkbg, 'van het volgende record')
        _df[dfvkeg] = _df[dfvkbg].shift(periods=-1)
        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkid2],
                                 nrec=_nrec, nkey=_nkey,
                                 u_may_change=False)

        print('\t\t\t7b. corrigeer de vbovkeg van het meest recente voorkomen')
        print('\t\t\tDit krijgt een datum in de toekomst:', future_date)
        idx = _df.groupby([dfid])[dfvkid2].transform(max) == _df[dfvkid2]
        _df.loc[idx, dfvkeg] = future_date
        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkid2],
                                 nrec=_nrec, nkey=_nkey,
                                 u_may_change=False)

        print('\n\t\t8 Terugcasten naar integer van', dfvkeg)    
        _df = _df.astype({dfvkeg: np.uintc})
        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkid2],
                                 nrec=_nrec, nkey=_nkey,
                                 u_may_change=False)
        # print(vbo_df.head(30))
     
        print('\n\t\t ----- Perc toegenomen voorkomens:', 
              round(100 * (_nkey/_nkey1 - 1), 1), '%')
        
        # print(bd['vbo'].head(30))
        # print('\n\n\t\t --------- aantal records in bd[vbo]: ', bd['vbo'].shape[0])
        # print(vbo_df.head(30))

        # print('\n\t\t9 Imputeren nieuwe vk nog zonder gfid (pndid)... ')
        print('\n\t\t9. Toevoegen van de kolommen uit', dfid,
              'die we nog misten')
        # cols = ['vboid', 'vbovkid', 'vbostatus', 'vbogmlx', 'vbogmly']
        # print(_df.info())
        
        _df = pd.merge(_df,
                       df.drop([gfid, dfvkbg, dfvkeg], axis=1).drop_duplicates(),
                       how='inner',
                       left_on=[dfid, dfvkid], right_on=[dfid, dfvkid])
        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkid2],
                                 nrec=_nrec, nkey=_nkey,
                                 u_may_change=False)
        # cols = ['vboid', 'vbovkid', 'vbovkid2', 'vbovkbg']
        # print(vbo_df[cols].head(30))
        # print(_df.info())
        
        vk = [dfid, dfvkid]
        
        print('\t\t2.10 Voeg gfid (pndid) nog toe')
        print('\t\t Hierna is dfid, vkid2 (vboid, vbovk2) niet meer uniek vanwege dubbele gfid (pndn)',
        'bij 1 dfid (vbo)')
        _df = pd.merge(_df,
                       df[[dfid, dfvkid, gfid]],
                       how='inner',
                       left_on=vk, right_on=vk)
        (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkid2],
                                 nrec=_nrec,
                                 nkey=_nkey,
                                 u_may_change=True)
        # print(bd['vbo'].head(30))
        print('\t\t------- Einde functie vksplitter -------')
        return _df
    
    bd['vbo'] = baglib.vksplitter(df=bd['vbo'], gf=bd['pnd'], 
                                  fijntype ='vbo', groftype = 'pnd', 
                                  future_date = FUTURE_DATE)

    
    print('\n\t\tEind stap 2: Schakel over bij vk identificatie naar vbovkid2')
    vbovk = ['vboid', 'vbovkid2']
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk}


    print(bd['vbo'].info())
    
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
                         on=pndvk)
    del bd['pnd']
    
    print(bd['vbo'].info())
    # (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], key_lst=vbovk,
    #                                             nrec=nrec['vbo'], nkey=nkey['vbo'], 
    #                                             u_may_change=False)
    
    
    bd['vbo'].set_index(vbovk, inplace=True)
    
    # zuinig met geheugen: Sommige types kunnen teruggecast worden.
    bd['vbo'] = baglib.recast_df_floats(bd['vbo'], BAG_TYPE_DICT)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    
    # toc = time.perf_counter()
    # baglib.print_time(toc - tic, 'gebruikte tijd:', printit)

    doel2_vbovk_u = nkey['vbo']
    '''
    '''
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
    '''
    
    # #############################################################################
    print('\n----7. Bewaren in koppelvlak3: vbovk -> hoofdpndvk met',
          doel2_vbovk_u, 'records...')
    # #############################################################################
    # tic = time.perf_counter()
    
    if doel2_vbovk_u > 20000000:
        print('Dit gaat even duren...')
        
    outputfile = OUTPUTDIR + 'vbovk_pndvk.csv'
    
    # vbovk_hoofdpndvk_df = vbovk_prio_df[['vboid', 'vbovkid', 'pndid', 'pndvkid']]
    # vbovk_hoofdpndvk_df.sort_values(['vboid', 'vbovkid']).to_csv(outputfile,
    #                                                            index=False)
    cols = ['vbovkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'pndid', 'pndvkid', 'pndstatus']
    bd['vbo'][cols].sort_index().to_csv(outputfile, index=True)
    
    toc = time.perf_counter()
    # print('\t\ttictoc - saving vbo-pnd file in', toc - tic, 'seconds')
    baglib.print_time(toc - tic, 'vbovk2_pndvk duurde', printit)

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
       
# ########################################################################
print('------------- Start bag_splits_vbovk lokaal ------------- \n')
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

    bag_splits_vbovk(current_month=current_month,
                     koppelvlak2=DIR02,
                     koppelvlak3=DIR03,
                     loglevel=printit)


