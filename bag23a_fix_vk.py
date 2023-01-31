#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Zaterdag 23 dec 2022

In de BAG wordt de levenloop van een bag object (bob) gedefinieerd door een 
rijtje opeenvolgende en aaneensluitende voorkomens (vk) met een begindatum 
geldigheid (vkbg) en een einddatum geldigheid (vkeg).

Een voorkomen van een bag object kan gekoppeld zijn aan een ander bag 
object. Als voorbeeld:

Een vbovk (verblijfsobject voorkomen) wordt aan een (of meerdere) pnd (pand) 
of num (nummeraanduiding) gelinkt. 

Dat is feitelijk onzorgvuldig omdat tijdens 
de looptijd van een vbovk zaken kunnen veranderen.

Doel: Repareer de voorkomens van de 7 BAG objecten zodat bij een wezenlijke 
verandering van het object ook daadwerkelijk een nieuw voorkomen van dat
object gemaakt wordt. 

Koppel hierna voorkomens aan voorkomens van het bovenliggende bagobject
in plaats van plaats van voorkomens aan het bag object zelf.

Voer daarnaast enkele andere reparatie werkzaamheden aan vk uit
zoals in onderstaande stappen staat beschreven:

Stappen:
    Stap 0: Inlezen
    Stap 1: Verwijder eendagsvliegen (vk die binnen een dag wijzigen)
    Stap 2: Ontdubbel: verwijder vk als er feitelijk niets wijzigt tussen twee
        opeenvolgende vk
    Stap 3: Numsplits: splits vk als er iets wijzigt met het gekoppelde Bag 
    object (bob). Doe dit voor 
            gem - prv (todo: gemeente - provincie)
            wpl - gem (woonplaats)
            opr - wpl (openbare ruimte)
            num - opr (nummeraanduiding)
            vbo - num (verblijfsobject)
    Stap 4: Pndsplits: vbo - pnd (nogmaal voor vbo, maar nu met pand)

Resultaat:
    voor elk van bovenstaande 6 relaties koppelt een "onderliggend" vk aan een
    "bovenliggend" vk. We noemen dit het fijntype en het groftype omdat het
    fijntype qua looptijd altijd volledig binnen het groftype valt. Het 
    resultaat heet ook wel een busbestand.
    
Voorbeeld voor vbo:
    voor vbo onderscheiden we 6 soorten wijzigingen die een nieuw vk tot gevolg
    kunnen hebben van dat vbo:
    
    1. Een pnd van dat vbo krijgt een nieuw vk
    2. Een pnd van dat vbo wordt afgesloten en krijgt geen nieuw vk (todo)
    3. De nummeraanduiding (num) van dat vbo krijgt een nieuw vk
    4. De openbare ruimte (opr) van die num  krijgt een nieuw vk
    5. De woonplaats (wpl) van die opr krijgt een nieuw vk
    6. De gemeente (gem) van die wpl krijgt een nieuw vk (todo)

Waarom doen we dit:
    Als voorbeeld van 6. Als een vbo in een nieuwe gemeente komt te liggen, 
    heeft dat impact op de statistische output. Om een consistent bus bestand 
    van vbo te krijgen moet je dan een nieuw vbovk aanmaken (tenzij op die datum
    al een nieuw vk begint)
    
Keuzen die we hierbij maken:
    1. een vbo kan niet zonder pnd bestaan en een pnd niet zonder vbo
    2. een vbo kan niet zonder num, kan niet zonder opr kan niet zonder
        wpl kan niet zonder gemeente bestaan.

Plaatjes vbo met twee panden:
    het vbo begint met 4 vk en eindigt met 7:

vbovk        1            2            3            4
vbo1  o-----------oo-----------oo-----------oo---------->
pnd1       o-----------oo------------------------------->
pnd2                       o-----------o

wordt

vbovk2         1     2   3  4      5     6      7
vbo1       o------oo---oo-oo---oo-----------oo---------->
pnd1       o-----------oo------------------------------->
pnd2                       o-----------o

Ook als het pnd eindigt kan dat gevolgen hebben voor het vbo vk (zie het eind
van pnd2. Dit is nog niet geimplementeerd.

"""

# ################ import libraries ###############################
import pandas as pd
# import numpy as np
import sys
# import os
import time
import baglib
from baglib import BAG_TYPE_DICT
from config import * 



# ############### Define functions ################################

def bag_fix_vk(loglevel = 10,
               current_month='testdata23',
               koppelvlak2=os.path.join('..', 'data', '02-csv'),
               koppelvlak3=os.path.join('..', 'data', '03-bewerktedata'),
               future_date=FUTURE_DATE):

    tic = time.perf_counter()
    ll = loglevel
    baglib.printkop(ll+40, 'Start bag_fix_vk' + str(current_month))

    # #############################################################################
    # print('00.............Initializing variables...............................')
    # #############################################################################
    # month and dirs
    INPUTDIR = os.path.join(koppelvlak2, current_month)
    K2DIR = INPUTDIR
    OUTPUTDIR = os.path.join(koppelvlak3, current_month)
    baglib.make_dir(OUTPUTDIR)


    if current_month == 'testdata23':
        current_year = 2000
    else:
        current_month = int(current_month)
        # current_year = int(current_month/100)
    
    pd.set_option('display.max_columns', 20)
    
    INPUT_FILES_DICT = {'vbo': os.path.join(K2DIR, 'vbo.csv'),
                        'pnd': os.path.join(K2DIR, 'pnd.csv'),
                        'num': os.path.join(K2DIR, 'num.csv'),
                        'opr': os.path.join(K2DIR, 'opr.csv'),
                        'wpl': os.path.join(K2DIR, 'wpl.csv')}
                        # 'gem': K2DIR + 'gem.csv'}
        
                       # 'wplgem': K2DIR + 'wplgem.csv'}
        
    vbovk = ['vboid', 'vbovkid']
    pndvk = ['pndid', 'pndvkid']
    numvk = ['numid', 'numvkid']
    oprvk = ['oprid', 'oprvkid']
    wplvk = ['wplid', 'wplvkid']
    gemvk = ['gemid', 'gemvkid']
    # wplgemvk = ['wplid', 'wplgemvkid']        
    
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk,
                'num': numvk,
                'opr': oprvk,
                'wpl': wplvk}
                # 'gem': gemvk}
    
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    nrec = {}
    nkey = {}
    nrec_listdict = []
    nkey_listdict = []
    stappenteller = 0
    # if printit = True bepaal-hoofdpnd prints extra info
    # printit = True
    
    TEST_D = {'gemid': ['0457', '0363', '0003'],
              'wplid': ['3386', '1012', '3631'],
              'oprid': ['0457300000000259', '0457300000000260', '0003300000116985'],
              # 'numid': ['1979200000000546', '0457200000521759', '0457200000521256'],
              # 'numid': ['0388200000212289'],
              'numid': ['0003200000136934'],
              'vboid': ['1714010000784185'],
              # 'vboid': ['0007010000000192'],
              # 'pndid': ['0388100000202416', '0388100000231732', '0388100000232080', '0388100000232081']
              'pndid': ['0003100000117987']}

    print('\n---------------DOELSTELLING--------------------------------')
    print('Doel: Repareer de voorkomens van de 7 BAG objecten zodat bij een wezenlijke\n',
          'verandering van het object ook daadwerkelijk een nieuw voorkomen van dat\n',
          'object gemaakt wordt. Hierdoor kun je voorkomens aan voorkomens koppelen in\n',
          'plaats van voorkomens aan een (compleet) BAG object.')
    print('Voer daarnaast enkele andere reparatie werkzaamheden aan vk s van bag objecten (bob)')
    print('-----------------------------------------------------------')

    # #########################################################################
    baglib.aprint(ll+30, '\n----', stappenteller, 'Inlezen van de inputbestanden-----------------------\n')
    # #########################################################################
    
    # print('huidige maand (verslagmaand + 1):', current_month)
    
    bd = baglib.read_input_csv(ll, INPUT_FILES_DICT, BAG_TYPE_DICT)

    for bob, vk in KEY_DICT.items():
        (nrec[bob], nkey[bob]) = baglib.df_comp(ll+20, bd[bob], key_lst=vk)

    nrec_listdict.append(nrec.copy())
    nkey_listdict.append(nkey.copy())
    stappenteller +=1
    
    # #############################################################################
    baglib.aprint(ll+30, '\n----', stappenteller, 'Verwijder eendagsvliegen -----------------------\n')
    # #############################################################################
    

    def pperc(ll, t, n):
        baglib.aprint(ll, round((t/n) * 100, 1))

    for bob, vk in KEY_DICT.items():
        baglib.aprint(ll+10, '\tVerwijder eendagsvliegen bij', bob)
        bd[bob] = baglib.fix_eendagsvlieg(bd[bob], bob+'vkbg', bob+'vkeg')
        baglib.aprint(ll, '\t\tCheck: zitten er dubbele vk in', bob+':')
        baglib.aprint(ll, baglib.find_double_vk(bd[bob], bob+'id', bob+'vkid').head(2))
        baglib.aprint(ll, '')

    for bob, vk in KEY_DICT.items():
        (nrec[bob], nkey[bob]) = baglib.df_comp(ll+20, bd[bob], key_lst=vk)
        dfid = bob + 'id'
        baglib.debugprint(title='\t\tNa het verwijderen van de eendagsvliegen:',
                          df=bd[bob], colname=dfid,
                          vals=TEST_D[dfid], loglevel=loglevel)

    nrec_listdict.append(nrec.copy())
    nkey_listdict.append(nkey.copy())
    stappenteller +=1

    baglib.debugprint(loglevel=ll+10, title='pnd eendagsvliegen verwijderd na stap '+str(stappenteller), 
                      df=bd['pnd'], colname='pndid', vals=TEST_D['pndid'])
    

    # #############################################################################
    baglib.aprint(ll+30, '\n----', stappenteller, 'Neem opeenvolgende gelijke vk samen------------------\n')
    # #############################################################################

    # relevant cols except id, vkid, vkbg
    relevant_cols = {# 'vbo': ['vbostatus', 'numid', 'oppervlakte' , 'pndid', 'woon', 'gezo' ,'indu', 'over', 'ondr', 'logi' ,'kant', 'wink', 'bij1' , 'celf' , 'sprt' , 'vbogmlx' , 'vbogmly'],
                     'pnd': ['pndstatus', 'bouwjaar', 'docnr', 'docdd', 'pndgmlx', 'pndgmly'],
                     'num': ['numstatus', 'huisnr', 'postcode', 'typeao', 'oprid'],
                     'opr': ['oprstatus', 'oprnaam', 'oprtype', 'wplid'],
                     'wpl': ['wplstatus', 'gemid']}
   
    for bob in relevant_cols.keys():
        dfid = bob + 'id'
        dfvkid = bob + 'vkid'
        dfvkid2 = bob + 'vkid2'
        dfvkid_oud = bob + 'vkid_oud'
        bd[bob] = merge_vk(ll, bd[bob], bob, FUTURE_DATE, relevant_cols[bob])
        bd[bob].rename(columns={dfvkid: dfvkid_oud, 
                                dfvkid2: dfvkid}, inplace=True)

        baglib.debugprint(title='\t\tResult merge_vk example:',
                          df=bd[bob], colname=dfid,
                          vals=TEST_D[dfid], loglevel=loglevel)

    for bob, vk in KEY_DICT.items():
        (nrec[bob], nkey[bob]) = baglib.df_comp(ll+20, bd[bob], key_lst=vk)
    nrec_listdict.append(nrec.copy())
    nkey_listdict.append(nkey.copy())
    stappenteller +=1


    # #############################################################################
    baglib.aprint(ll+30, '\n----', stappenteller, 'Roep vksplitter aan voor de bag objecten---------------\n')
    # #############################################################################

    
    bd['opr'] = vksplitter(df=bd['opr'], gf=bd['wpl'],
                           fijntype='opr', groftype='wpl',
                           future_date=FUTURE_DATE,
                           test_d=TEST_D, loglevel=loglevel)

    cols = ['oprid', 'oprvkid']
    baglib.debugprint(title='Testje na vksplitter',
                      df=bd['opr'], colname='oprid', vals=TEST_D['oprid'], sort_on=cols, 
                      loglevel=loglevel)
    
    bd['num'] = vksplitter(df=bd['num'], gf=bd['opr'],
                           fijntype='num', groftype='opr',
                           future_date=FUTURE_DATE,
                           test_d=TEST_D, loglevel=loglevel)
   
    baglib.debugprint(title='input voor vksplitter vbo-num relatie',
                      df=bd['vbo'], colname='vboid',
                      vals=TEST_D['vboid'], loglevel=ll)

    bd['vbo'] = vksplitter(df=bd['vbo'], gf=bd['num'],
                           fijntype='vbo', groftype='num',
                           future_date=FUTURE_DATE,
                           test_d=TEST_D, loglevel=loglevel)

    # (nrec3a, nkey3a) = baglib.df_comp(ll+20, bd['vbo'], key_lst=vbovk) 
    # rename column _oud so it won't get confused when we invoke vksplitter 
    # for vbo for the second time, but now with pnd as groftype
    bd['vbo'].rename(columns={'vbovkid_oud': 'vbovkid_org'}, inplace=True)
    
    
    for bob, vk in KEY_DICT.items():
        (nrec[bob], nkey[bob]) = baglib.df_comp(ll+20, bd[bob], key_lst=vk)
    nrec_listdict.append(nrec.copy())
    nkey_listdict.append(nkey.copy())
    stappenteller +=1
    
    
    baglib.debugprint(loglevel=ll+10, title='pndvk input voor stap '+str(stappenteller), 
                      df=bd['pnd'], colname='pndid', vals=TEST_D['pndid'])


    cols = ['vboid', 'vbovkid', 'vbovkid_org', 'vbovkbg', 'vbovkeg', 'numid', 'numvkid', 'pndid']
    baglib.debugprint(title='input voor vksplitter vbo-pnd relatie',
                      df=bd['vbo'][cols], colname='vboid',
                      vals=TEST_D['vboid'], loglevel=ll)



    bd['vbo'] = vksplitter(df=bd['vbo'], gf=bd['pnd'],
                           fijntype='vbo', groftype='pnd',
                           future_date=FUTURE_DATE,
                           test_d=TEST_D, loglevel=loglevel)

    cols = ['vboid', 'vbovkid', 'vbovkid_org', 'vbovkbg', 'vbovkeg', 'numid', 'numvkid', 'pndid', 'pndvkid']
    baglib.debugprint(title='output vksplitter na de vbo-pnd relatie',
                      df=bd['vbo'][cols], colname='vboid',
                      vals=TEST_D['vboid'], loglevel=ll)


    bd['vbo'].drop(columns='vbovkid_oud', inplace=True)    
    # print(bd['vbo'].info())
    baglib.debugprint(loglevel=ll+10, title='vbovk gekoppeld aan pndvk na stap '+str(stappenteller), 
                      df=bd['vbo'], colname='pndid', vals=TEST_D['pndid'])
    
    
    for bob, vk in KEY_DICT.items():
        (nrec[bob], nkey[bob]) = baglib.df_comp(ll+20, bd[bob], key_lst=vk)
    nrec_listdict.append(nrec.copy())
    nkey_listdict.append(nkey.copy())
    stappenteller +=1

    
    baglib.debugprint(title='Resultaat voor voorbeeld vbo:',
               df=bd['vbo'], colname='vboid', vals=TEST_D['vboid'], sort_on=['vboid', 'vbovkid'], 
               loglevel=20)
    
    
    # zuinig met geheugen: Sommige types kunnen teruggecast worden.
    bd['vbo'] = baglib.recast_df_floats(bd['vbo'], BAG_TYPE_DICT)
    

     
    # #############################################################################
    baglib.aprint(ll+30, '\n-----Bewaren in koppelvlak3 -------------------')
    # #############################################################################
    # tic = time.perf_counter()
 
    for bob, vk in KEY_DICT.items():
        baglib.aprint(ll+20, '\tBewaren koppelbaar gemaakte', bob+'.csv')
        outputfile = OUTPUTDIR + bob+'.csv'
    
        # cols = ['vbovkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'pndid', 'pndvkid', 'numid', 'numvkid']
        bd[bob].sort_values(by=[bob+'id', bob+'vkid']).to_csv(outputfile, index=False)
    

    baglib.aprint(ll+30, '\n------ Aantallen voorkomens na de stappen:')

    df = pd.DataFrame(nkey_listdict)
    df = df.div(df.iloc[0]).round(2)
    baglib.aprint(ll+30, df)
    outputfile = OUTPUTDIR + 'log_'+str(current_month)+'.csv'
    df.to_csv(outputfile)

    toc = time.perf_counter()
    baglib.aprint(ll+40, '\n*** Einde bag_fix_vk in', (toc - tic)/60, 'min ***\n')

    
    
def vksplitter(loglevel=10,
               df='vbo_df', gf='pnd_df', fijntype='vbo', groftype='pnd', 
               future_date=FUTURE_DATE, test_d={}):
    
    '''Splits voorkomens (vk) voor df, zodat elk vk van df in een gf vk past.
    df is het "fijntype" en gf is het "groftype".
    
    dfid identificeert de eenheden van df, bijvoorbeeld vbo, 
    waarvan de vk als records in df staan. gfid idem voor gf (pnd)

    input: beschouw twee vk van een pand (gfid) en 1 vk van een vbo (dfid)
    gfid:            o------------------oo-------------o
    dfid:                   o------------------o

    output: het vbovk wordt gesplitst zodat het binnen een pnd vk valt
    gfid:            o------------------oo-------------o
    dfid:                   o-----------oo-------o
    
    dfvkbg is de kolom met de vk begindatums van df.
    gfvkbg idem voor gf.
    
    We gaan dit aanpakken door de begindatums van de vk (dfvkbg en gfvkbg)
    in 'e'en kolom te verzamelen en dit te sorteren en ontdubbelen per dfid
    
    uitgangspunt is om geen nieuwe vk aan te maken v'o'or het eerste vk 
    van dfid.
 
    Voorwaarden: de kolommen in de volgende 7 assignments moeten bestaan
    in de dataframes df en gf:
    
 '''
    ll = loglevel
    baglib.aprint(ll+30, '\n-----------------------------------------------------------')   
    baglib.aprint(ll+30, '------ Start vksplitter; fijntype:', fijntype, ', groftype:', 
          groftype)   
    baglib.aprint(ll+30, '-----------------------------------------------------------\n')   

    # de volgende kolommen moeten bestaan voor fijntype en groftype:
    dfid = fijntype + 'id'
    dfvkbg = fijntype + 'vkbg'
    dfvkeg = fijntype + 'vkeg'
    dfvkid = fijntype + 'vkid'
    gfid = groftype + 'id'
    gfvkbg = groftype + 'vkbg'
    gfvkeg = groftype + 'vkeg'
    gfvkid = groftype + 'vkid'
    dfgf_bg = fijntype + groftype + '_bg'
    dfgf_eg = fijntype + groftype + '_eg'
    relatie = fijntype+'-'+groftype
    
    # deze kolom gaan we maken om de nieuwe vk te identificeren:
    dfvkid2 = fijntype + 'vkid2'

    # aantallen in de uitgangssituatie     
    (_nrec1, _nkey1) = baglib.df_comp(loglevel=ll+20, df=df, key_lst=[dfid, dfvkid])

    # #############################################################################
    baglib.aprint(ll+10, '\n----', fijntype+'vk-splitter stap 0\n----',
          'Bepaal voor elke', relatie, 'relatie het interval',
          dfgf_bg+'-'+dfgf_eg, 'waarop beide bestaan')
    # #############################################################################

    baglib.aprint(ll, '\t0a. Bepaal voor', relatie, 'de kleinste', dfvkbg)

    baglib.debugprint(title='bij stap 0a-pre',
                      df=df, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, loglevel=loglevel)


    cols = [dfid, gfid, dfvkbg]
    dfgf_bgeg = df[cols].groupby([dfid, gfid]).min()

    baglib.debugprint(title='bij stap 0a',
                      df=dfgf_bgeg.reset_index(), colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, loglevel=loglevel)

    baglib.aprint(ll, '\t0b. Voeg hier de grootste', dfvkeg, 'aan toe')

    cols = [dfid, gfid, dfvkeg]
    dfgf_bgeg = pd.merge(dfgf_bgeg, df[cols].groupby([dfid, gfid]).max(), on=[dfid, gfid]).reset_index()
    
    baglib.debugprint(title='bij stap 0b',
                      df=dfgf_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, loglevel=loglevel)


    baglib.aprint(ll, '\t0c. Voeg hieraan toe de kleinste', gfvkbg)
    cols = [gfid, gfvkbg, gfvkeg]
    dfgf = pd.merge(dfgf_bgeg[[dfid, gfid]], gf[cols], on=gfid)
    cols = [dfid, gfid, gfvkbg]
    dfgf_bgeg = pd.merge(dfgf_bgeg, dfgf[cols].groupby([dfid,gfid]).min().reset_index(), on=[dfid, gfid])
    
    baglib.debugprint(title='bij stap 0c',
                      df=dfgf_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, loglevel=loglevel)
    
    
    baglib.aprint(ll, '\t0d. Voeg hieraan toe de grootste', gfvkeg)
    cols = [dfid, gfid, gfvkeg]
    dfgf_bgeg = pd.merge(dfgf_bgeg, dfgf[cols].groupby([dfid,gfid]).max().reset_index(), on=[dfid, gfid])
    
    baglib.debugprint(title='bij stap 0d',
                      df=dfgf_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, loglevel=loglevel)
    
    baglib.aprint(ll, '\t0e. Bepaal', dfgf_bg, 'als maximum van', dfvkbg, 'en', gfvkbg)
    dfgf_bgeg[dfgf_bg] = dfgf_bgeg[[dfvkbg, gfvkbg]].max(axis=1)
    dfgf_bgeg.drop([dfvkbg, gfvkbg], axis=1, inplace=True)

    baglib.debugprint(title='bij stap 0e',
                      df=dfgf_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, loglevel=loglevel)
    
    baglib.aprint(ll, '\t0f. Bepaal', dfgf_eg, 'als minimum van', dfvkeg, 'en', gfvkeg)
    dfgf_bgeg[dfgf_eg] = dfgf_bgeg[[dfvkeg, gfvkeg]].min(axis=1)
    dfgf_bgeg.drop([dfvkeg, gfvkeg], axis=1, inplace=True)

    baglib.debugprint(title='bij stap 0e',
                      df=dfgf_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, loglevel=loglevel)
    
    baglib.aprint(ll, '\t0g. Bepaal beide data ook voor', dfid, 'alleen')
    cols = [dfid, dfgf_bg, dfgf_eg]
    df_bgeg = dfgf_bgeg[cols].groupby(dfid).agg({dfgf_bg: 'min', dfgf_eg: 'max'}).reset_index()
    # print(df_bgeg)

    baglib.debugprint(title='bij stap 0f',
                      df=df_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, loglevel=loglevel+5)
    
    # print(df_bgeg.head())
    
    # #############################################################################
    baglib.aprint(ll+10, '\n----', fijntype+'vk-splitter stap 1 en 2\n----',
          'Verzamel de vkbg van', fijntype, 'en', groftype, 'in _df en maak',
          'hiermee nieuwe', fijntype, 'vk\n')
    # #############################################################################

    baglib.aprint(ll, '\t\tStap 1: we beginnen de reguliere vk van', fijntype, 'gebaseerd op',
          dfvkbg, '\n\t\tHet bestaande', dfvkid, 'gaat straks een rol spelen bij het imputeren')
    
    (_nrec, _nkey) = baglib.df_comp(loglevel=ll, df=df, key_lst=[dfid, dfvkbg])
    cols = [dfid, dfvkid, dfvkbg]
    _df = df[cols].drop_duplicates()
    (_nrec, _nkey) = baglib.df_comp(loglevel=ll, df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, u_may_change=True)
    
    baglib.debugprint(title='bij stap 1',
                      df=_df, colname=dfid,
                      vals=test_d[dfid], sort_on=cols, loglevel=loglevel)


   # #############################################################################
    baglib.aprint(ll+10, '\n----', fijntype+'vk-splitter stap 2\n----',
          'Voeg van', groftype, 'de', gfvkbg, 'hier aan toe. Hiervoor',
          'hebben we de', gfvkbg, 'uit', groftype, 'nodig.')
    # #############################################################################
    
    _gf = pd.merge(gf[[gfid, gfvkbg]], 
                   df[[dfid, gfid]].drop_duplicates(),
                   how='inner', on=gfid)
    # baglib.aprint(ll, _gf.head(50))
    
    cols = [dfid, gfvkbg]
    baglib.debugprint(title='bij stap 2a. '+dfid+' met '+gfvkbg+' van de bijbehorende '+gfid,
               df=_gf, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel-10)

    cols = [dfid, dfvkbg]
    _gf = _gf[[dfid, gfvkbg]].drop_duplicates().rename({gfvkbg: dfvkbg}, axis='columns')
    
    baglib.debugprint(title='bij stap 2b. '+dfid+' met de unieke '+gfvkbg+' hernoemd naar '+dfvkbg,
               df=_gf, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel)
    
    
    _df = pd.concat([_df, _gf]).drop_duplicates(subset=[dfid, dfvkbg], keep='first')
    (_nrec, _nkey) = baglib.df_comp(loglevel=ll, df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, u_may_change=True)
    
    baglib.debugprint(title='bij 2c. '+dfid+' met nu ook de unieke '+dfvkbg+' erbij. De NaNs treden op\n\
               \t\t als de '+dfvkbg+' eerst een '+gfvkbg+' van de '+gfid+' was.',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel)

        
    # #############################################################################
    baglib.aprint(ll+10, '\n----', fijntype+'vk-splitter stap 3\n----',
          'Voeg hier van', groftype, 'de', gfvkeg, 'aan toe, voor zover',
          'deze ongelijk is aan', future_date)
    baglib.aprint(ll, '\t\tHiervoor hebben we de koppeling\n\t\t', fijntype, '-', groftype,
          'nodig, plus de', gfvkeg, 'van', groftype)
    # #############################################################################
    baglib.aprint(ll+10, '\t\tTBD ----------------')
    
    '''
    _gf = pd.merge(gf[[gfid, gfvkeg]], 
                   df[[dfid, gfid]].drop_duplicates(),
                   how='inner', on=gfid)
    # baglib.aprint(ll, _gf.head(50))
    baglib.debugprint(title='3a. vier wpl met die drie straatjes geeft 13 records door veel verschillende bg',
               df=_gf, 
               colname='oprid',
               vals=['0457300000000259', '0457300000000260', '0003300000116985'], 
               sort_on=['oprid', 'wplvkeg'], loglevel=loglevel)
    
    
    _gf = _gf[[dfid, gfvkeg]].drop_duplicates().rename({gfvkeg: dfvkbg}, axis='columns')
    baglib.debugprint(title='3b. zonder woonplaats, duplicaten eruit, bg hernoemd',
               df=_gf,, 
               colname='oprid',
               vals=['0457300000000259', '0457300000000260', '0003300000116985'], 
               sort_on=['oprid', 'oprvkbg'], loglevel=loglevel)
    
    _gf = _gf[_gf[dfvkbg] != future_date]
    
    _df = pd.concat([_df, _gf]).drop_duplicates(subset=[dfid, dfvkbg] , keep='first')
    (_nrec, _nkey) = baglib.df_comp(loglevel=ll+20, df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, u_may_change=True)
    
    baglib.debugprint(title='3c. drie straatjes samengevoegde vkbg van opr en wpl. oprvkdi=nan is van wpl',
               df=_df, 
               colname='oprid',
               vals=['0457300000000259', '0457300000000260', '0003300000116985'], 
               sort_on=['oprid', 'oprvkbg'], loglevel=loglevel)
    '''
    
    # #############################################################################
    baglib.aprint(ll+10, '\n----', fijntype+'vk-splitter stap 4\n----',
          'Opvullen van de nans van de', dfvkid, 'met ffill')
    # #############################################################################
    baglib.aprint(ll, '\n\t\tDe sortering luistert nauw:\n')
    # https://stackoverflow.com/questions/27012151/forward-fill-specific-columns-in-pandas-dataframe


    baglib.aprint(ll, '\t\t4a. Werwijder', dfid, 'die buiten de range van de', relatie, 'vallen')
    _df = pd.merge(_df, df_bgeg, how='inner', on=dfid)
    msk = _df[dfvkbg] >= _df[dfgf_bg]
    _df = _df[msk]

    baglib.debugprint(title='bij stap 4a',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel)

    baglib.aprint(ll, '\t\t4b. Maak van het', dfvkid, 'met het laagste', dfvkbg, 'een 1',
          'om te voorkomen dat we met NaN gaan ffillen')
    
    # tmp_df gaat alle de records identificeren die op 1 gezet moeten worden.


    cols = [dfid, dfvkbg]
    _df.sort_values(by=cols, inplace=True)
    tmp_df = _df[cols].groupby(dfid).first().reset_index()
    # kolom tmp wordt gebruikt om te bepalen van welke records de dfvkid op 1
    # gezet moet worden. Dit is zo als deze kolom de waarde True (of eigenlijk
    # not nan heeft). Daarom: 
    tmp_df['tmp'] = True

    baglib.debugprint(title='4b1. Eerst de minimale ' +dfvkbg+ ' per '+dfid+' bepalen. Hiervoor zetten we tmp op True:',
               df=tmp_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel)
    
    # doe een leftmerge met _df en je weet in _df welke records het betreft...
    colsid = [dfid, dfvkbg, dfvkid]
    _df = pd.merge(_df[colsid], tmp_df, how='left', on=cols)

    baglib.debugprint(title='4b2: dit koppelen we aan het lopende dataframe van ' +fijntype,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel)


    # ... namelijk de waarden waar tmp niet gelijk aan nan is
    _df.loc[_df['tmp'].notna(), dfvkid] = 1
    _df.drop(columns='tmp', inplace=True)
    
    baglib.debugprint(title='4b3 en daarmee kunnen we het begin op 1 zetten',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel)

    # sys.exit('buggy end: tmp_df does not select min of vbovkbg!?')



    baglib.aprint(ll, '\n\t\t4c. Sorteer op', dfid, dfvkbg, '(nan last), waarna je met\n',
          '\t\t\tffill de nans kunt opvullen van de', dfvkid)
    cols = [dfid, dfvkbg]
    _df = _df.sort_values(by=cols, na_position='last')
    
    _df.loc[:,dfvkid].iat[0] = 1 # if the first record in NaN, then fill gives an error
    
    _df.loc[:,dfvkid] = _df.loc[:,dfvkid].ffill().astype({dfvkid:int})

    # baglib.aprint(ll, _df.head(30))
    baglib.debugprint(title='4a. NaNs staan nog verkeerd bij die '+dfid+' waar de vk rij begint met een NaN',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel)


    # #############################################################################
    baglib.aprint(ll+10, '\n----', fijntype+'vk-splitter stap 5\n----',
          'Voeg', dfvkeg, 'toe in twee stappen:\n')
    # #############################################################################

    _df = make_vkeg(ll, _df, fijntype, FUTURE_DATE)

    # #############################################################################
    baglib.aprint(ll+10, '\n----', fijntype+'vk-splitter stap 6\n----',
          'Maak een nieuwe teller voor',fijntype, 'vk genaamd', dfvkid2, 
          '\n\t\tom deze te kunnen onderscheiden van de bestaande', dfvkid)
    # #############################################################################
    
    _df = baglib.make_counter(ll, _df, grouper=dfid, newname=dfvkid2, cols=[dfid, dfvkbg])
    
    baglib.debugprint(title='bij stap 6. '+dfid+' met hun nieuwe vk tellers '+dfvkid2,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel)

    (_nrec, _nkey) = baglib.df_comp(loglevel=ll+20, df=_df, key_lst=[dfid, dfvkid2], nrec=_nrec, 
                             nkey=_nkey, u_may_change=True)

    
    # #############################################################################
    baglib.aprint(ll+10, '\n----', fijntype+'vk-splitter stap 7\n----',
          'Koppel', gfid, gfvkid, 'erbij.')
    # #############################################################################
    
    baglib.aprint(ll, '\n\t\t\t7a. voeg', gfid, 'toe met de input df van', fijntype)
    cols = [dfid, dfvkid, gfid]
    baglib.debugprint(title='input voor stap 7a. '+dfid+' met '+gfid+' uit het input df',
               df=df[cols].drop_duplicates(),
               colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel)


    _df = pd.merge(_df, df[cols].drop_duplicates(), how='inner', on=[dfid, dfvkid])
    


    baglib.debugprint(title='resultaat van stap 7a. '+dfid+' met hun '+gfid,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=[dfid, dfvkid2], 
               loglevel=loglevel)
    
    (_nrec, _nkey) = baglib.df_comp(loglevel=ll+20, df=_df, key_lst=[dfid, dfvkbg],
                           nrec=_nrec, nkey=_nkey,
                           u_may_change=True)
    
    baglib.aprint(ll, '\n\t\t\t7b. voeg eerst', gfvkid, gfvkbg, gfvkeg,
          'toe met de input dataframe van', groftype)
    baglib.aprint(ll, '\t\t\t', dfid, 'wordt nu gekoppeld met elk vk van zijn', gfid)
    cols = [gfid, gfvkid, gfvkbg, gfvkeg]
    _df = pd.merge(_df, gf[cols], how='inner', on=gfid)
    baglib.debugprint(title='input voor stap 7b. '+gfid,
               df=gf[cols], colname=gfid, vals=test_d[gfid], sort_on=cols, 
               loglevel=loglevel)

    baglib.debugprint(title='bij stap 7b. '+dfid+' met hun '+gfid+', '+gfvkid,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel-20)

        
    baglib.aprint(ll, '\n\t\t\t7c. Filter zodat het midden van een', fijntype, 'vk binnen een',
          groftype, 'vk valt.\n\t\t\tDit kan nu dankzij het splitsen van het', 
          fijntype, 'vk')
    _df['midden'] = (_df[dfvkbg] + _df[dfvkeg] ) * 0.5
    msk = (_df[gfvkbg] < _df['midden']) & (_df['midden'] < _df[gfvkeg])
    cols = [dfid, dfvkid2, dfvkid, gfid, gfvkid, dfvkbg, dfvkeg]
    _df = _df[msk][cols]

    baglib.debugprint(title='bij stap 7c. '+dfid+' met hun '+dfvkid+' gekoppeld met '+gfid+' en '+gfvkid,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel)

    (_nrec, _nkey) = baglib.df_comp(loglevel=ll, df=_df, key_lst=[dfid, dfvkbg],
                           nrec=_nrec, nkey=_nkey,
                           u_may_change=True)

    # #############################################################################
    baglib.aprint(ll+10, '\n----', fijntype+'vk-splitter stap 8\n----',
          'Toevoegen van de kolommen uit', fijntype,
          'die we nog misten')
    # #############################################################################
    
    _df = pd.merge(_df,
                   df.drop([gfid, dfvkbg, dfvkeg], axis=1).drop_duplicates(),
                   how='inner',
                   left_on=[dfid, dfvkid], right_on=[dfid, dfvkid])

    baglib.debugprint(title='bij stap 8: '+dfid+' met nieuwe geimputeerde vks',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               loglevel=loglevel-20)


    (_nrec, _nkey) = baglib.df_comp(loglevel=ll, df=_df, key_lst=[dfid, dfvkid2],
                             nrec=_nrec, nkey=_nkey,
                             u_may_change=False)



    baglib.aprint(ll+30, '\n-----------------------------------------------------------')   
    baglib.aprint(ll+30, '------ Einde vksplitter; fijntype:', fijntype, ', groftype:', 
          groftype)   
    baglib.aprint(ll+30, '------ Perc toegenomen', fijntype, 'voorkomens:', 
          round(100 * (_nkey/_nkey1 - 1), 1), '%')
    baglib.aprint(ll+30, '-----------------------------------------------------------\n')   

    return _df.rename(columns={dfvkid: dfvkid+'_oud', dfvkid2: dfvkid})
    

def make_vkeg(loglevel, df, bob, future_date):
    '''Maak de voorkomen einddatum geldigheid (vkeg), gegeven id en vkbg
    (voorkomen begindatum geldigheid) van een bob (bagobject).
    
    Conventies:
        bob + 'id' is een kolomnaam in df en identificeert het bob
        bob + 'vkbg' is een kolomnaam in df
        bob + 'vkeg' wordt aangemaakt als vervanger van de bestaande in df
        
    Stappen:
        1. Sorteer op id en vkbg
        2. doe een df.shift(periods = -1) om de begindatum van het volgende record te krijgen
        3. corrigeer de laatste vkeg van het vk rijtje. Maak deze gelijk aan future_date. 
        
    '''

    # init variabelen
    _ll = loglevel
    _dfid = bob + 'id'
    _dfvkbg = bob + 'vkbg'
    _dfvkeg = bob + 'vkeg'
    (_nrec, _nkey) = baglib.df_comp(loglevel=_ll, df=df, key_lst=[_dfid, _dfvkbg])

    baglib.aprint(_ll, '\t\t\tmake_vkeg input: aantal', bob, 'records:', _nrec, '; aantal', bob+'vk:', _nkey)
    baglib.aprint(_ll, '\t\t\tmake_vkeg 1: sorteer', bob, 'op', _dfid, _dfvkbg)
    _df = df.sort_values(by=[_dfid, _dfvkbg])
    # print(_df.info())
    
    baglib.aprint(_ll, '\t\t\tmake_vkeg 2: neem voor', _dfvkeg, 'de', _dfvkbg, 'van het volgende record')
    _df[_dfvkeg] = _df[_dfvkbg].shift(periods=-1)
    # print(_df.head())

    baglib.aprint(_ll, '\t\t\tmake_vkeg 3: corrigeer de vbovkeg van het meest recente', bob, 'voorkomen')
    baglib.aprint(_ll, '\t\t\t\t\tdit krijgt een datum in de toekomst:', future_date)
    _idx = _df.groupby([_dfid])[_dfvkbg].transform(max) == _df[_dfvkbg]
    _df.loc[_idx, _dfvkeg] = future_date

    baglib.aprint(_ll, '\t\t\tmake_vkeg 4: terugcasten van', _dfvkeg, 'naar int')
    _df = _df.astype({_dfvkeg:int})

    (_nrec, _nkey) = baglib.df_comp(loglevel=_ll, df=df, key_lst=[_dfid, _dfvkbg], nrec=_nrec, nkey=_nkey, u_may_change=False)
    
    return _df


def merge_vk(loglevel, df, bob, future_date, cols):
    ''' Neem de voorkomens van df samen als voor een dfid de waarden van de
    relevante cols gelijk zijn. We noemen dit "gelijke opeenvolgende vk". Neem 
    de minimale dfvkbg van elke set gelijke vk. Maak nieuwe vk id in dfvkid2.
    
    Conventies:
        bob + 'id' is een kolomnaam in df en identificeert het bob
        bob + 'vkbg' is een kolomnaam in df
        bob + 'vkeg' wordt aangemaakt als vervanger van de bestaande in df
        
    Stappen:
        1. verwijder dubbele records vwb df[dfid + cols], neem minimale dfvkbg
        2. maak nieuwe dfvkeg
        3. maak nieuwe tellers dfvkid2
        4. return nieuwe ingekorte df
    '''
 
    # init variabelen
    _ll = loglevel
    _dfid = bob + 'id'
    _dfvkid2 = bob + 'vkid2'
    _dfvkbg = bob + 'vkbg'
    _dfvkeg = bob + 'vkeg'
    _vk = [_dfid, _dfvkbg]
    _cols = [_dfid] + cols
    _cols2 = _cols + [_dfvkbg]
    (_nrec, _nkey) = baglib.df_comp(_ll, df, _vk)
    baglib.aprint(_ll, '\t\tmerge_vk input: aantal', bob, 'records:', _nrec, '; aantal', bob+'vk:', _nkey)

    baglib.aprint(_ll, '\t\tmerge_vk 1: verwijder dubbele opeenvolgende vk')
    _df = df[_cols2].sort_values(by=[_dfid, _dfvkbg]).drop_duplicates(subset=_cols, keep='first')
   
    baglib.aprint(_ll, '\t\tmerge_vk 2: voeg', _dfvkeg, 'toe')
    _df = make_vkeg(_ll, _df, bob, future_date)
    
    baglib.aprint(_ll, '\t\tmerge_vk 3: maak een nieuwe vk teller', _dfvkid2)
    _df = baglib.make_counter(_ll, _df, _dfid, _dfvkid2, [_dfid, _dfvkbg])

    (_nrec1, _nkey1) = baglib.df_comp(_ll, _df, _vk, nrec=_nrec, nkey=_nkey, u_may_change=True)

    baglib.aprint(_ll, '\t\tmerge_vk 4: ----------- perc vk:',
                  round(100 * (_nkey1/_nkey - 1), 1), '%')

    return _df
    
# ########################################################################
# ########################################################################

# loglevel ll:
# ll = -10 # minstel loggin
# ll = 10  # klein beetje logging
# ll = 10 # hoofdkoppen
# ll = 20 # koppen
# ll = 30 # tellingen
# ll = 40 # data voorbeelden 

ll = 20

if __name__ == '__main__':

    baglib.printkop(ll+40, OMGEVING)
    current_month = baglib.get_arg1(sys.argv, DIR02)

    baglib.printkop(ll+30, 'Lokale aanroep')
    bag_fix_vk(loglevel=ll,
               current_month=current_month,
               koppelvlak2=DIR02,
               koppelvlak3=DIR03)


