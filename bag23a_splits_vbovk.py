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

-------------------------
Functie splits_bobvk.py:
-------------------------
    bob = bagobject

1. Bepaal voor wpl-gem of er nieuwe wplvk moeten worden aangemaakt. Een nieuw
wplvk wordt alleen aangemaakt er een nieuw gemvk is
2. Idem voor opr-wpl
3. Idem voor num-opr
4. Idem voor vbo-num
5. Idem voor vbo-pnd

Voor deze 5 relaties gebruiken we de functie vksplitter. Deze werkt als volgt:

-------------------------
Functie vksplitter.py:
-------------------------
We specificeren het voor vbo-pnd. Het geldt echter voor elk van bovenstaande
5 relaties.

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
import numpy as np
import sys
# import os
import time
import baglib
from baglib import BAG_TYPE_DICT
from config import LOCATION


# ############### Define functions ################################

def vksplitter(df='vbo_df', gf='pnd_df', fijntype ='vbo', groftype = 'pnd', 
               future_date = 20321231, test_d={}, debuglevel=0):
    
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
    print('-----------------------------------------------------------')   
    print('------ Start vksplitter; fijntype:', fijntype, ', groftype:', 
          groftype)   
    print('-----------------------------------------------------------')   
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
    
    
    # deze kolom gaan we maken om de nieuwe vk te identificeren:
    dfvkid2 = fijntype + 'vkid2'
    # aantallen in de uitgangssituatie     

    # print('DDDDDEBBBBBUUUUG', [dfid, dfvkid])

    (_nrec1, _nkey1) = baglib.df_comp(df, key_lst=[dfid, dfvkid])



    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 0\n----',
          'Bepaal voor elke', fijntype+'-'+groftype, 'relatie het interval',
          dfgf_bg+'-'+dfgf_eg, 'waarop beide bestaan\n')
    # #############################################################################


    relatie = fijntype+'-'+groftype
    print('\t0a. Bepaal voor', relatie, 'de kleinste', dfvkbg)
    cols = [dfid, gfid, dfvkbg]
    dfgf_bgeg = df[cols].groupby([dfid, gfid]).min()

    baglib.debugprint(title='bij stap 0a',
                      df=dfgf_bgeg.reset_index(), colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, debuglevel=debuglevel)

    print('\t0b. Voeg hier de grootste', dfvkeg, 'aan toe')

    cols = [dfid, gfid, dfvkeg]
    dfgf_bgeg = pd.merge(dfgf_bgeg, df[cols].groupby([dfid, gfid]).max(), on=[dfid, gfid]).reset_index()
    
    baglib.debugprint(title='bij stap 0b',
                      df=dfgf_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, debuglevel=debuglevel)


    print('\t0c. Voeg hieraan toe de kleinste', gfvkbg)
    cols = [gfid, gfvkbg, gfvkeg]
    dfgf = pd.merge(dfgf_bgeg[[dfid, gfid]], gf[cols], on=gfid)
    cols = [dfid, gfid, gfvkbg]
    dfgf_bgeg = pd.merge(dfgf_bgeg, dfgf[cols].groupby([dfid,gfid]).min().reset_index(), on=[dfid, gfid])
    
    baglib.debugprint(title='bij stap 0c',
                      df=dfgf_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, debuglevel=debuglevel)
    
    
    print('\t0d. Voeg hieraan toe de grootste', gfvkeg)
    cols = [dfid, gfid, gfvkeg]
    dfgf_bgeg = pd.merge(dfgf_bgeg, dfgf[cols].groupby([dfid,gfid]).max().reset_index(), on=[dfid, gfid])
    
    baglib.debugprint(title='bij stap 0d',
                      df=dfgf_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, debuglevel=debuglevel)
    
    print('\t0e. Bepaal', dfgf_bg, 'als maximum van', dfvkbg, 'en', gfvkbg)
    dfgf_bgeg[dfgf_bg] = dfgf_bgeg[[dfvkbg, gfvkbg]].max(axis=1)
    dfgf_bgeg.drop([dfvkbg, gfvkbg], axis=1, inplace=True)

    baglib.debugprint(title='bij stap 0e',
                      df=dfgf_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, debuglevel=debuglevel)
    
    print('\t0f. Bepaal', dfgf_eg, 'als minimum van', dfvkeg, 'en', gfvkeg)
    dfgf_bgeg[dfgf_eg] = dfgf_bgeg[[dfvkeg, gfvkeg]].min(axis=1)
    dfgf_bgeg.drop([dfvkeg, gfvkeg], axis=1, inplace=True)

    baglib.debugprint(title='bij stap 0e',
                      df=dfgf_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, debuglevel=debuglevel)
    
    print('\t0g. Bepaal beide data ook voor', dfid, 'alleen')
    cols = [dfid, dfgf_bg, dfgf_eg]
    df_bgeg = dfgf_bgeg[cols].groupby(dfid).agg({dfgf_bg: 'min', dfgf_eg: 'max'}).reset_index()
    # print(df_bgeg)

    baglib.debugprint(title='bij stap 0f',
                      df=df_bgeg, colname=dfid,
                      vals=test_d[dfid], sort_on=dfid, debuglevel=debuglevel)
    
    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 1\n----',
          'Verzamel de vkbg van', fijntype, 'en', groftype, 'in _df en maak',
          'hiermee nieuwe', fijntype, 'vk\n')
    # #############################################################################

    print('\t\tWe beginnen de reguliere vk van', fijntype, 'gebaseerd op',
          dfvkbg, '\n\t\tHet bestaande', dfvkid, 'gaat straks een rol spelen bij het imputeren')
    
    (_nrec, _nkey) = baglib.df_comp(df=df, key_lst=[dfid, dfvkbg])
    cols = [dfid, dfvkid, dfvkbg]
    _df = df[cols].drop_duplicates()
    (_nrec, _nkey) = baglib.df_comp(df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, u_may_change=True)
    
    baglib.debugprint(title='bij stap 1',
                      df=_df, colname=dfid,
                      vals=test_d[dfid], sort_on=cols, debuglevel=debuglevel)


   # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 2\n----',
          'Voeg van', groftype, 'de', gfvkbg, 'hier aan toe. Hiervoor',
          'hebben we de koppeling '+fijntype+'-'+groftype, 'nodig,\n plus',
          'de', gfvkbg, 'van', groftype)
    # #############################################################################
    
    _gf = pd.merge(gf[[gfid, gfvkbg]], 
                   df[[dfid, gfid]].drop_duplicates(),
                   how='inner', on=gfid)
    # print(_gf.head(50))
    
    cols = [dfid, gfvkbg]
    baglib.debugprint(title='bij stap 2a. '+dfid+' met '+gfvkbg+' van de bijbehorende '+gfid,
               df=_gf, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)

    cols = [dfid, dfvkbg]
    _gf = _gf[[dfid, gfvkbg]].drop_duplicates().rename({gfvkbg: dfvkbg}, axis='columns')
    baglib.debugprint(title='bij stap 2b. '+dfid+' met de unieke '+gfvkbg+' hernoemd naar '+dfvkbg,
               df=_gf, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)
    
    
    _df = pd.concat([_df, _gf]).drop_duplicates(subset=[dfid, dfvkbg], keep='first')
    (_nrec, _nkey) = baglib.df_comp(df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, u_may_change=True)
    
    baglib.debugprint(title='bij 2c. '+dfid+' met nu ook de unieke '+dfvkbg+' erbij. De NaNs treden op\n\
               \t\t als de '+dfvkbg+' eerst een '+gfvkbg+' van de '+gfid+' was.',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)

        
    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 3\n----',
          'Voeg hier van', groftype, 'de', gfvkeg, 'aan toe, voor zover',
          'deze ongelijk is aan', future_date)
    print('\t\tHiervoor hebben we de koppeling\n\t\t', fijntype, '-', groftype,
          'nodig, plus de', gfvkeg, 'van', groftype)
    # #############################################################################
    print('\t\tTBD ----------------')
    
    '''
    _gf = pd.merge(gf[[gfid, gfvkeg]], 
                   df[[dfid, gfid]].drop_duplicates(),
                   how='inner', on=gfid)
    # print(_gf.head(50))
    baglib.debugprint(title='3a. vier wpl met die drie straatjes geeft 13 records door veel verschillende bg',
               df=_gf, 
               colname='oprid',
               vals=['0457300000000259', '0457300000000260', '0003300000116985'], 
               sort_on=['oprid', 'wplvkeg'], debuglevel=debuglevel)
    
    
    _gf = _gf[[dfid, gfvkeg]].drop_duplicates().rename({gfvkeg: dfvkbg}, axis='columns')
    baglib.debugprint(title='3b. zonder woonplaats, duplicaten eruit, bg hernoemd',
               df=_gf,, 
               colname='oprid',
               vals=['0457300000000259', '0457300000000260', '0003300000116985'], 
               sort_on=['oprid', 'oprvkbg'], debuglevel=debuglevel)
    
    _gf = _gf[_gf[dfvkbg] != future_date]
    
    _df = pd.concat([_df, _gf]).drop_duplicates(subset=[dfid, dfvkbg] , keep='first')
    (_nrec, _nkey) = baglib.df_comp(df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, u_may_change=True)
    
    baglib.debugprint(title='3c. drie straatjes samengevoegde vkbg van opr en wpl. oprvkdi=nan is van wpl',
               df=_df, 
               colname='oprid',
               vals=['0457300000000259', '0457300000000260', '0003300000116985'], 
               sort_on=['oprid', 'oprvkbg'], debuglevel=debuglevel)
    '''
    
    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 4\n----',
          'Opvullen van de nans van de', dfvkid, 'met ffill')
    # #############################################################################
    print('\n\t\tDe sortering luistert nauw:\n')
    # https://stackoverflow.com/questions/27012151/forward-fill-specific-columns-in-pandas-dataframe

    baglib.debugprint(title='begin van stap 4',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)


    print('\t\t4a. Werwijder', dfid, 'die buiten de range van de', relatie, 'vallen')
    _df = pd.merge(_df, df_bgeg, how='inner', on=dfid)
    msk = _df[dfvkbg] >= _df[dfgf_bg]
    _df = _df[msk]

    baglib.debugprint(title='bij stap 4a',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)

    print('\t\t4b. Maak van het', dfvkid, 'met het laagste', dfvkbg, 'een 1',
          'om te voorkomen dat we met NaN gaan ffillen')
    
    # tmp_df gaat alle de records identificeren die op 1 gezet moeten worden.


    cols = [dfid, dfvkbg]
    _df.sort_values(by=cols, inplace=True)
    tmp_df = _df[cols].groupby(dfid).first().reset_index()
    # kolom tmp wordt gebruikt om te bepalen van welke records de dfvkid op 1
    # gezet moet worden. Dit is zo als deze kolom de waarde True (of eigenlijk
    # not nan heeft). Daarom: 
    tmp_df['tmp'] = True

    baglib.debugprint(title='4b1. NaNs goed zetten bij die '+dfid+' waar de vk rij begint met een NaN',
               df=tmp_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)
    
    # doe een leftmerge met _df en je weet in _df welke records het betreft...
    colsid = [dfid, dfvkbg, dfvkid]
    _df = pd.merge(_df[colsid], tmp_df, how='left', on=cols)

    baglib.debugprint(title='4c',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)

    # ... namelijk de waarden waar tmp niet gelijk aan nan is
    _df.loc[_df['tmp'].notna(), dfvkid] = 1
    _df.drop(columns='tmp', inplace=True)
    
    baglib.debugprint(title='4d begin staat op 1',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)

    # sys.exit('buggy end: tmp_df does not select min of vbovkbg!?')



    print('\n\t\t4b. Sorteer op', dfid, dfvkbg, '(nan last), waarna je met\n',
          '\t\t\tffill de nans kunt opvullen van de', dfvkid)
    cols = [dfid, dfvkbg]
    _df = _df.sort_values(by=cols, na_position='last')
    
    _df.loc[:,dfvkid].iat[0] = 1 # if the first record in NaN, then fill gives an error
    
    _df.loc[:,dfvkid] = _df.loc[:,dfvkid].ffill().astype({dfvkid:int})

    # print(_df.head(30))
    baglib.debugprint(title='4a. NaNs staan nog verkeerd bij die '+dfid+' waar de vk rij begint met een NaN',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)


    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 5\n----',
          'Voeg', dfvkeg, 'toe in twee stappen:\n')
    # #############################################################################

    print('\t\t\t5a. neem de', dfvkbg, 'van het volgende record (sortering uit stap 5)')
 
    _df[dfvkeg] = _df[dfvkbg].shift(periods=-1)
    (_nrec, _nkey) = baglib.df_comp(_df, key_lst=[dfid, dfvkbg],
                             nrec=_nrec, nkey=_nkey,
                             u_may_change=False)

    baglib.debugprint(title='bij stap 5a. '+dfid+' met hun '+dfvkeg+'. Deze staat verkeerd voor de laatste van de vk rij',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)

    # print(_df[_df[dfvkeg].isna()])

    print('\t\t\t5b. corrigeer de vbovkeg van het meest recente', fijntype, 'voorkomen')
    print('\t\t\tDit krijgt een datum in de toekomst:', future_date)
    idx = _df.groupby([dfid])[dfvkbg].transform(max) == _df[dfvkbg]
    _df.loc[idx, dfvkeg] = future_date
    _df = _df.astype({dfvkeg:int})

    baglib.debugprint(title='bij stap 5b. '+dfid+' met de laatste '+dfvkeg+' uit hun vk rijtje op '+str(future_date)+' gezet.',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)

    (_nrec, _nkey) = baglib.df_comp(_df, key_lst=[dfid, dfvkbg],
                           nrec=_nrec, nkey=_nkey,
                           u_may_change=False)


    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 6\n----',
          'Maak een nieuwe teller voor',fijntype, 'vk genaamd', dfvkid2, 
          '\n\t\tom deze te kunnen onderscheiden van de bestaande', dfvkid)
    # #############################################################################
    cols = [dfid, dfvkbg]
    _df = _df.sort_values(by=cols, na_position='last')
    _df = baglib.makecounter(_df, grouper=dfid, newname=dfvkid2, sortlist=[dfid, dfvkbg, gfid])
    print('\t\tHet voorkomen (vk) van een', dfid, 'was tot nu toe geidentificeerd met de',
          '\n\t\tbegindatum van dat vk. Schakel vanaf nu over op', dfvkid2,
          'om vk te identificeren')

    baglib.debugprint(title='bij stap 6. '+dfid+' met hun nieuwe vk tellers '+dfvkid2,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)

    (_nrec, _nkey) = baglib.df_comp(_df, key_lst=[dfid, dfvkid2], nrec=_nrec, 
                             nkey=_nkey, u_may_change=True)

    
    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 7\n----',
          'Koppel', gfid, gfvkid, 'erbij.')
    # #############################################################################
    
    print('\n\t\t\t7a. voeg', gfid, 'toe met de input df van', fijntype)
    cols = [dfid, dfvkid, gfid]
    baglib.debugprint(title='input voor stap 7a. '+dfid+' met '+gfid+' uit het input df',
               df=df[cols].drop_duplicates(),
               colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)


    _df = pd.merge(_df, df[cols].drop_duplicates(), how='inner', on=[dfid, dfvkid])
    


    baglib.debugprint(title='resultaat van stap 7a. '+dfid+' met hun '+gfid,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=[dfid, dfvkid2], 
               debuglevel=debuglevel)
    
    (_nrec, _nkey) = baglib.df_comp(_df, key_lst=[dfid, dfvkbg],
                           nrec=_nrec, nkey=_nkey,
                           u_may_change=True)
    
    print('\n\t\t\t7b. voeg eerst', gfvkid, gfvkbg, gfvkeg,
          'toe met de input dataframe van', groftype)
    print('\t\t\t', dfid, 'wordt nu gekoppeld met elk vk van zijn', gfid)
    cols = [gfid, gfvkid, gfvkbg, gfvkeg]
    _df = pd.merge(_df, gf[cols], how='inner', on=gfid)
    baglib.debugprint(title='input voor stap 7b. '+gfid,
               df=gf[cols], colname=gfid, vals=test_d[gfid], sort_on=cols, 
               debuglevel=debuglevel)

    baglib.debugprint(title='bij stap 7b. '+dfid+' met hun '+gfid+', '+gfvkid,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel-20)

        
    print('\n\t\t\t7c. Filter zodat het midden van een', fijntype, 'vk binnen een',
          groftype, 'vk valt.\n\t\t\tDit kan nu dankzij het splitsen van het', 
          fijntype, 'vk')
    _df['midden'] = (_df[dfvkbg] + _df[dfvkeg] ) * 0.5
    msk = (_df[gfvkbg] < _df['midden']) & (_df['midden'] < _df[gfvkeg])
    cols = [dfid, dfvkid2, dfvkid, gfid, gfvkid, dfvkbg, dfvkeg]
    _df = _df[msk][cols]

    baglib.debugprint(title='bij stap 7c. '+dfid+' met hun '+dfvkid+' gekoppeld met '+gfid+' en '+gfvkid,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel)

    (_nrec, _nkey) = baglib.df_comp(_df, key_lst=[dfid, dfvkbg],
                           nrec=_nrec, nkey=_nkey,
                           u_may_change=True)

    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 8\n----',
          'Toevoegen van de kolommen uit', fijntype,
          'die we nog misten')
    # #############################################################################
    
    _df = pd.merge(_df,
                   df.drop([gfid, dfvkbg, dfvkeg], axis=1).drop_duplicates(),
                   how='inner',
                   left_on=[dfid, dfvkid], right_on=[dfid, dfvkid])

    baglib.debugprint(title='bij stap 8: '+dfid+' met nieuwe geimputeerde vks',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=debuglevel-20)


    (_nrec, _nkey) = baglib.df_comp(_df, key_lst=[dfid, dfvkid2],
                             nrec=_nrec, nkey=_nkey,
                             u_may_change=False)



    print('\n-----------------------------------------------------------')   
    print('------ Einde vksplitter; fijntype:', fijntype, ', groftype:', 
          groftype)   
    print('------ Perc toegenomen', fijntype, 'voorkomens:', 
          round(100 * (_nkey/_nkey1 - 1), 1), '%')
    print('-----------------------------------------------------------')   

    return _df.rename(columns={dfvkid: dfvkid+'_oud', dfvkid2: dfvkid})
    
    
def find_double_vk(df, bobid, bobvkid):
    '''Find the double voorkomen (vk) in df, identified by bobid, bobvkid.'''
    return df.groupby([bobid, bobvkid]).size().to_frame('aantal').sort_values(by='aantal', ascending=False)


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
    
    KEY_DICT = {'vbo': vbovk,
                'pnd': pndvk,
                'num': numvk,
                'opr': oprvk,
                'wpl': wplvk}
    
    bd = {}         # dict with df with bagobject (vbo en pnd in this case)
    nrec = {}
    nkey = {}    
    # if printit = True bepaal-hoofdpnd prints extra info
    # printit = True
    
    FUTURE_DATE = 20321231
    DEBUGLEVEL = 10
    
    TEST_D = {'wplid': ['3386', '1012', '3631'],
              'oprid': ['0457300000000259', '0457300000000260', '0003300000116985'],
              # 'numid': ['1979200000000546', '0457200000521759', '0457200000521256'],
              # 'numid': ['0388200000212289'],
              'numid': ['0003200000136934'],
              # 'vboid': ['0457010000060735', '0457010000061531', '1979010000000545', '0388010000212290'],
              # 'vboid': ['0388010000212290'],
              'vboid': ['0003010000128544'],
              # 'vboid': ['0007010000000192'],
              'pndid': ['0388100000202416', '0388100000231732', '0388100000232080', '0388100000232081']}

    print('\n---------------DOELSTELLING--------------------------------')
    print('----splits VBO voorkomens (vbovk) als er tijdens de loooptijd')
    print('----van dat vbovk een van 5 gebeurtenissen optreedt')
    print('-----------------------------------------------------------')

    # #########################################################################
    print('\n----0a. Inlezen van de inputbestanden-----------------------\n')
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
        print('\t\tCheck: zitten er dubbele vk in', bob+':')
        print(find_double_vk(bd[bob], bob+'id', bob+'vkid').head(2))
        print()

    # #############################################################################
    print('\n----1. Roep vksplitter aan -----------------------\n')
    # #############################################################################

    
    bd['opr'] = vksplitter(df=bd['opr'], gf=bd['wpl'],
                                  fijntype='opr', groftype='wpl',
                                  future_date=FUTURE_DATE,
                                  test_d=TEST_D, debuglevel=DEBUGLEVEL)

    cols = ['oprid', 'oprvkid2']
    baglib.debugprint(title='Testje na vksplitter',
                      df=bd['opr'], colname='oprid', vals=TEST_D['oprid'], sort_on=cols, 
                      debuglevel=DEBUGLEVEL)
    
    bd['num'] = vksplitter(df=bd['num'], gf=bd['opr'],
                                  fijntype='num', groftype='opr',
                                  future_date=FUTURE_DATE,
                                  test_d=TEST_D, debuglevel=DEBUGLEVEL)
    print(bd['num'].info())
    
    
    bd['vbo'] = vksplitter(df=bd['vbo'], gf=bd['num'],
                                  fijntype='vbo', groftype='num',
                                  future_date=FUTURE_DATE,
                                  test_d=TEST_D, debuglevel=DEBUGLEVEL+20)

    # rename column _oud so it won't get confused when we invoke vksplitter 
    # for vbo for the second time, but now with pnd as groftype
    bd['vbo'].rename(columns={'vbovkid_oud': 'vbovkid_org'}, inplace=True)
    
    
    
    bd['vbo'] = vksplitter(df=bd['vbo'], gf=bd['pnd'],
                           fijntype='vbo', groftype='pnd',
                           future_date=FUTURE_DATE,
                           test_d=TEST_D, debuglevel=DEBUGLEVEL)

    bd['vbo'].drop(columns='vbovkid_oud', inplace=True)    
    # print(bd['vbo'].info())
   
    
    '''
    baglib.debugprint(title='Resultaat voor voorbeeld vbo:',
               df=bd['vbo'][cols], colname='vboid', vals=TEST_D['vboid'], sort_on=['vboid', 'vbovkid'], 
               debuglevel=20)
    
    # print(bd['vbo'].head(30))
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], key_lst=vbovk) 
    
    doel_vbovk_u = nkey['vbo']
    n_vbovk = nrec['vbo']
    print('\tConcreet doel:', doel_vbovk_u, 'vbovk van een pndvk voorzien.')

    
    # zuinig met geheugen: Sommige types kunnen teruggecast worden.
    bd['vbo'] = baglib.recast_df_floats(bd['vbo'], BAG_TYPE_DICT)
    (nrec['vbo'], nkey['vbo']) = baglib.df_comp(bd['vbo'], nrec=nrec['vbo'],
                                                nkey=nkey['vbo'],
                                                u_may_change=False)
    '''
    toc = time.perf_counter()
    baglib.print_time(toc - tic, 'gebruikte tussentijd:', printit)

    
    # #############################################################################
    print('\n2.----Bewaren in koppelvlak3 -------------------')
    # #############################################################################
    # tic = time.perf_counter()
 
    for bob, vk in KEY_DICT.items():
        print('\tBewaren koppelbaar gemaakte', bob+'.csv')
        outputfile = OUTPUTDIR + bob+'.csv'
    
        # cols = ['vbovkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'pndid', 'pndvkid', 'numid', 'numvkid']
        bd[bob].sort_values(by=[bob+'id', bob+'vkid']).to_csv(outputfile, index=False)
    
    toc = time.perf_counter()
    baglib.print_time(toc - tic, 'bag_splits_vbovk duurde', printit)
    
    
    
    
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


