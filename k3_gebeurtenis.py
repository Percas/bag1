#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
In de BAG wordt de levenloop van een bag object (bob) gedefinieerd door een 
rijtje opeenvolgende en aaneensluitende voorkomens (vk) met een begindatum 
geldigheid (vkbg) en een einddatum geldigheid (vkeg).

Een voorkomen van een bag object kan gekoppeld zijn aan een ander bag 
object. Als voorbeeld:

Een vbovk (verblijfsobject voorkomen) wordt aan een (of meerdere) pnd (pand) 
of num (nummeraanduiding) gelinkt. 

Dat is feitelijk onzorgvuldig omdat tijdens 
de looptijd van een vbovk iets kan veranderen bij het gelinkte pnd of num,
bijvoorbeeld het bouwjaar van het pand of de gemeente waarin het num ligt.

Doel: Repareer de voorkomens van de 7 BAG objecten zodat bij een wezenlijke 
verandering van het object ook daadwerkelijk een nieuw voorkomen van dat
object gemaakt wordt. 

Koppel hierna voorkomens aan voorkomens van het bovenliggende bagobject
in plaats van plaats van voorkomens aan het bag object zelf.

Voer daarnaast enkele andere reparatie werkzaamheden aan vk uit
zoals in onderstaande stappen staat beschreven:

Stappen:
    Verwijder eendagsvliegen (vk die binnen een dag wijzigen)
    Merge vk: merge vk als er feitelijk niets wijzigt tussen twee
        opeenvolgende vk
    Vksplitter: splits vk als er iets wijzigt met het gekoppelde Bag 
    object (bob). Doe dit voor 
            gem - prv (todo: gemeente - provincie)
            wpl - gem (woonplaats)
            opr - wpl (openbare ruimte)
            num - opr (nummeraanduiding)
            vbo - num (verblijfsobject)
            vbo - pnd (nogmaals met vbo, maar dan met pnd)
Resultaat:
    voor elk van bovenstaande relaties koppelt een "onderliggend" vk aan een
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
van pnd2. Dit is niet geimplementeerd.

'''

# ################ import libraries ###############################
import pandas as pd
import numpy as np
import sys
import os
import time
import baglib
from config import OMGEVING, KOPPELVLAK0, KOPPELVLAK2, KOPPELVLAK3a, BAG_OBJECTEN, BAG_TYPE_DICT, RELEVANT_COLS_DICT, FILE_EXT, LOGFILE
import logging
# from k02_bag import k02_bag
from k1_xml import k1_xmlgem, k1_xml

# ############### Define functions ################################

def k2_fixvk(maand, logit):
    '''Repareer: 
        de parquet bestanden in koppelvlak k2 van extractmaand maand 
    naar:
        parquet formaat in koppelvlak k3a-bewerkt
        
    De repatie-acties bestaan uit:
        1. het verwijderen van eendagsvliegen 
        2. het samennemen van gelijke voorkomens
        3. het splitsen van voorkomens als er iets gebeurd met het 
        bovenliggende bagobject (bijvoorbeeld vbo-pnd of vbo-num)
    '''
        
    tic = time.perf_counter()
    logit.info(f'start k2_fixvk({maand})')

    # output
    dir_k3a_maand = os.path.join(KOPPELVLAK3a, maand)
    baglib.make_dirs(dir_k3a_maand, logit) # only make it if it doesn't exist yet

    for bagobject in BAG_OBJECTEN + ['wplgem']:
        if not os.path.exists(os.path.join(KOPPELVLAK2, maand, bagobject+'.'+FILE_EXT)):
            logit.warning(f'{bagobject}.{FILE_EXT} in koppelvlak 2 niet gevonden. Probeer af te leiden')
            k1_xml(bagobject, maand, logit)

    
    # We continue with wplgem as if it is the wpl file, because we need the
    # connection between wpl and gem
    baglib.copy_wplgem2wpl(dir1=KOPPELVLAK2, maand=maand, file_ext=FILE_EXT, logit=logit)
   

    ls_dict = {}
    bobs = ['vbo', 'pnd', 'wpl', 'num', 'opr']
    for bob in bobs:
        ls_dict[bob] = {}
            

    # ####################################################################
    # Aanroep van functie fixvk_fijngrof voor de verschillende combi's
    # ####################################################################
    
    (fijntype_df, groftype_df, ls_dict) =\
        fixvk_fijngrof(fijntype='opr',
                       groftype='wpl',
                       maand=maand,
                       ls_dict=ls_dict, logit=logit)

    (fijntype_df, groftype_df, ls_dict) =\
        fixvk_fijngrof(fijntype='num',
                       groftype='opr',
                       groftype_df=fijntype_df,
                       maand=maand,
                       ls_dict=ls_dict, logit=logit)

    (fijntype_df, groftype_df, ls_dict) =\
        fixvk_fijngrof(fijntype='vbo',
                       groftype='num',
                       groftype_df=fijntype_df,
                       maand=maand,
                       ls_dict=ls_dict, logit=logit)
    
    baglib.df_compare(fijntype_df, vk_lst=['vboid', 'vbovkid'],
                      nrec_nvk_ratio_is_1=False, logit=logit)

    (fijntype_df, groftype_df, ls_dict) =\
        fixvk_fijngrof(fijntype='vbo',
                       groftype='pnd',
                       fijntype_df=fijntype_df,
                       maand=maand,
                       ls_dict=ls_dict, logit=logit)
    
    baglib.df_compare(fijntype_df, vk_lst=['vboid', 'vbovkid'],
                      nrec_nvk_ratio_is_1=False, logit=logit)

    # groftype wordt bewaard in de functie fixvk_fijngrof. vbo moet nog bewaard
    # worden, want deze is nooit groftype
    logit.debug('bewaren van vbo\n')
    outputdir = os.path.join(KOPPELVLAK3a, maand)
    baglib.make_dirs(outputdir)
    outputfile = os.path.join(outputdir, 'vbo')
    baglib.save_df2file(df=fijntype_df, outputfile=outputfile, file_ext=FILE_EXT,
                        includeindex=False, logit=logit)

    df = pd.DataFrame(ls_dict)
    df = df.div(df.iloc[0]).round(2)
    logit.warning(f'af en toename aantallen records na elke stap\n{df}')
    
    toc = time.perf_counter()
    logit.info(f'einde bag_fix_vk in {(toc - tic)/60} min')

    
def fixvk_fijngrof(fijntype_df=pd.DataFrame(),
                   groftype_df=pd.DataFrame(),
                   fijntype='vbo',
                   groftype='pnd',
                   maand='202304',
                   ls_dict={},
                   logit=logging.DEBUG):
    '''Doe de stappen inlezen, eendagsvliegen fixen, mergen van vk, 
    splitten van voorkomens en bewaren voor fijntype en groftype.
    Opm: de stappen t/m mergen hoeven alleen als ze nog niet eerder gedaan 
    zijn. Het is al eerder gedaan als het betreffende dataframe niet leeg is.'''

    logit.info(f'start fixvk_fijngrof({maand}) met fijntype {fijntype}, groftype {groftype}')
    df_type = 'pandas'

    if fijntype_df.empty: # als fijntype_df niet empty dan is dit eerder gebeurd
        fijntype_df = baglib.read_input(input_file=os.path.join(KOPPELVLAK2, maand, fijntype),
                                        bag_type_d=BAG_TYPE_DICT, 
                                        file_ext=FILE_EXT,
                                        output_file_type=df_type,
                                        logit=logit)


        # if fijntype == 'vbo':
        #     # logit.warning('dbug dbug dubg dubg dubg')
        #     baglib.df_compare(fijntype_df, vk_lst=['vboid', 'vbovkid'],
        #                      nrec_nvk_ratio_is_1=False, logit=logit)

        ls_dict[fijntype]['0-ingelezen'] = fijntype_df.shape[0]

        logit.info(f'start fix_eendagsvlieg in {maand}')
        fijntype_df = baglib.fix_eendagsvlieg(fijntype_df, fijntype+'vkbg',
                                              fijntype+'vkeg', logit,
                                              df_type=df_type)
        ls_dict[fijntype]['1-eendagsvliegen'] = fijntype_df.shape[0]
        
        logit.info(f'start merge_vk in {maand}')
        fijntype_df = baglib.merge_vk(df=fijntype_df, bob=fijntype,
                                      relevant_cols=RELEVANT_COLS_DICT[fijntype],
                                      logit=logit, df_type=df_type)
        ls_dict[fijntype]['2-mergen-vk'] = fijntype_df.shape[0]


    if groftype_df.empty:
        groftype_df = baglib.read_input(input_file=os.path.join(KOPPELVLAK2,  maand, groftype),
                                        bag_type_d=BAG_TYPE_DICT, 
                                        file_ext=FILE_EXT,
                                        output_file_type=df_type,
                                        logit=logit)
        ls_dict[groftype]['0-ingelezen'] = groftype_df.shape[0]

        logit.info(f'start fix_eendagsvlieg in {maand}')
        groftype_df = baglib.fix_eendagsvlieg(groftype_df, groftype+'vkbg',
                                              groftype+'vkeg', logit,
                                              df_type=df_type)
        ls_dict[groftype]['1-eendagsvliegen'] = groftype_df.shape[0]

        # merge voorkomens        
        logit.info(f'start merge_vk in {maand}')
        groftype_df = baglib.merge_vk(df=groftype_df, bob=groftype,
                                      relevant_cols=RELEVANT_COLS_DICT[groftype], 
                                      logit=logit)
        ls_dict[groftype]['2-mergen-vk'] = groftype_df.shape[0]



    # splits voorkomens van fijntype tgv gebeurtenis in groftype

    logit.info(f'start vk_splitter in {maand}')
    fijntype_df = vksplitter(df=fijntype_df,
                             gf=groftype_df,
                             fijntype=fijntype,
                             groftype=groftype,
                             logit=logit)
    ls_dict[fijntype]['3-vk-splitter'] = fijntype_df.shape[0]
    ls_dict[groftype]['3-vk-splitter'] = groftype_df.shape[0]

    outputdir = os.path.join(KOPPELVLAK3a, maand)
    baglib.make_dirs(outputdir, logit=logit)
    outputfile = os.path.join(outputdir, groftype)
    baglib.save_df2file(df=groftype_df, outputfile=outputfile, 
                        file_ext=FILE_EXT,
                        includeindex=False, logit=logit)
    
    
    return (fijntype_df, groftype_df, ls_dict)



    
    
def vksplitter(df='vbo_df',
               gf='pnd_df',
               fijntype='vbo',
               groftype='pnd', 
               logit=logging.DEBUG,
               df_type='pandas'):
    
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
    # relatie = fijntype+'-'+groftype
    
    # deze kolom gaan we maken om de nieuwe vk te identificeren:
    dfvkid2 = fijntype + 'vkid2'

    # aantallen in de uitgangssituatie; neem [dfid, dfvkbg] als sleutel
    # _nrec, _nkey = baglib.df_comp(df=df, key_lst=[dfid, dfvkbg], logit=logit)
    _nrec, _nkey = baglib.df_compare(df=df, vk_lst=[dfid, dfvkbg], logit=logit)
    # baglib.df_compare(df=df,
    #                   vk_lst=[dfid, dfvkid], nrec_nvk_ratio_is_1=False,
    #                   logit=logit)
    
    # initiele aantallen
    _nrec1, _nkey1 = (_nrec, _nkey)
    logit.info(f'*** start vksplitter met {_nrec1} {fijntype} records waarvan {_nkey1} voorkomens en groftype {groftype}')

    
    # #############################################################################
    logit.debug('stap 1: bepaal interval waarop fijntype id en groftype id beide bestaan')
    # #############################################################################

    # logit.debug('1a. Bepaal de kleinste voorkomen begindatum van fijntype')
    cols = [dfid, gfid, dfvkbg]
    dfgf_bgeg = df[cols].groupby([dfid, gfid]).min()

    # logit.debug('1b. voeg hier de grootste voorkomen einddatum van fijntype aan toe')
    cols = [dfid, gfid, dfvkeg]
    dfgf_bgeg = pd.merge(dfgf_bgeg, df[cols].groupby([dfid, gfid]).max(), on=[dfid, gfid]).reset_index()
    
    # logit.debug('1c. voeg hieraan toe de kleinste voorkomen begindatum van groftype')
    cols = [gfid, gfvkbg, gfvkeg]
    dfgf = pd.merge(dfgf_bgeg[[dfid, gfid]], gf[cols], on=gfid)
    cols = [dfid, gfid, gfvkbg]
    dfgf_bgeg = pd.merge(dfgf_bgeg, dfgf[cols].groupby([dfid,gfid]).min().reset_index(), on=[dfid, gfid])
    
    # logit.debug('1d. voeg hieraan toe de grootste voorkomen einddatum van groftype')
    cols = [dfid, gfid, gfvkeg]
    dfgf_bgeg = pd.merge(dfgf_bgeg, dfgf[cols].groupby([dfid,gfid]).max().reset_index(), on=[dfid, gfid])
    
    del dfgf
    
    # logit.debug('bepaal het maximum van beide voorkomen begindatums')
    dfgf_bgeg[dfgf_bg] = dfgf_bgeg[[dfvkbg, gfvkbg]].max(axis=1)
    dfgf_bgeg.drop([dfvkbg, gfvkbg], axis=1, inplace=True)

    
    # logit.debug('bepaal het minimum van beide voorkomen einddatums')
    dfgf_bgeg[dfgf_eg] = dfgf_bgeg[[dfvkeg, gfvkeg]].min(axis=1)
    dfgf_bgeg.drop([dfvkeg, gfvkeg], axis=1, inplace=True)

    
    # logit.debug('bepaal het interval waarop fijntype bestaat')
    cols = [dfid, dfgf_bg, dfgf_eg]
    df_bgeg = dfgf_bgeg[cols].groupby(dfid).agg({dfgf_bg: 'min', dfgf_eg: 'max'}).reset_index()
    # print(df_bgeg)

    del dfgf_bgeg
    
    # #############################################################################
    logit.debug('stap 2: zet voorkomen begindatums van fijn en groftype bij elkaar in 1 kolom')
    logit.debug('opmerking: het voorkomen id  van fijntype wordt gebruikt voor het imputeren later')
    # #############################################################################

    # de vkbg van fijntype
    cols = [dfid, dfvkid, dfvkbg]
    _df = df[cols].drop_duplicates()
    
    # de vkbg van groftype. hiervoor is een merge nodig omdat gfvkbg alleen in gf staat
    _gf = pd.merge(gf[[gfid, gfvkbg]], 
                   df[[dfid, gfid]].drop_duplicates(),
                   how='inner', on=gfid)
    
    # hernoem de kolomnaam
    cols = [dfid, gfvkbg]
    _gf = _gf[cols].drop_duplicates().rename({gfvkbg: dfvkbg}, axis='columns')
    
    # zet alles bij elkaar en ontdubbel
    _df = pd.concat([_df, _gf]).drop_duplicates(subset=[dfid, dfvkbg], keep='first')
    
    del _gf

    # aantallen in de uitgangssituatie; [dfid, dfvkbg] nog steeds sleutel. marge mag zeker 50% zijn
    # _nrec, _nkey = baglib.df_comp(df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, toegestane_marge=0.5, logit=logit)
    _nrec, _nkey = baglib.df_compare(df=_df, vk_lst=[dfid, dfvkbg], nrec=_nrec, nvk=_nkey, 
                                     nvk_marge=1, logit=logit) #marge is 100%
    


    # #############################################################################
    logit.debug('stap 3: zet de eerste dfvkid in een rij fid op 1 als deze Nan is (alleen als deze Nan is!)')
    # #############################################################################
    
    # tmp_df gaat alle de records identificeren die op 1 gezet moeten worden.
    cols = [dfid, dfvkbg]
    _df.sort_values(by=cols, inplace=True)
    tmp_df = _df[cols].groupby(dfid).first().reset_index()
    # kolom tmp wordt gebruikt om te bepalen van welke records de dfvkid op 1
    # gezet moet worden. Dit is zo als deze kolom de waarde True (of eigenlijk
    # not nan heeft). Daarom: 
    tmp_df['tmp'] = True
    # doe een leftmerge met _df: _df heeft nu een kolom tmp die aangeeft of het 
    # betreffende record op 1 gezet moet worden
    colsid = [dfid, dfvkbg, dfvkid]
    _df = pd.merge(_df[colsid], tmp_df, how='left', on=cols)
    
    
    # aantallen mogen niet wijzigen met sleutel [dfid, dfvkbg]
    # _nrec, _nkey = baglib.df_comp(df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, toegestane_marge=0, logit=logit)
    _nrec, _nkey = baglib.df_compare(df=_df, vk_lst=[dfid, dfvkbg], nrec=_nrec, nvk=_nkey, 
                                     nvk_marge=0, logit=logit)

    del tmp_df

    # BUGfix: alleen op 1 zetten als de waarde van dfvkid gelijk aan Nan is, 
    # als de vkid bijvoorbeeld 2 zou zijn, krijg je straks verkeerde 
    # imputaties als je m nu op 1 gaat zetten.
    # dit lossen we op door tmp op Nan te zetten als dfvkid gelijk is aan notna()
    _df.loc[_df[dfvkid].notna(), 'tmp'] = np.nan
    _df.loc[_df['tmp'].notna(), dfvkid] = 1
    _df.drop(columns='tmp', inplace=True)
    
    # aantallen mogen niet wijzigen met sleutel [dfid, dfvkbg]
    # _nrec, _nkey = baglib.df_comp(df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, toegestane_marge=0, logit=logit)
    _nrec, _nkey = baglib.df_compare(df=_df, vk_lst=[dfid, dfvkbg], nrec=_nrec, nvk=_nkey, 
                                     nvk_marge=0, logit=logit)

    # #############################################################################
    logit.debug('stap 4: imputeren met forward fill ')
    # #############################################################################
    cols = [dfid, dfvkbg]
    _df = _df.sort_values(by=cols, na_position='last')
    _df.loc[:,dfvkid].iat[0] = 1 # if the first record in NaN, then fill gives an error
    _df.loc[:,dfvkid] = _df.loc[:,dfvkid].ffill().astype({dfvkid:int})

    # aantallen mogen niet wijzigen met sleutel [dfid, dfvkbg]. 
    # _nrec, _nkey = baglib.df_comp(df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, toegestane_marge=0, logit=logit)
    _nrec, _nkey = baglib.df_compare(df=_df, vk_lst=[dfid, dfvkbg], nrec=_nrec, nvk=_nkey, 
                                     nvk_marge=0, logit=logit)

    # 4a. Verwijder de fijntype id die buiten de range van fijntype-groftype
    # interval vallen. Als je dit eerder doet gaat het ffillen hierboven fout
    # omdat je dan soms als eerste een Nan hebt als vooromen id
    # #############################################################################
    logit.debug('stap 5: verwijder fvkid die buiten de range van de relatie vallen')
    # #############################################################################
    _df = pd.merge(_df, df_bgeg, how='inner', on=dfid)
    msk = _df[dfvkbg] >= _df[dfgf_bg]
    _df = _df[msk]

    del df_bgeg
    _df.drop(columns=[dfgf_bg, dfgf_eg], inplace=True)    

    # intervalrandjes gaan weg; aantal gaat kwart omlaag; sleutel [dfid, dfvkbg]
    # _nrec, _nkey = baglib.df_comp(df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, toegestane_marge=0.25, logit=logit)
    _nrec, _nkey = baglib.df_compare(df=_df, vk_lst=[dfid, dfvkbg], nrec=_nrec, nvk=_nkey, 
                                     nvk_marge=-0.25, logit=logit)

    # #############################################################################
    logit.debug('stap 6: maak een nieuwe voorkomen id aan (id2) en einddatum aan voor elke dfid')
    # #############################################################################

    _df = baglib.make_vkeg(_df, fijntype, logit)
    _df = baglib.make_counter(df=_df, dfid=dfid, 
                              old_counter=dfvkid,
                              new_counter=dfvkid2, 
                              dfvkbg=dfvkbg, logit=logit)
    _nrec, _nkey = baglib.df_compare(df=_df, vk_lst=[dfid, dfvkbg], nrec=_nrec, nvk=_nkey, 
                                     nvk_marge=0, logit=logit)
    
    # #############################################################################
    logit.debug('stap 7: koppel groftype er weer bij')
    # #############################################################################

    # 7a koppel eerst aan alle voorkomens (dfid, dfvkid) het groftype id (met het fijntype dataframe uit de input)
    cols = [dfid, dfvkid, gfid]
    _df = pd.merge(_df, df[cols].drop_duplicates(), how='inner', on=[dfid, dfvkid])

    # _df.drop(columns=dfvkid, inplace=True) kan hier nog niet. dfkvid nodig om bij te koppelen
    # _df = _df.rename(columns={dfvkid2: dfvkid})
    _df['midden'] = (_df[dfvkbg] + _df[dfvkeg] ) * 0.5
    _nrec, _nkey = baglib.df_compare(df=_df, vk_lst=[dfid, dfvkbg], nrec=_nrec, nvk=_nkey, 
                                     nvk_marge=0, logit=logit)

    # efficient met geheugen anders crasht ie bij vbo-pnd met 32GB
    _astype_cols = {_i: BAG_TYPE_DICT[_i] for _i in list(_df.columns)}
    _df = _df.astype(dtype=_astype_cols)

    
    logit.debug('7b: koppel alle vk van groftype bij')
    # 7b koppel hier alle voorkomens van groftype id bij
    # opmerking: dit kost wat geheugen!
    cols = [gfid, gfvkid, gfvkbg, gfvkeg]
    _df = pd.merge(_df, gf[cols], how='inner', on=gfid)
    _nrec, _nkey = baglib.df_compare(df=_df, vk_lst=[dfid, dfvkbg], nrec=_nrec, nvk=_nkey, 
                                     nvk_marge=3, logit=logit)
   

    logit.debug('7c: filter zodat het midden van een fijntype binnen groftype valt')
    # 7c filter zodat het midden van een fijntype vk binnen een groftype vk valt
    # Dit kan nu dankzij het splitsen van het fijntype (hier hebben we het om gedaan)
    msk = (_df[gfvkbg] < _df['midden']) & (_df['midden'] < _df[gfvkeg])
    cols = [dfid, dfvkid, dfvkid2, gfid, gfvkid, dfvkbg, dfvkeg]
    _df = _df[msk][cols]
    _nrec, _nkey = baglib.df_compare(df=_df, vk_lst=[dfid, dfvkbg], nrec=_nrec, nvk=_nkey, 
                                     nvk_marge=3, logit=logit)


    # #############################################################################
    logit.debug('stap 8: koppel de andere kolommen van f erbij (op fid, fvkid)')
    # #############################################################################

    # _ = baglib.df_compare(df.drop([gfid, dfvkbg, dfvkeg], axis=1).drop_duplicates(),
    baglib.df_compare(df=df,
                      vk_lst=[dfid, dfvkid],
                      logit=logit)


    _df = pd.merge(_df,
                   df.drop([gfid, dfvkbg, dfvkeg], axis=1).drop_duplicates(),
                   how='inner',
                   left_on=[dfid, dfvkid],
                   right_on=[dfid, dfvkid])

    _nrec, _nkey = baglib.df_compare(df=_df, vk_lst=[dfid, dfvkbg], nrec=_nrec, nvk=_nkey, 
                                     nvk_marge=0, logit=logit)

    logit.info(f'*** einde vksplitter, wijziging in {fijntype} vk: {round(100 * (_nkey/_nkey1 - 1), 1)} %')


    _df.drop(columns=dfvkid, inplace=True)
    _df = _df.rename(columns={dfvkid2: dfvkid})

    return _df

    
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
    logit.info('start k2_fixvk vanuit main')
    logit.warning(OMGEVING)
    logit.setLevel(logging.DEBUG)
    
    maand_lst = baglib.get_args(sys.argv, KOPPELVLAK0)

    for maand in maand_lst:
        k2_fixvk(maand, logit)


