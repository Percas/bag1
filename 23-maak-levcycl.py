#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on June 11, 2022

Purpose: make the levenscyclus file for VBO: 
    vbovk met bouwjaar en typeinliggend
    restrictions: vbovk must be in IN_VOORRAAD and in a gemeente
    
0.1: initial version based on 23-bew-data.py


"""

# ################ import libraries ###############################
import pandas as pd
import baglib
import os
import sys
# import numpy as np


# ############### Define functions ################################
def num2gem(bagobj_dict):
    '''Link num-opr-wpl-gem based on input dict with these 4 df's.'''

    print('\n\tMerge num (num-opr) with opr (opr-wpl), link on oprid')
    _res_df = pd.merge(bagobj_dict['num'][['numid', 'oprid']], 
                       bagobj_dict['opr'][['oprid', 'wplid']],
                       how='left')
    baglib.df_total_vs_key2('num-opr', _res_df, ['numid'])
    '''
    print('DEBUG1 num-opr:')
    print(_res_df.info())
    print(bagobj_d['gemwpl'].info())
    '''
    print('\n\tMerge num-wpl with wpl-gem, link on wplid...')
    _res_df = pd.merge(_res_df,
                       bagobj_dict['gemwpl'][['wplid', 'gemid']],
                       how='left')
    # baglib.df_total_vs_key2('num-opr-wpl-gem', _res_df, ['oprid'])
    
    return _res_df

def read_dfs(ddir, df_lst, keys_lst):
    '''Read the csv files in dflist in directory ddir. Return dict with 
    dflist as keys and dataframes created from csv files as values'''
    
    _df_dict = {}
    for _file in df_lst:
        print('\tReading', _file, 'in', ddir)
        _df_dict[_file] = pd.read(ddir + _file + '.csv')
        _df_dict[keys_lst] = _df_dict[keys_lst].astype(str)
        return _df_dict


# #################################################################
print('-----------------------------------------------------------')
print('Bepaal bouwjaar en typeinliggend van alle vbovk in voorraad')
print('-----------------------------------------------------------')

print('\tDefinitie: een vbovk is in voorraad als\n',
      "\tvbostatus is in 'inge', 'inni', 'verb', 'buig' en\n",
      '\tvbovk ligt in een gemeente\n')

# #############################################################################
# print('00.............Initializing variables...............................')
# #############################################################################
# month and dirs
os.chdir('..')
BASEDIR = os.getcwd() + '/'
baglib.print_omgeving(BASEDIR)
DATADIR = BASEDIR + 'data/'
DIR02 = DATADIR + '02-csv/'
DIR03 = DATADIR + '03-bewerktedata/'
current_month = baglib.get_arg1(sys.argv, DIR02)
INPUTDIR = DIR02 + current_month + '/'
K3DIR = DIR03 + current_month + '/'
OUTPUTDIR = K3DIR
baglib.make_dir(OUTPUTDIR)
IN_VOORRAAD = ['inge', 'inni', 'verb', 'buig']
# BAGTYPES = ['vbo', 'lig', 'sta', 'pnd', 'num', 'opr', 'wpl']
LEES_BAG_OBJECTEN = ['vbo', 'num', 'opr', 'wpl', 'gemwpl', 'pnd']
LEES_K3_DF = ['vbovk_hoofdpndvk', 'vbovk_nvbo'] # K3 is KOPPELVLAK3
bagobj_d = {} # dict to store the bagobject df's
k3_d = {} # dict to store koppelvlak3 input df's: vbovk-pndvk
peildatum = baglib.last_day_of_month(current_month)
LOG_LVL = 1

veldnaam_d1 = {
    'vboid': 'OBJECTNUMMER',
    'vbovkbg': 'AANVLEVCYCLWOONNIETWOON',
    'vbovkeg': 'EINDLEVCYCLWOONNIETWOON',
    'dummy1': 'KENMERKWIJZIGDATUM',
    'invoorraad': 'INVOORRAAD',
    'voorraadtype': 'VBOVOORRAADTYPE',
    'bouwjaar': 'VBOBOUWJAAR',
    'typeinliggend': 'VBOTYPEINLIGGEND',
    'oppervlakte': 'VBOOPPERVLAKTE',
    'vbostatus': 'VBOSTATUS'}

gebruiksdoel_dict = {
    'woon': 'VBOWOONFUNCTIE',
    'over': 'VBOOVERIGE_GEBRUIKSFUNCTIE',
    'kant': 'VBOKANTOORFUNCTIE',
    'gezo': 'VBOGEZONDHEIDSFUNCTIE',
    'bij1': 'VBOBIJEENKOMSTFUNCTIE',
    'ondr': 'VBOONDERWIJSFUNCTIE',
    'wink': 'VBOWINKELFUNCTIE',
    'sprt': 'VBOSPORTFUNCTIE',
    'logi': 'VBOLOGIESFUNCTIE',
    'indu': 'VBOINDUSTRIEFUNCTIE',
    'celf': 'VBOCELFUNCTIE'}

# ############################################################################
print('\n1.----Inlezen bagobjecten die we nodig hebben----------------------')
# ############################################################################
print('\t1.1 in koppelvlak 2:', LEES_BAG_OBJECTEN)

'''
bagobj_d = read_dfs(INPUTDIR, LEES_BAG_OBJECTEN)
'''

for bagobj in LEES_BAG_OBJECTEN:
    print('\n', bagobj, 'in', INPUTDIR, 'inlezen...')
    bagobj_d[bagobj] = pd.read_csv(INPUTDIR + bagobj + '.csv')
    bagobj_d[bagobj] = bagobj_d[bagobj].astype(str)
    bagobj_d[bagobj][[bagobj + 'vkbg', bagobj + 'vkeg']] =\
        bagobj_d[bagobj][[bagobj + 'vkbg', bagobj + 'vkeg']].astype(int)

    if bagobj != 'gemwpl': # gemwpl has no vkid
        baglib.df_total_vs_key3(bagobj +'vk', bagobj_d[bagobj], 
                                [bagobj + 'id', bagobj + 'vkid'], LOG_LVL)
    # The following bagobj have no vk link in vbo, so linking is on active vk:
    # vbo -> num -> opr -> wpl -> gem
    if bagobj in ['num', 'opr', 'wpl', 'gemwpl']:
        print('\n\t\tActieve', bagobj, 'selecteren...')
        bagobj_d[bagobj] = baglib.select_active_vk(bagobj, bagobj_d[bagobj],
                                                   peildatum)
        bagobj_d[bagobj].drop([bagobj + 'vkeg', bagobj +'vkbg'],
                              axis=1, inplace=True)
        if bagobj != 'gemwpl':
            baglib.df_total_vs_key3('actieve ' + bagobj, bagobj_d[bagobj], 
                                    [bagobj + 'id', bagobj + 'vkid'],
                                    LOG_LVL)


print('\t1.1 in koppelvlak 3:', LEES_K3_DF)
for k3_df in LEES_K3_DF:
    print('\n\t', k3_df, 'in', K3DIR, 'inlezen...')
    k3_d[k3_df] = pd.read_csv(K3DIR + k3_df + '.csv')
    k3_d[k3_df] = k3_d[k3_df].astype(str)
    

# #############################################################################
print('\n2.----Verrijken van VBO met woonplaats en gemeente---------    ')
# #############################################################################
print('\n\tDe koppeling loopt als volgt: vbo -> num -> opr -> wpl -> gem\n',
      '\tOmdat het Kadaster de laatste vier bagobjecten aan elkaar linkt zonder\n',
      '\top het vk ervan te letten, nemen we voor deze laatste vier steeds\n',
      '\thet actieve vk. Alleen voor vbo houden we alle voorkomens in beeld.')


print('\n\t2.1 Bepaal eerst de koppeling num-gem:')
nowg_df = num2gem(bagobj_d)

print('\n\t2.2 Koppel num-gem aan vbovk')
vnowg_df = pd.merge(bagobj_d['vbo'], nowg_df, how='left')
baglib.df_total_vs_key2('vbo-num-opr-wpl-gem', vnowg_df, ['vboid', 'vbovkid'])


vnowg_df2 = vnowg_df[['vboid', 'vbovkid', 'vbostatus', 'gebruiksdoel',
                      'oppervlakte', 'gemid']].drop_duplicates()
baglib.df_total_vs_key2('vbovk waarin dubbele verwijderd', vnowg_df2,
                        ['vboid', 'vbovkid'])
vnowg_df2.dropna(subset=['gemid'], axis='rows', inplace=True)
baglib.df_total_vs_key2('vbovk waarin die zonder gemid verwijderd zijn',
                        vnowg_df2,
                        ['vboid', 'vbovkid'])

# #############################################################################
print('\n3.----Bepalen vbo in voorraad---------    ')
# #############################################################################
vbovk_voorraad_df = vnowg_df2.loc[vnowg_df2['vbostatus'].isin(IN_VOORRAAD)]
baglib.df_in_vs_out('voorraad', vnowg_df2, vbovk_voorraad_df)

# #############################################################################
print('\n4.----Bepalen bouwjaar van vbo in voorraad---------    ')
# #############################################################################
vbovk_bouwjaar_df = pd.merge(vnowg_df2, k3_d['vbovk_hoofdpndvk'], how='left')
baglib.df_in_vs_out('hoofdpnd bepaald', vnowg_df2, vbovk_bouwjaar_df)
print('DEBUG1', vbovk_bouwjaar_df.info())
'''
# #############################################################################
print('\n9.----Wegschrijven VBO vk---------------------------------------    ')
# #############################################################################

print('\n\t2.3 Wegschrijven naar vbo.csv', result_df.shape[0],
      'actuele verrijkte unieke vbovk')
outputfile = OUTPUTDIR + 'vbo.csv'
result_df.to_csv(outputfile, index=False)
print('DEBUG6: resultaat:')
print(result_df.info())
'''