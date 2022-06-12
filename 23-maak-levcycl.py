#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on June 11, 2022

Purpose: make the levenscyclus file for VBO: 
    vbovk met bouwjaar en typeinliggend
    restriction: vbovk must be in IN_VOORRAAD
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

    print('\n\tAdd gemid to wpl, link on wplid...')
    _res_df = pd.merge(bagobj_dict['wpl']['wplid'], 
                       bagobj_dict['gemwpl'][['wplid', 'gemid']],
                       how='left')
    baglib.df_total_vs_key2('wpl-gem', _res_df, ['wplid'])
    
    '''
    print('DEBUG1 wg_df:')
    print(wg_df.info())
    print(bagobj_d['opr'].info())
    '''
    
    print('\n\tAdd wplid, gemid to opr, link on wplid...')
    _res_df = pd.merge(bagobj_dict['opr'][['oprid', 'wplid']],
                       _res_df, 
                       how='left')
    baglib.df_total_vs_key2('opr-wpl-gem', _res_df, ['oprid'])
    
    '''
    print('DEBUG2: owg:')
    print(owg_df.info())
    print('DEBUG3: het bagobj num:')
    print(bagobj_d['num'].info())
    '''
    
    print('\n\tAdd oprid, wplid, gemid to num, link on oprid')
    _res_df = pd.merge(bagobj_dict['num'][['numid', 'oprid']], 
                       _res_df,
                       how='left')
    baglib.df_total_vs_key2('num-opr-wpl-gem', _res_df, ['numid'])
    
    '''
    print('DEBUG4: nowg_df')
    print(nowg_df.info())
    '''
    
    return _res_df    
    
def num2gem2(bagobj_dict):
    '''Link num-opr-wpl-gem based on input dict with these 4 df's.'''

    print('\n\tMake num-opr, link on oprid')
    _res_df = pd.merge(bagobj_dict['num'][['numid', 'oprid']], 
                       bagobj_dict['opr'][['oprid', 'wplid']],
                       how='left')
    baglib.df_total_vs_key2('num-opr', _res_df, ['numid'])
    '''
    print('DEBUG1 num-opr:')
    print(_res_df.info())
    print(bagobj_d['gemwpl'].info())
    '''
    print('\n\tMake num-opr-wpl-gem, link on wplid...')
    _res_df = pd.merge(_res_df,
                       bagobj_dict['gemwpl'][['wplid', 'gemid']],
                       how='left')
    baglib.df_total_vs_key2('num-opr-wpl-gem', _res_df, ['oprid'])

    return _res_df    
    
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
OUTPUTDIR = DIR03 + current_month + '/'
IN_VOORRAAD = ['inge', 'inni', 'verb', 'buig']
# BAGTYPES = ['vbo', 'lig', 'sta', 'pnd', 'num', 'opr', 'wpl']
BAG_OBJECTEN = ['vbo', 'num', 'opr', 'wpl', 'gemwpl']
bagobj_d = {} # dict to store the bagobject df's
peildatum = baglib.last_day_of_month(current_month)
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
print('\n----Inlezen bagobjecten die we nodig hebben------------------------')
# ############################################################################

for bagobj in BAG_OBJECTEN:
    print('\n', bagobj, 'in', INPUTDIR, ':')
    bagobj_d[bagobj] = pd.read_csv(INPUTDIR + bagobj + '.csv')
    if bagobj == 'gemwpl':
        bagobj['gemwpl']['wplvkid'] = bagobj['gemwpl'].groupby('wplid').cumcount()
    
    # if bagobj != 'gemwpl':  # gemwpl had no gemwplvkid
    baglib.df_total_vs_key2(bagobj +'vk', bagobj_d[bagobj], 
                            [bagobj + 'id', bagobj + 'vkid'])

    if bagobj in ['num', 'opr', 'wpl', 'gemwpl']:
        print('\n\t\tActieve', bagobj, 'selecteren...')
        bagobj_d[bagobj] = baglib.select_active_vk(bagobj, bagobj_d[bagobj],
                                                   peildatum)
        bagobj_d[bagobj].drop([bagobj + 'vkeg', bagobj +'vkbg'],
                              axis=1, inplace=True)
        if bagobj != 'gemwpl':
            baglib.df_total_vs_key2('actieve ' + bagobj, bagobj_d[bagobj], 
                                    [bagobj + 'id', bagobj + 'vkid'])

# #############################################################################
print('\n----Verrijken van VBO met woonplaats en gemeente---------    ')
# #############################################################################
print('\n\tDe koppeling loopt als volgt: vbo -> num -> opr -> wpl -> gem\n',
      '\tOmdat het Kadaster de laatste vier bagobjecten aan elkaar linkt zonder\n',
      '\top het vk ervan te letten, nemen we voor deze laatste vier steeds\n',
      '\thet actieve vk. Alleen voor vbo houden we alle voorkomens in beeld.')
print('\tWe koppelen de bag objecten erbij van rechts naar links:')


nowg_df = num2gem2(bagobj_d)


print('\n\tAdd numid, oprid, wplid, gemid to vbo, link on numid')
vnowg_df = pd.merge(bagobj_d['vbo'], nowg_df, how='left')
baglib.df_total_vs_key2('vbo-num-opr-wpl-gem', vnowg_df, ['vboid', 'vbovkid'])
# print(vnowg_df.info())


'''
print('DEBUG5 vnowg:')
print(vnowg_df.info())
'''


result_df = vnowg_df[['vboid', 'vbovkid', 'vbostatus', 'gebruiksdoel',
                      'oppervlakte', 'gemid']].drop_duplicates()
baglib.df_total_vs_key2('vbovk waarin dubbele verwijderd', result_df,
                        ['vboid', 'vbovkid'])
result_df.dropna(subset=['gemid'], axis='rows', inplace=True)
baglib.df_total_vs_key2('vbovk waarin die zonder gemid verwijderd zijn',
                        result_df,
                        ['vboid', 'vbovkid'])

# vnowg_df.drop(['numid', 'numvkid', 'typeao', 'oprid', 'oprstatus',
#                'oprvkid', 'pndid', 'numstatus'], axis=1, inplace=True)

print('\n\tWegschrijven naar vbo.csv', result_df.shape[0],
      'actuele verrijkte unieke vbovk')
outputfile = OUTPUTDIR + 'vbo.csv'
result_df.to_csv(outputfile, index=False)
print('DEBUG6: resultaat:')
print(result_df.info())
