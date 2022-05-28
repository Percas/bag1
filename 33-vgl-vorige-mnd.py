#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  7 09:40:35 2022
@author: anton
Purpose: Find the differences between sucessive months of the vbovk-pndvk

0.1: introduce python logging to screen and to file (with socratica's help)
0.2: clean up
0.3: verschillen classicieren:
    zelfde pnd, maar nieuw pndv vs echt ander pnd
    
"""
# ################ import libraries ###############################
import pandas as pd
import os
import sys
# import datetime
import bagpy

# ############### Define functions ################################
# #############################################################################
print('----------------------------------------------------------------------')
print('- DOEL: vergelijken aantallen t-1 en t van vbovk-hoofdpndvk')
# #############################################################################
print('- We maken vier verzamelingen:\n',
      '- \t\tonn: old not new: vbovk van t-1 niet in t\n',
      '- \t\tnno: new not old: vbovk van t niet in t-1\n',
      '- \t\tpgelijk: vbovk in t-1 en t, waarbij pndvk gelijk blijft\n',
      '- \t\tpwissel: vbovk waarbij pndvk wisselt van t-1 op t\n\n',
      '- \t\tWe bewaren de laatste verzameling. Van alle verzamelingen\n',
      '- \t\tzullen we aantallen en percentages berekenen. Het is aannemelijk\n',
      '- \t\tdat deze percentages redelijk constant zullen zijn over de tijd')
print('----------------------------------------------------------------------')

# #############################################################################
print('\n0.............Variabelen initialiseren..............................')
# #############################################################################
# month and dirs
os.chdir('..')
BASEDIR = os.getcwd() + '/'
print('\tBasisdirectory:', BASEDIR)
if BASEDIR[-4:-1] == 'ont':
    print('\t\t\t---------------------------------')
    print('\t\t\t--------ONTWIKKELOMGEVING--------')
    print('\t\t\t---------------------------------')
else:
    print('\t\t\t---------------------------------')
    print('\t\t\t--------PRODUCTIEOMGEVING--------')
    print('\t\t\t---------------------------------')

DATADIR = BASEDIR + 'data/'
DIR02 = DATADIR + '02-csv/'
DIR03 = DATADIR + '03-bewerktedata/'
DIR04 = DATADIR + '04-ana/'
current_month = bagpy.get_arg1(sys.argv, DIR03)
CSVDIR = DIR02 + str(current_month) + '/'
INPUTDIR = DIR03 + current_month + '/'
OUTPUTDIR = DIR04 + current_month + '/'
current_month = int(current_month)
current_year = int(current_month/100)
prev_month = current_month - 1
# If current month is january, then prev_month now ends with 00.
if prev_month / 100 == int(prev_month / 100):
    prev_month -= 88  # prev from 202201 is 202200 - 88 = 202112
PREV_MONTH_INPUTDIR = DIR03 + str(prev_month) + '/'
print('\tVerwerkingsmaand t:', current_month, '. t-1:', prev_month)
# print('Previous month output dir: ' + PREV_MONTH_OUTPUTDIR)

# cols = ['id', 'vkid', 'pndid', 'pndvkid']
result_dict = {}

# #############################################################################
print('\n1.............Data inlezen...........................................')
# #############################################################################
print('\tvbovk_hoofdpndvk van', prev_month, '...')
prev_vbovk_hoofdpnd_df = pd.read_csv(PREV_MONTH_INPUTDIR +\
                                     'vbovk_hoofdpndvk.csv',
                                     dtype={'id': str, 'vkid': int,
                                      'pndid': str, 'pndvkid': int})
result_dict = bagpy.df_total_vs_key('prev_vbovk_hoofdpnd', 
                                    prev_vbovk_hoofdpnd_df,
                                    ['vboid', 'vbovkid'], result_dict)
print('\tvbovk_hoofdpndvk van', current_month, '...')
vbovk_hoofdpnd_df = pd.read_csv(INPUTDIR +\
                                'vbovk_hoofdpndvk.csv',
                                dtype={'id': str, 'vkid': int,
                                       'pndid': str, 'pndvkid': int})
result_dict = bagpy.df_total_vs_key('vbovk_hoofdpnd', vbovk_hoofdpnd_df,
                                    ['vboid', 'vbovkid'], result_dict)

print('\tInlezen bestand pnd.csv uit koppelvlak 2:')
pndvk_df = pd.read_csv(CSVDIR + 'pnd.csv',
                       dtype={'pndid': str, 'pndvkid': int})

print('\tInlezen bestand vbo.csv uit koppelvlak 2:')
vbovk_df = pd.read_csv(CSVDIR + 'vbo.csv',
                       dtype={'vboid': str, 'vbovkid': int})
print('vbovk_df:', vbovk_df.info())
vbovk_df['vboid'] = vbovk_df['vboid'].astype('int')
print('vbovk_df:', vbovk_df.info())
# #############################################################################
print('\n2.............Data vergelijken.......................................')
# #############################################################################
same_vbovk = pd.merge(prev_vbovk_hoofdpnd_df[['vboid', 'vbovkid']],
                      vbovk_hoofdpnd_df[['vboid', 'vbovkid']],
                      how='inner')
print('same_vbovk:\n:', same_vbovk.info())
print('\tAantal overeenkomende vbovk t-1 en t:', same_vbovk.shape[0])
print('\tWe nemen dit aantal in het vervolg als basis (in de noemer)')

print('\n\tonn: in "Records uit" staat het aantal records oud niet nieuw:')
onn_df = pd.concat([prev_vbovk_hoofdpnd_df[['vboid', 'vbovkid']],
                   same_vbovk]).drop_duplicates(keep=False)
result_dict = bagpy.vgl_dfs('onn', vbovk_hoofdpnd_df,
                             onn_df, result_dict)

print('\n\tnno: in "Records uit" staat het aantal records nieuw niet oud:')
nno_df = pd.concat([vbovk_hoofdpnd_df[['vboid', 'vbovkid']],
                   same_vbovk]).drop_duplicates(keep=False)
result_dict = bagpy.vgl_dfs('nno', vbovk_hoofdpnd_df,
                             nno_df, result_dict)


same_vbovk_pndvk = pd.merge(prev_vbovk_hoofdpnd_df, vbovk_hoofdpnd_df,
                            how='inner')
print('\tAantal overeenkomende vbovk-pndvk t-1 naar t:',
      same_vbovk_pndvk.shape[0])
print('\tDit is gelijk aan pwissel + pgeljk')

print('\n\tpwissel: Aantal vbovk met pnd wissel staat in "Records uit,"\n',
      '\tpgelijk: vbovk met pnd gelijkgebleven staat in "Verschil":')
vbovk_p_change = pd.concat([same_vbovk, same_vbovk_pndvk[['vboid',
                                                          'vbovkid']]])\
    .drop_duplicates(keep=False)
result_dict = bagpy.vgl_dfs('pgelijk', vbovk_hoofdpnd_df,
                            vbovk_p_change, result_dict)
print('vbovk_p_change:\n', vbovk_p_change.info())

# #############################################################################
print('\n3.............Output maken: pnd wisselingen opslaan................')
# #############################################################################

print('\n\t----Eerst maand t:', current_month)
print('\tVoeg pndvk toe aan die vbovk met wisselend pnd:')
vbovk_pndvk_change = pd.merge(vbovk_p_change, vbovk_hoofdpnd_df, how='inner')
bagpy.vgl_dfs('checkgelijk1:', vbovk_p_change, vbovk_pndvk_change, result_dict)

print('\tmerge met pnd.csv uit koppelvlak 2 en controleer aantal records:')
cur_vbovk_pndvk = pd.merge(vbovk_pndvk_change,
                           pndvk_df[['pndid', 'pndvkid', 'pndstatus',
                                     'pndvkbg', 'pndvkeg', 'docnr',
                                     'bouwjaar']], how='inner')
cur_vbovk_pndvk.rename(columns={'pndid': 'pndid_t', 'pndvkid': 'pndvkid_t',
                                'pndvkbg': 'pndvkbg_t',
                                'pndvkeg': 'pndvkeg_t',
                                'docnr': 'docnr_t',
                                'pndstatus': 'pndstatus_t', 
                                'bouwjaar': 'bouwjaar_t'}, inplace=True)
bagpy.vgl_dfs('checkgelijk2:', vbovk_pndvk_change, cur_vbovk_pndvk,
              result_dict)

print('\n\t----Vervolgens maand t-1:', prev_month)
vbovk_pndvk_change = pd.merge(vbovk_p_change, prev_vbovk_hoofdpnd_df,
                              how='inner')
bagpy.vgl_dfs('checkgelijk1:', vbovk_p_change, vbovk_pndvk_change,
              result_dict)

print('\tmerge met pnd.csv uit koppelvlak 2 en controleer aantal records:')
prev_vbovk_pndvk = pd.merge(vbovk_pndvk_change,
                            pndvk_df[['pndid', 'pndvkid', 'pndstatus',
                                      'pndvkbg', 'pndvkeg', 'docnr',
                                      'bouwjaar']], how='inner')
prev_vbovk_pndvk.rename(columns={'pndid': 'pndid_t-1',
                                 'pndvkid': 'pndvkid_t-1',
                                 'pndvkbg': 'pndvkbg_t-1',
                                 'pndvkeg': 'pndvkeg_t-1',
                                 'docnr': 'docnr_t-1',
                                 'pndstatus': 'pndstatus_t-1', 
                                 'bouwjaar': 'bouwjaar_t-1'},
                        inplace=True)
bagpy.vgl_dfs('checkgelijk2:', vbovk_pndvk_change, prev_vbovk_pndvk,
              result_dict)

# print(cur_vbovk_pndvk.info())
print('\tMerge t-1 met t')
vbovk_pndwissel_tmin1_t = pd.merge(prev_vbovk_pndvk, cur_vbovk_pndvk,
                                   how='inner')
bagpy.vgl_dfs('checkgelijk3:', prev_vbovk_pndvk, vbovk_pndwissel_tmin1_t,
              result_dict)
print(vbovk_pndwissel_tmin1_t.info())

print('\tMerge met vbovk:')
vbovk_res_df = pd.merge(vbovk_pndwissel_tmin1_t,
                        vbovk_df[['vboid', 'vbovkid',
                                  'vbovkbg', 'vbovkeg']], how='inner')

print('Merge met vbovk zodat we vbovkbg en vbovkeg hebben')
bagpy.vgl_dfs('checkgelijk4', vbovk_pndwissel_tmin1_t, vbovk_res_df,
              result_dict)

print('\tWegschrijven vbovk_pndwissel_tmin1_t in koppelvlak 4...')
outputfile = OUTPUTDIR + 'vbovk_res.csv'
vbovk_res_df.to_csv(outputfile, index=False)


# #############################################################################
print('\n----6. Samenvatting: bewaren van de belangrijkste kerngetallen------')
# #############################################################################

bepaal_hoofdpand_kerngetallen_file = DIR03 +\
     'bepaal_pandwissel_kerngetallen.csv'
current_month = str(current_month)
try:
    result_df = pd.read_csv(bepaal_hoofdpand_kerngetallen_file)
except:
    result_df = pd.DataFrame(list(result_dict.items()),
                              columns=['Maand', current_month])
else:
    current_df = pd.DataFrame(list(result_dict.items()),
                              columns=['Maand', current_month])
    result_df[current_month] = current_df[current_month]

outputfile = DIR03 + 'bepaal_pandwissel_kerngetallen.csv'
result_df.to_csv(outputfile, index=False)
print(result_df)
