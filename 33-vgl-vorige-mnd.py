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
0.4: clean. investigate and improve types of dataframe
    
"""
# ################ import libraries ###############################
import pandas as pd
import os
import sys
import baglib

# ############### Define functions ################################
# #############################################################################
print('----------------------------------------------------------------------')
print('- DOEL: vergelijken van de vbovk-hoofdpndvk koppeling')
# #############################################################################
print('- tussen een BAG extract van t-1 en t')
print('- We maken vier verzamelingen:\n',
      '- \t\tonn: oud niet nieuw: vbovk van t-1 niet in t\n',
      '- \t\tnno: nieuw niet oud: vbovk van t niet in t-1\n',
      '- \t\tpgelijk: vbovk in t-1 en t, waarbij pndvk gelijk blijft\n',
      '- \t\tpwissel: vbovk waarbij pndvk wisselt van t-1 op t\n\n',
      '- \t\tWe bewaren de vbovk uit t-1 en t met pndvk wisselingen.\n',
      '- \t\tWe berkenen percentages t.o.v. de vbovk van extract t:\n',
      '- \t\t\t% onn, % nno, % gelijke panden, % wisselende panden\n',
      '- \t\tWe tellen het aantal wisselingen panden (dus niet pndvk)\n',
      '- \t\tbouwjaar, documentnrs, zodat we kunnen zien of deze\n',
      '- \t\tenigszins constant zijn over de tijd')
print('----------------------------------------------------------------------')

# #############################################################################
print('\n0.............Variabelen initialiseren..............................')
# #############################################################################
# month and dirs
os.chdir('..')
BASEDIR = os.getcwd() + '/'
# print('\tBasisdirectory:', BASEDIR)
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
current_month = baglib.get_arg1(sys.argv, DIR03)
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
result_dict = {}

# #############################################################################
print('\n1.............Data inlezen...........................................')
# #############################################################################
print('\n\t---- 1a. vbovk_hoofdpndvk van', prev_month, '...')
prev_vbovk_hoofdpndvk_df = pd.read_csv(PREV_MONTH_INPUTDIR +\
                                       'vbovk_hoofdpndvk.csv', dtype=object)
baglib.df_total_vs_key2('prev_vbovk_hoofdpnd', 
                         prev_vbovk_hoofdpndvk_df,
                         ['vboid', 'vbovkid'])

print('\n\t---- 1b. vbovk_hoofdpndvk van', current_month, '...')
vbovk_hoofdpndvk_df = pd.read_csv(INPUTDIR +\
                                  'vbovk_hoofdpndvk.csv', dtype=object)
baglib.df_total_vs_key2('vbovk_hoofdpndvk', vbovk_hoofdpndvk_df,
                        ['vboid', 'vbovkid'])

print('\n\t---- 1c. Inlezen bestand pnd.csv uit koppelvlak 2')
pndvk_df = pd.read_csv(CSVDIR + 'pnd.csv',
                       dtype=object)

print('\n\t---- 1d. Inlezen bestand vbo.csv uit koppelvlak 2')
vbovk_df = pd.read_csv(CSVDIR + 'vbo.csv', dtype=object,
                       usecols=['vboid', 'vbovkid',
                                'vbovkbg', 'vbovkeg']).drop_duplicates()


# #############################################################################
print('\n2.............Data vergelijken.......................................')
# #############################################################################
print('\n\t---- 2a. vbovk vergelijken')
same_vbovk_df = pd.merge(prev_vbovk_hoofdpndvk_df[['vboid', 'vbovkid']],
                         vbovk_hoofdpndvk_df[['vboid', 'vbovkid']],
                         how='inner')
print('\tPercentage dezelfde vbovk in t-1 en t ten opzichte van t')
result_dict['perc_samevbovk'] = baglib.get_perc(vbovk_hoofdpndvk_df,
                                                same_vbovk_df)

onn_df = pd.concat([prev_vbovk_hoofdpndvk_df[['vboid', 'vbovkid']],
                   same_vbovk_df]).drop_duplicates(keep=False)
print('\tPercentage vbovk in t-1 en niet in t, ten opzichte van t')
result_dict['perc_onn'] = baglib.get_perc(vbovk_hoofdpndvk_df,
                                                onn_df)

nno_df = pd.concat([vbovk_hoofdpndvk_df[['vboid', 'vbovkid']],
                   same_vbovk_df]).drop_duplicates(keep=False)
print('\tPercentage vbovk niet in t-1 en wel in t, ten opzichte van t')
result_dict['perc_nno'] = baglib.get_perc(vbovk_hoofdpndvk_df,
                                          nno_df)

print('\n\t---- 2b. vbovk + pndvk vergelijken')
same_vbovk_pndvk_df = pd.merge(prev_vbovk_hoofdpndvk_df, vbovk_hoofdpndvk_df,
                               how='inner')
print('\tPercentage overeenkomende vbovk-pndvk t-1 en t tov t:')
result_dict['perc-oen-pndvk'] = baglib.get_perc(vbovk_hoofdpndvk_df,
                                                same_vbovk_pndvk_df)
'''
print('\n\tAantal overeenkomende vbovk-pndvk t-1 naar t:',
      same_vbovk_pndvk.shape[0])
print('\tDit is gelijk aan pwissel + pgeljk')
'''
vbovk_p_change = pd.concat([same_vbovk_df, same_vbovk_pndvk_df[['vboid',
                                                                'vbovkid']]])\
    .drop_duplicates(keep=False)

print('\tPercentage dat een wijzigend pndvk heeft:')
result_dict['perc-pndvk-wissel'] = baglib.get_perc(vbovk_hoofdpndvk_df,
                                                   vbovk_p_change)

# #############################################################################
print('\n3.............Output verrijken......................................')
# #############################################################################

print('\tWe starten met vbovk uit maand t-1 en t met wisselend pndvk')
print('\n\t---- 3a. data verrijken maand t =', current_month)
print('\tVoeg pndvk toe')
vbovk_pndvk_change = pd.merge(vbovk_p_change, vbovk_hoofdpndvk_df, how='inner')

print('\tvoeg verdere pnd info toe via pnd.csv:')
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

print('\n\t---- 3b. data verrijken maand t-1:', prev_month)
print('\tvoeg weer pndvk en pnd info toe')
vbovk_pndvk_change = pd.merge(vbovk_p_change, prev_vbovk_hoofdpndvk_df,
                              how='inner')
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

print('\n\t---- 3c. Voeg t-1 en t samen tot 1 record')
vbovk_pndwissel_tmin1_t = pd.merge(prev_vbovk_pndvk, cur_vbovk_pndvk,
                                   how='inner')

print('\tVoeg de vkbg en vkeg van het vbovk toe:')
vbovk_res_df = pd.merge(vbovk_pndwissel_tmin1_t,
                        vbovk_df[['vboid', 'vbovkid',
                                  'vbovkbg', 'vbovkeg']], how='inner')

print('Aantal vbovk met pndvk wissel:', vbovk_res_df.shape[0])

# #############################################################################
print('\n4.............Output maken: pndvk wisselingen opslaan................')
# #############################################################################
print('Als er pndid staat bedoelen we pndid en niet pndvk:')
for wsl in ['pndid', 'bouwjaar', 'pndstatus', 'pndvkid', 'docnr']:
    w_df = vbovk_res_df[vbovk_res_df[wsl + '_t-1'] != vbovk_res_df[wsl + '_t']]
    print('\tAantal', wsl, 'wisselingen:\t', w_df.shape[0])
    result_dict['perc-' + wsl + '-wissel'] =\
        baglib.get_perc(vbovk_hoofdpndvk_df, w_df)
    # result_dict[wsl + '_wissel'] = w_df.shape[0]

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
