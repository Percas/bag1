#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22

Purpose: make the following connections:
    vbovk - numvk
    numvk - oprvk
    oprvk  - wplvk
    
   
Purpose step 1: connect vbovk to numvk (instead of num)
step 1a: connect vbovk to all numvk of its num in num.csv
step 1b: select that numvk for which numvkbg < vbovkeg <= numvkeg 
    result: vbovk - numvk
Step 2: make numvk - oprvk (instead of numvk - opr)
Step 3: make oprvk - wplvk (instead of oprvk - wpl)
step 2b: connect numvk to all oprvk
0.1: first attempt to create microdata
0.2: make a VBO view for peilmomenten
0.3: connect active vbo to opr-wpl-gem

"""

# ################ import libraries ###############################
import pandas as pd
import numpy as np
import baglib
import os
import sys
import time

# import numpy as np


# ############### Define functions ################################
def my_leftmerge(left_df, right_df, check_nan_column=None):
    """
    Do a simple left merge including a checks on number of records.

    Returns
    -------
    The left merged dataframe.

    """
    _before = left_df.shape[0]
    _df = pd.merge(left_df, right_df, how='left')
    _after = _df.shape[0]
    if _before != _after:
        print('aantal records gewijzigd in merge. voor:', _before, 'na:',
              _after)
    if check_nan_column is not None:
        _empty_fields = _df[check_nan_column].isna().sum()
        _perc = round(100 * _empty_fields/_after, 2)
        if _empty_fields != 0:
            print('\tMerge resultaat heeft', _perc, 'procent lege velden')
    return _df


print('-----------------------------------------------------------')
print('----maak vbovk-numvk, numvk-oprvk, oprvk-wplvk---------------')
print('-----------------------------------------------------------')

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
'''
current_month = int(current_month)
current_year = int(current_month/100)
current_year = int(current_month/100)
loop_count = 0
'''
# _VOORRAAD = ['inge', 'inni', 'verb', 'buig']
# BAGTYPES = ['vbo', 'lig', 'sta', 'pnd', 'num', 'opr', 'wpl']
BOBJ = ['vbo', 'num', 'opr', 'wpl']
bdict = {} # dict to store the bagobject df's
# peildatum_i = 20220630
n_rec = {}
n_rec_u = {}
perc = {}

small_type_dict = {'vboid': 'string',
                   'pndid': 'string',
                   'numid': 'string',
                   'oprid': 'string',
                   'wplid': 'string',
                   'gemid': 'string',
                   'vbovkid': np.short,
                   'pndvkid': np.short,
                   'numvkid': np.short,
                   'oprvkid': np.short,
                   'wplvkid': np.short,
                   'vbovkbg': np.uintc, 
                   'vbovkeg': np.uintc,
                   'pndvkbg': np.uintc, 
                   'pndvkeg': np.uintc,
                   'numvkbg': np.uintc,
                   'numvkeg': np.uintc,
                   'oprvkbg': np.uintc, 
                   'oprvkeg': np.uintc,
                   'wplvkbg': np.uintc, 
                   'wplvkeg': np.uintc,
                   'vbostatus': 'category',
                   'pndstatus': 'category',
                   'numstatus': 'category',
                   'oprstatus': 'category',
                   'wplstatus': 'category',
                   'oprtype': 'category',
                   'gebruiksdoel': 'category',
                   'typeao': 'category',
                   'oppervlakte': np.uintc,
                   'bouwjaar': np.uintc,
                   'docnr': 'string',
                   'postcode': 'string',
                   'huisnr': 'string',
                   'oprnaam': 'string',
                   'wplnaam': 'string'
                   }
printit = True
# ############################################################################
print('----1. Inlezen bagobjecten ---------------------------------')
# #############################################################################
tic = time.perf_counter()

for bobj in BOBJ:
    print('\n', bobj, 'in', INPUTDIR, ':')
    bdict[bobj] = pd.read_csv(INPUTDIR + bobj + '.csv',
                              dtype=small_type_dict)
    # bdict[bobj].set_index([bobj+'id', bobj+'vkid'], inplace=True)
    key_lst = [bobj+'id', bobj+'vkid']
    (n_rec[bobj], n_rec_u[bobj], perc[bobj]) =\
        baglib.df_comp(bdict[bobj], key_lst=key_lst)
    print('\tAantal records:', n_rec[bobj])
    print('\tAantal vk:     ', n_rec_u[bobj])
    
    bdict[bobj] = baglib.fix_eendagsvlieg(bdict[bobj], bobj+'vkbg',
                                          bobj+'vkeg')
    (n_rec[bobj], n_rec_u[bobj], perc[bobj]) =\
        baglib.df_comp(bdict[bobj], key_lst=key_lst,
                       n_rec=n_rec[bobj], n_rec_u=n_rec_u[bobj])

    bobju = bobj + 'u'
    bdict[bobju] = bdict[bobj][key_lst].set_index(key_lst)
    bdict[bobju] = bdict[bobju][~bdict[bobju].index.duplicated()]
    print(bdict[bobju].info())
    # print('\tAantal records:', n_rec[bobj])
    # print('\tAantal vk:     ', n_rec_u[bobj])
    # print(bdict[bagobj].info())
    # print(bdict[bobj])

bdict['vbo'] = bdict['vbo'][['vboid', 'vbovkid', 'vbovkbg', 'vbovkeg', 'numid']]
bdict['num'] = bdict['num'][['numid', 'numvkid', 'numvkbg', 'numvkeg', 'oprid']]
bdict['opr'] = bdict['opr'][['oprid', 'oprvkid', 'oprvkbg', 'oprvkeg', 'wplid']]
bdict['wpl'] = bdict['wpl'][['wplid', 'wplvkid', 'wplvkbg', 'wplvkeg']]

'''
for bobj in BOBJ:
    print(bdict[bobj])
'''    
toc = time.perf_counter()
baglib.print_time(toc - tic, 'step 1 in', printit)

# #############################################################################
print('\n------2. Merging ---------------------------------------')
# #############################################################################
tic = time.perf_counter()

bdict['vbo'] = pd.merge(bdict['vbo'],
                        bdict['num'],
                        how='left',
                        left_on='numid',
                        right_on='numid')
(n_rec['vbo'], n_rec_u['vbo'], perc['vbo']) =\
    baglib.df_comp(bdict['vbo'], key_lst=['vboid', 'vbovkid'],
                   n_rec=n_rec['vbo'], n_rec_u=n_rec_u['vbo'],
                   u_may_change=False)

# print(bdict['vbo'].info())

toc = time.perf_counter()
baglib.print_time(toc - tic, 'step 2a in', printit)

# print(bdict['vbo'].info())

'''
msk = (bdict['vbo']['numvkbg'] < bdict['vbo']['vbovkeg']) &\
    (bdict['vbo']['vbovkeg'] <= bdict['vbo']['numvkeg'])
'''
msk = (bdict['vbo']['numvkbg'] <= bdict['vbo']['vbovkeg']) &\
    (bdict['vbo']['vbovkeg'] <= bdict['vbo']['numvkeg'])

tic = time.perf_counter()

bdict['vbo'] = bdict['vbo'][msk]

(n_rec['vbo'], n_rec_u['vbo'], perc['vbo']) =\
    baglib.df_comp(bdict['vbo'], key_lst=['vboid', 'vbovkid'],
                   n_rec=n_rec['vbo'], n_rec_u=n_rec_u['vbo'],
                   u_may_change=True)
toc = time.perf_counter()
baglib.print_time(toc - tic, 'step 2b masking in', printit)

print(bdict['vbo'].info())
key_lst = ['vboid', 'vbovkid']
print(bdict['vbo'][key_lst].drop_duplicates().info())
'''
202207 O omgeving:
    vbovk: 
        2485492 (na verwijderen eendagsvliegen)
        
        2483676 (na koppelen met numvk met msk < vbovkeg <=)
        1315466  (na koppelen met numvk met msk <= vbovkeg <)
        2484573  (na koppelen met numvk met msk <= vbovkeg <=)
            uniek!

----------------
(n_vbovk, doel2_vbovk_u, vbovk_perc) = \
    baglib.df_comp(vbovk_df, n_rec=n_vbovk, n_rec_u=n_vbovk_u,
                   u_may_change=True)

verschil_u =  n_vbovk_u - doel2_vbovk_u
n_vbovk_u = doel2_vbovk_u



'''
'''


# #############################################################################
print('\ngemwpl in', INPUTDIR)
# #############################################################################
gemwpl_df = pd.read_csv(INPUTDIR + 'gemwpl.csv')
baglib.df_total_vs_key2('gemwpl', gemwpl_df, ['wplid', 'gemwplvkbg'])
print('\tActieve relaties gemwpl selecteren...')
gemwpl_df = baglib.select_active_vk('gemwpl', gemwpl_df, peildatum_i)
# active_gemwpl_df.drop(['vkeg', 'vkbg'], axis=1, inplace=True)
# active_gemwpl_df.rename(columns={'status': 'gemwplstatus'},
#                         inplace=True)

# #############################################################################
print('\n----Verrijken van VBO met woonplaats en gemeente---------    ')
# #############################################################################

print('\n\tAdd gemid to wpl, link on wplid...')
# wg_df = my_leftmerge(bdict['wpl'], gemwpl_df, 'gemid')

wg_df = pd.merge(bdict['wpl'], gemwpl_df[['wplid', 'gemid']], how='left')
baglib.df_total_vs_key2('wpl-gem', wg_df, ['wplid'])


print('\n\tAdd wplid, gemid to opr, link on wplid...')
owg_df = my_leftmerge(bdict['opr'], wg_df, 'gemid')
baglib.df_total_vs_key2('opr-wpl-gem', owg_df, ['oprid', 'oprvkid'])
# print('DEBUG1', owg_df.info())

print('\n\tAdd oprid, wplid, gemid to num, link on oprid')
nowg_df = my_leftmerge(bdict['num'], owg_df, 'wplid')
baglib.df_total_vs_key2('num-opr-wpl-gem', nowg_df, ['numid', 'numvkid'])
# print('DEBUG2', nowg_df.info())

print('\n\tAdd numid, oprid, wplid, gemid to vbo, link on numid')
vnowg_df = my_leftmerge(bdict['vbo'], nowg_df, 'oprid')
baglib.df_total_vs_key2('vbo-num-opr-wpl-gem', vnowg_df, ['vboid', 'vbovkid'])
# print(vnowg_df.info())
# print('DEBUG3', vnowg_df.info())
result_df = vnowg_df[['vboid', 'vbovkid', 'vbostatus', 'gebruiksdoel',
                      'oppervlakte', 'postcode', 'huisnr', 'oprid',
                      'wplid', 'gemid']].drop_duplicates()
baglib.df_total_vs_key2('resultaatbestand met vbovk', result_df,
                        ['vboid', 'vbovkid'])


# vnowg_df.drop(['numid', 'numvkid', 'typeao', 'oprid', 'oprstatus',
#                'oprvkid', 'pndid', 'numstatus'], axis=1, inplace=True)
print('\n\tWegschrijven naar vbo.csv', result_df.shape[0],
      'actuele verrijkte unieke vbovk')
outputfile = OUTPUTDIR + 'vbo_wpl.csv'
# vnowg_df.dropna().to_csv(outputfile, index=False)
result_df.to_csv(outputfile, index=False)
'''
