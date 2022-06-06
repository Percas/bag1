#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22

Purpose: make a count of voorraad and other VBO stuff
0.1: first attempt to create microdata
0.2: make a VBO view for peilmomenten

"""

# ################ import libraries ###############################
import pandas as pd
import baglib
import os
import sys
# import numpy as np


# ############### Define functions ################################
def select_active_vk(bagobj, df, idate):
    """
    Select active voorkomens of df on idate.

    Returns
    -------
    subset of df of active voorkomens. Active on idate.

    Select id,vkid of those records of df with vkbg <= idate <= vkeg.

    Parameters
    ----------
    df : dataframe with column names vkbg and vkeg
        both these fields are 8-digit In664 of format YYYYMMDD
    idate : int64
        8-digit Integer of format YYYYMMDD
    Returns
    -------
    returns id, vkid those records with vkbg <= idate <= vkeg and the
    highest vkid. This is the active one

    """
    _mask = (df[bagobj + 'vkbg'] <= idate) & (idate < df[bagobj + 'vkeg'])
    _df_active = df[_mask].copy()

    _all_voorkomens = df.shape[0]
    _all_active_voorkomens = _df_active.shape[0]
    _perc_active = round(100 * _all_active_voorkomens / _all_voorkomens, 2)
    print('\tActieve voorkomens:                   ', _all_active_voorkomens)
    print('\tPercentage actieve voorkomens tov tot:', _perc_active, '%')
    # ################# LOGGING ################
    return _df_active



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
print('----Tel de woningvoorraad in Nederland---------------------')
print('-----------------------------------------------------------')

#
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
IN_VOORRAAD = ['inge', 'inni', 'verb', 'buig']
BAGTYPES = ['vbo', 'lig', 'sta', 'pnd', 'num', 'opr', 'wpl']
bagobj_d = {} # dict to store the 7 bagobject df's
peildatum_i = 20220331

# ############################################################################
print('----Inlezen bagobjecten en selecteren actieve------------')
# #############################################################################

for bagobj in BAGTYPES:
    print('\n', bagobj, 'in', INPUTDIR, ':')
    bagobj_d[bagobj] = pd.read_csv(INPUTDIR + bagobj + '.csv')
    baglib.df_total_vs_key2(bagobj, bagobj_d[bagobj], 
                            [bagobj + 'id', bagobj + 'vkid'])
    print('\tActieve', bagobj, 'selecteren...')
    bagobj_d[bagobj] = select_active_vk(bagobj, bagobj_d[bagobj],
                                        peildatum_i)
    bagobj_d[bagobj].drop([bagobj + 'vkeg', bagobj +'vkbg'],
                          axis=1, inplace=True)
    baglib.df_total_vs_key2(bagobj, bagobj_d[bagobj], 
                            [bagobj + 'id', bagobj + 'vkid'])

# #############################################################################
print('\ngemwpl in', INPUTDIR)
# #############################################################################
gemwpl_df = pd.read_csv(INPUTDIR + 'gemwpl.csv')
baglib.df_total_vs_key2('gemwpl', gemwpl_df, ['wplid', 'wplvkbg'])
print('\tActieve relaties gemwpl selecteren...')
gemwpl_df = select_active_vk('wpl', gemwpl_df, peildatum_i)
# active_gemwpl_df.drop(['vkeg', 'vkbg'], axis=1, inplace=True)
# active_gemwpl_df.rename(columns={'status': 'gemwplstatus'},
#                         inplace=True)

# #############################################################################
print('\n----Verrijken van VBO met woonplaats en gemeente---------    ')
# #############################################################################

print('\n\tAdd gemid to wpl, link on wplid...')
# wg_df = my_leftmerge(bagobj_d['wpl'], gemwpl_df, 'gemid')

wg_df = pd.merge(bagobj_d['wpl'], gemwpl_df[['wplid', 'gemid']], how='left')
baglib.df_total_vs_key2('wpl-gem', wg_df, ['wplid'])


print('\n\tAdd wplid, gemid to opr, link on wplid...')
owg_df = my_leftmerge(bagobj_d['opr'], wg_df, 'gemid')
baglib.df_total_vs_key2('opr-wpl-gem', owg_df, ['oprid', 'oprvkid'])
print('DEBUG1', owg_df.info())

print('\n\tAdd oprid, wplid, gemid to num, link on oprid')
nowg_df = my_leftmerge(bagobj_d['num'], owg_df, 'wplid')
baglib.df_total_vs_key2('num-opr-wpl-gem', nowg_df, ['numid', 'numvkid'])
print('DEBUG2', nowg_df.info())

print('\n\tAdd numid, oprid, wplid, gemid to vbo, link on numid')
vnowg_df = my_leftmerge(bagobj_d['vbo'], nowg_df, 'oprid')
baglib.df_total_vs_key2('vbo-num-opr-wpl-gem', vnowg_df, ['vboid', 'vbovkid'])
# print(vnowg_df.info())
print('DEBUG3', vnowg_df.info())

vnowg_df.drop(['numid', 'numvkid', 'typeao', 'oprid', 'oprstatus',
               'oprvkid'], axis=1, inplace=True)
outputfile = OUTPUTDIR + 'vbo.csv'
# vnowg_df.dropna().to_csv(outputfile, index=False)
vnowg_df.to_csv(outputfile, index=False)
'''
'''