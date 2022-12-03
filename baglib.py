#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Functions used in different python scripts
"""
# ################ import libraries ###############################
import pandas as pd
import time
# import logging
import os
import sys
# import datetime
# import logging
import numpy as np


# ################ define datastructures ###############################

BAG_TYPE_DICT = {'vboid': 'string',
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
                 'docdd': np.uintc,
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
                 'wplnaam': 'string',
                 'woon': bool,
                 'over': bool,
                 'kant': bool,
                 'gezo': bool,
                 'bij1': bool,
                 'ondr': bool,
                 'wink': bool,
                 'sprt': bool,
                 'logi': bool,
                 'indu': bool,
                 'celf': bool,
                 'vbogmlx': float,
                 'vbogmly': float,
                 'pndgmlx': float,
                 'pndgmly': float,
                 'liggmlx': float,
                 'liggmly': float,
                 'stagmlx': float,
                 'stagmly': float,
                 'inliggend': np.uintc                 
                 }



# ############### Define functions ################################
'''
def myprint(left_text, right_result):
    # """Print a text and the result."""
    # print(f"{left_text : <45}" + str(right_result))
    print(f"{left_text : <45}" + str(right_result))

def prtxt(astring):
    print(astring)


def get_df_from_csv(idir, csv_file, dtype_dict, cols):
    """Return dataframe from csv_file in dir_path."""
    try:
        # prtxt('Contents of this dir: ' +
        #          str(os.listdir(idir)))
        _df = read_csv(idir,
                       csv_file,
                       dtype_dict, cols)
        return _df
    except FileNotFoundError:
        prtxt('Error: kan dit bestand niet openen: ' +
                  idir + csv_file)
'''
def get_arg1(arg_lst, ddir):
    _lst = os.listdir(ddir)
    if len(arg_lst) <= 1:
        # sys.exit('Usage: ' + arg_lst[0] + '  <month>, where <month> in '
        #         + str(_lst))
        return "testdata"
    _current_month = arg_lst[1]
    if _current_month not in _lst:
        sys.exit('Usage: ' + arg_lst[0] + '  <month>, where <month> in '
                 + str(_lst))
    return _current_month

'''
def total_vs_index(df, printit):
    _uniq = df.index.drop_duplicates().shape[0]
    # print nr of records vs unique records in index.
    if printit:
        print('\t\tAantal records:        ', df.shape[0])
        # print('\t\tAantal uniek in index: ', df.index.unique().shape[0])
        print('\t\tAantal uniek in index: ', _uniq)
    return _uniq


def df_total_vs_key(bagobj_name, df, key_lst, result_dict):
    'rint number of (unique) records of df given key_lst are keys in df.
    _n_rec = df.shape[0]
    _n_rec_u = df[key_lst].drop_duplicates().shape[0]
    _diff = _n_rec - _n_rec_u
    # _perc = int(round(100 * _diff / _n_rec, 2))
    _perc = round(100 * _diff / _n_rec, 2)
    print('\t\tAantal records:         ', _n_rec,
          '\n\t\tAantal uniek:           ', _n_rec_u,
          '\n\t\tNiet uniek:             ', _diff,
          '\n\t\tPercentage niet uniek:  ', _perc)
    result_dict[bagobj_name + '_tot'] = _n_rec
    result_dict[bagobj_name + '_uniek'] = _n_rec_u
    result_dict[bagobj_name + '_verschil'] = _diff
    result_dict[bagobj_name + '_perc_niet_uniek'] = _perc
    return result_dict


def df_total_vs_key2(subject1, df, key_lst):
    'rint number of (unique) records of df given key_lst are keys in df.
    _n_rec = df.shape[0]
    _n_rec_u = df[key_lst].drop_duplicates().shape[0]
    _diff = _n_rec - _n_rec_u
    # _perc = int(round(100 * _diff / _n_rec, 2))
    _perc = round(100 * _diff / _n_rec, 2)
    print('\n\t\tInformatie over', subject1)
    print('\t\tAantal records:         ', _n_rec,
          '\n\t\tAantal uniek:           ', _n_rec_u,
          '\n\t\tNiet uniek:             ', _diff,
          '\n\t\tPercentage niet uniek:  ', _perc)


def df_in_vs_out(proces_name, df_in, df_out):
    _n_in = df_in.shape[0]
    _n_out = df_out.shape[0]
    _verschil = _n_in - _n_out
    _perc = round(100 * _verschil / _n_in, 3)
    print('\t\tRecords in:         ', _n_in)
    print('\t\tVerschil:           ', _verschil)
    print('\t\tRecords uit:        ', _n_out)
    print('\t\tPerc verschil:      ', _perc)
    
def vgl_dfs(proces_name, df_in, df_out, result_dict):
    _n_in = df_in.shape[0]
    _n_out = df_out.shape[0]
    _verschil = _n_in - _n_out
    _perc = round(100 * _verschil / _n_in, 3)
    print('\t\tRecords in:         ', _n_in)
    print('\t\tVerschil:           ', _verschil)
    print('\t\tRecords uit:        ', _n_out)
    print('\t\tPerc verschil:      ', _perc)
    result_dict[proces_name + '_in'] = _n_in
    result_dict[proces_name + '_uit'] = _n_out
    result_dict[proces_name + '_verschil'] = _verschil
    result_dict[proces_name + '_perc'] = _perc
    return result_dict

def vgl_dfs2(proces_name, df_in, df_out,
             rec_in   ='\t\tRecords in:\t\t',
             rec_out  ='\t\tRecords uit:\t',
             verschil ='\t\tVerschil:\t\t',
             pverschil='\t\t%Verschil:\t\t'):
    _n_in = df_in.shape[0]
    _n_out = df_out.shape[0]
    _verschil = _n_in - _n_out
    _perc = round(100 * _verschil / _n_in, 3)
    print(rec_in, _n_in)
    print(verschil, _verschil)
    print(rec_out, _n_out)
    print(pverschil, _perc)
    return (_n_in, _n_out)
    
    result_dict[proces_name + rec_in] = _n_in
    result_dict[proces_name + rec_out] = _n_out
    result_dict[proces_name + verschil] = _verschil
    result_dict[proces_name + pverschil] = _perc
'''

def diff_idx_df(df1, df2):
    '''Return tuple: (dfboth, df1not2, df2not1).'''
    print('\tdiff_idx_df: in beide, in 1 niet 2, in 2 niet 1:')
    _df = pd.concat([df1, df2])
    _dfboth = _df[~_df.index.duplicated(keep='first')]
    _df = pd.concat([df1, _dfboth])
    _df1not2 = _df[~_df.index.duplicated(keep=False)]
    _df = pd.concat([df2, _dfboth])
    _df2not1 = _df[~_df.index.duplicated(keep=False)]
    return (_dfboth, _df1not2, _df2not1)
    

def get_perc(df_in, df_out):
    _perc = round(100 * df_out.shape[0] / df_in.shape[0], 3)
    print('\t\tPercentage:', _perc, '%')
    return _perc

def fix_eendagsvlieg(df, b_str, e_str):
    """Return df with voorkomens removed that start and end on same day."""
    # print('\tVerwijderden van eendagsvliegen:')
    return df[df[b_str] < df[e_str]]

def print_omgeving(adir):
    if adir[-4:-1] == 'ont':
        print('\t---------------------------------')
        print('\t--------ONTWIKKELOMGEVING--------')
        print('\t---------------------------------\n')
    else:
        print('\t---------------------------------')
        print('\t--------PRODUCTIEOMGEVING--------')
        print('\t---------------------------------\n')


def select_active_vk(df, bagobj, idate):
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

    _nkey = df.shape[0]
    _navk = _df_active.shape[0]
    _davk = _navk / _nkey
    print('\t\tActieve voorkomens:   ', _navk)
    print('\t\tGedeelte actieve vk:', _davk)
    return _df_active


def last_day_of_month(month_str):
    '''Return the last day of the month as integer. Format: 20220331.'''
    _last = 100 * int(month_str) + 31 # most common situation
    m = int(month_str[-2:])
    if m in [4, 6, 9, 11]:
        _last -= 1 # compensate for months with 30 days
    if m == 2:
        _last -= 3 # compensate for februari, does not work in schrikkeljaar
    print('\tLaatste dag van de maand:', _last)
    return _last


def make_dir(path):
    if not os.path.exists(path):
        print('\n\tAanmaken outputmap', path)
        os.makedirs(path)

def recast_df_floats(df, dict1):
    '''Recast the float64 types in df to types in dict1 afer df.fillna(0)'''
    _float_cols = df.select_dtypes(include=['float']).columns
    print('\tDowncasting types of:', list(_float_cols))
    _type_dict = {k: dict1[k] for k in _float_cols}
    df[_float_cols] = df[_float_cols].fillna(0)
    # df[_float_cols] = df[_float_cols].astype(_type_dict)
    return df.astype(_type_dict)


def print_time(seconds, info, printit):
    '''Print toc-tic s if printit = True.'''
    if printit:
        print(info, seconds / 60, 'min\n')
        # print(info, time.strftime('%H:%M:%S', time.gmtime(int(seconds))))

def df_comp(df, key_lst=[], nrec=0, nkey=0, u_may_change=True):
    '''
    Check if df has n_rec records and n_rec_u unique records.
    Use key_lst to determine the unique keys. If empty use df.index.
    Return the triple (number of records, unique nr of rec, percentage unique).
    '''
    _nrec = df.shape[0]
    if key_lst == []:
        _nkey = df.index.drop_duplicates().shape[0]
    else:
        _nkey = df[key_lst].drop_duplicates().shape[0]
        # print('DEBUG df_comp')
        # print(df[key_lst].drop_duplicates())
        # print(df[key_lst].drop_duplicates().shape[0])
    
    if nrec ==  0:
        return (_nrec, _nkey)
    
    if nrec != _nrec:
        print('\t\tAantal records in:', nrec, 'aantal uit:', _nrec)
    else:
        print('\t\tAantal input records ongewijzigd:\n\t\tRecords in = uit =',
              nrec)
    if nkey != _nkey:
        print('\t\tAantal', key_lst,  'in:', nkey,
              'aantal', key_lst, 'uit:', _nkey)
        if not u_may_change:
            print('FOUT: aantal unieke eenheden gewijzigd!')
    else:
        print('\t\tAantal', key_lst, 'ongewijzigd: vk in = uit =',
              nkey)
    # in_equals_out = (_n_rec == n_rec) and (_n_rec_u == n_rec_u) 
    # print('\tNr of records in equals out:', in_equals_out)
    return (_nrec, _nkey)


    
def ontdubbel_idx_maxcol(df, max_cols):
    '''Ontdubbel op de idx van df door de grootste waarde in de 
    kolommen uit max_cols te nemen.'''
    _df = df.sort_values(axis=0, by=max_cols, ascending=False)
    return _df[~_df.index.duplicated(keep='first')] 


def read_input_csv(file_d, bag_type_d):
    '''Read the input csv files in file_d dict and return them in a dict of
    df. Use the bag_type_d dict to get the (memory) minimal types.'''
    _bdict = {}
    for _k, _f in file_d.items():
        print('\tInlezen', _k, '...')
        _bdict[_k] = pd.read_csv(_f, 
                                 dtype=bag_type_d,
                                 keep_default_na=False)
        # print('DEBUG:', _bdict[_k].info())
        # print('DEBUG:', _bdict[_k].head())
    return _bdict
        
def print_legenda():
    '''Print een paar veel voorkomende afkortingen.'''
    print(f'{"Legenda":~^80}')
    print(f'~\t{"vbo:  verblijfsobject":<38}', f'{"pnd:  pand":<38}')
    print(f'~\t{"vk:   voorkomen":<38}', f'{"pndvk:  pand voorkomen":<38}')
    print(f'~\t{"vkbg: voorkomen begindatum geldigheid":<38}',
          f'{"vkeg: voorkomen einddatum geldigheid":<38}')
    print(f'~\t{"n...:  aantal records in df":<38}',
          f'{"bob: bagobject":<38}')
    print(f'{"~":~>80}')


def ontdubbel_maxcol(df, subset, lowest):
    ''' Ontdubbel op subset door de laagste waarde van lowest te nemen.'''
    _df = df.sort_values(axis=0, by=lowest, ascending=True)
    return(_df[~_df.duplicated(subset=subset, keep='first')])


def select_vk_on_date(df, bg, eg, peildatum):
    """
    df is the dataframe of a bagobj. Every bagobj has subsequent voorkomens
    (vk), numbered 1, 2, 3 with subsequent intervals startdate, enddate (bg, eg)
    return a df with the vk of the bagobj on peildatum.
    """
    _mask = (df[bg] <= peildatum) & (peildatum < df[eg])
    return df[_mask]


def peildatum(df, subset, bg, eg, peildatum):
    ''' df is the dataframe of a bagobject. Determine all the bagobject 
    voorkomen on peildatum. If there's more than one vk, 
    take the most recent. This is done by using eg in the function 
    ontdubbel_maxcol'''
    _df = select_vk_on_date(df, bg, eg, peildatum)
    # print([bagobj, eg])
    # print(_df.info())
    return ontdubbel_maxcol(_df, subset, eg)


def makecounter(df, grouper, newname):
     ''' Add a column with name newname that is a counter 
     for the column grouper.''' 
     # make a new counter for vbovkid. call it vbovkid2
     tmp = df.groupby(grouper).cumcount()+1
     df[newname] = tmp.to_frame()
     return df
