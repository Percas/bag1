#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Functions used in different python scripts
"""
# ################ import libraries ###############################
import pandas as pd
# import time
import os
import sys
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
def get_arg1(arg_lst, ddir):
    _lst = os.listdir(ddir)
    if len(arg_lst) <= 1:
        sys.exit('Usage: ' + arg_lst[0] + '  <month>, where <month> in '
                 + str(_lst))
    _current_month = arg_lst[1]
    if _current_month not in _lst:
        sys.exit('Usage: ' + arg_lst[0] + '  <month>, where <month> in '
                 + str(_lst))
    return _current_month


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
    
    if nrec ==  0:
        return (_nrec, _nkey)
    
    if nrec != _nrec:
        print('\t\tAantal records in:', nrec, 'aantal uit:', _nrec)
    else:
        print('\t\tAantal input records ongewijzigd:\n\t\tRecords in = uit =',
              nrec)
    if nkey != _nkey:
        print('\t\tAantal vk in:', nkey,
              'aantal vk uit:', _nkey)
        if not u_may_change:
            print('FOUT: aantal unieke eenheden gewijzigd!')
    else:
        print('\t\tAantal vk ongewijzigd: vk in = uit =',
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
