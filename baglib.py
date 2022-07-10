#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Functions used in different python scripts
"""
# ################ import libraries ###############################
import pandas as pd
# import logging
import os
import sys
# import datetime
# import logging

# ############### Define functions ################################
def myprint(left_text, right_result):
    """Print a text and the result."""
    # print(f"{left_text : <45}" + str(right_result))
    print(f"{left_text : <45}" + str(right_result))

def prtxt(astring):
    print(astring)

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

def df_total_vs_key(bagobj_name, df, key_lst, result_dict):
    '''Print number of (unique) records of df given key_lst are keys in df.'''
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
    '''Print number of (unique) records of df given key_lst are keys in df.'''
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

def df_total_vs_key3(subject1, df, key_lst, log_lvl):
    '''Print number of (unique) records of df given key_lst are keys in df.'''
    if log_lvl > 0:
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
    '''
    result_dict[proces_name + rec_in] = _n_in
    result_dict[proces_name + rec_out] = _n_out
    result_dict[proces_name + verschil] = _verschil
    result_dict[proces_name + pverschil] = _perc
    '''

def get_perc(df_in, df_out):
    _perc = round(100 * df_out.shape[0] / df_in.shape[0], 3)
    print('\t\tPercentage:', _perc, '%')
    return _perc

def fix_eendagsvlieg(name, df, b_str, e_str):
    """Return df with voorkomens removed that start and end on same day."""
    _df = df[df[b_str] < df[e_str]]
    df_in_vs_out(name + '_1dv', df, _df)
    return _df


def print_omgeving(adir):
    if adir[-4:-1] == 'ont':
        print('\t\t\t---------------------------------')
        print('\t\t\t--------ONTWIKKELOMGEVING--------')
        print('\t\t\t---------------------------------\n')
    else:
        print('\t\t\t---------------------------------')
        print('\t\t\t--------PRODUCTIEOMGEVING--------')
        print('\t\t\t---------------------------------\n')


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
    print('\t\tActieve voorkomens:   ', _all_active_voorkomens)
    print('\t\tPercentage actieve vk:', _perc_active, '%')
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
