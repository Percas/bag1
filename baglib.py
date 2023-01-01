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
def assigniffound(node,
                  taglist,
                  namespace,
                  value_if_not_found=None):
    '''
        Parameters
    ----------
    node: elem element
       the node of the subtree to be searched
    taglist : list of strings
       list of tags containing the tages of the nodes to be processed
       while going one level deeper in the xml tree
    namespace : string
        the dictionary ns with the namespace stuff
    value_if_not_found: if the tag in the taglist is not found, assigniffound
        returns this value

    Returns
    -------
    assignifffound digs through the xml tree until it reaches the node with
    the last tagname. If this node is found, its .text property is returned,
    if not the parameter value_if_not_found is returned


           level1 = level0.find('Objecten:voorkomen', ns)
           if level1 is not None:
               level2 = level1.find('Historie:Voorkomen', ns)
               if level2 is not None:
                   vkid = assignifexist(level2, 'Historie:voorkomenidentificatie', ns)
                   vkbg = assignifexist(level2, 'Historie:beginGeldigheid', ns)
                   vkegexist = level2.find('Historie:eindGeldigheid', ns)
                   if vkegexist is not None:
                       vkeg = vkegexist.text
                   else:
                       vkeg = '2999-01-01'
    Example taglist:
    _taglist = ['Objecten:voorkomen',
                'Historie:Voorkomen',
                'Historie:voorkomenidentificatie']
    '''

    i = 0
    current_level = node
    while i < len(taglist):
        level_down = current_level.find(taglist[i], namespace)
        if level_down is not None:
            i += 1
            current_level = level_down
        else:
            return value_if_not_found
    return current_level.text

def date2int(_date_str):
    '''
    Parameters
    ----------
    datestring : string
        string of format 2019-03-24
    Returns
    -------
    the integer 20190314
    '''
    _str = _date_str.replace('-', '')
    return int(_str)


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
    _key_lst = key_lst
    
    if key_lst == []:
        _nkey = df.index.drop_duplicates().shape[0]
        _key_lst = df.index.names
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
        print('\t\tAantal eenheden in:', nkey,
              'aantal', _key_lst, 'uit:', _nkey)
        if not u_may_change:
            print('FOUT: aantal unieke eenheden gewijzigd!')
    else:
        print('\t\tAantal', _key_lst, 'ongewijzigd: vk in = uit =',
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


def makecounter(df, grouper, newname, sortlist):
     ''' Add a column with name newname that is a counter 
     for the column grouper.''' 
     # make a new counter for vbovkid. call it vbovkid2
     # tmp = df.groupby(grouper).cumcount()+1
     # print('DEBUG', df.info())
     # print('DEBUG', sortlist)
     # tmp = df.sort_values(by=sortlist, axis=0)
     
     tmp = df.groupby(grouper).cumcount()+1
     df[newname] = tmp.to_frame()
     return df


def debugprint(title='', df='vbo_df', colname='vboid',
               vals=[], sort_on=[], debuglevel=10):
    '''print a the lines of the df where df[colname]==val.'''
    if debuglevel > 10:
        print('\n\t\tDEBUGPRINT:', title)
        _df = df.loc[df[colname].isin(vals)].sort_values(by=sort_on)        
        print('\t\tAantal records:', _df.shape[0], '\n')
        print(_df.to_string(index=False))
    print()



def vksplitter(df='vbo_df', gf='pnd_df', fijntype ='vbo', groftype = 'pnd', 
               future_date = 20321231, test_d={}):
    
    '''Splits voorkomens (vk) voor df, zodat elk vk van df in een gf vk past.
    df is het "fijntype" en gf is het "groftype".
    
    dfid identificeert de eenheden van df, bijvoorbeeld vbo, 
    waarvan de vk als records in df staan. gfid idem voor gf (pnd)

    input: beschouw twee vk van een pand (gfid) en 1 vk van een vbo (dfid)
    gfid:            o------------------oo-------------o
    dfid:                   o------------------o

    output: het vbovk wordt gesplitst zodat het binnen een pnd vk valt
    gfid:            o------------------oo-------------o
    dfid:                   o-----------oo-------o
    
    dfvkbg is de kolom met de vk begindatums van df.
    gfvkbg idem voor gf.
    
    We gaan dit aanpakken door de begindatums van de vk (dfvkbg en gfvkbg)
    in 'e'en kolom te verzamelen en dit te sorteren en ontdubbelen per dfid
    
    uitgangspunt is om geen nieuwe vk aan te maken v'o'or het eerste vk 
    van dfid.
 
    Voorwaarden: de kolommen in de volgende 7 assignments moeten bestaan
    in de dataframes df en gf:
    
 '''
    print('-----------------------------------------------------------')   
    print('------ Start vksplitter; fijntype:', fijntype, ', groftype:', 
          groftype)   
    print('-----------------------------------------------------------')   
    # de volgende kolommen moeten bestaan voor fijntype en groftype:
    dfid = fijntype + 'id'
    dfvkbg = fijntype + 'vkbg'
    dfvkeg = fijntype + 'vkeg'
    dfvkid = fijntype + 'vkid'
    gfid = groftype + 'id'
    gfvkbg = groftype + 'vkbg'
    gfvkeg = groftype + 'vkeg'
    gfvkid = groftype + 'vkid'
    
    # deze kolom gaan we maken om de nieuwe vk te identificeren:
    dfvkid2 = fijntype + 'vkid2'
    # aantallen in de uitgangssituatie     

    # print('DDDDDEBBBBBUUUUG', [dfid, dfvkid])

    (_nrec1, _nkey1) = df_comp(df, key_lst=[dfid, dfvkid])
    _debuglevel = 0

    
    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 1\n----',
          'Verzamel de vkbg van', fijntype, 'en', groftype, 'in _df en maak',
          'hiermee nieuwe', fijntype, 'vk\n')
    # #############################################################################

    print('\t\tWe beginnen de reguliere vk van', fijntype, 'gebaseerd op',
          dfvkbg, '\n\t\tHet bestaande', dfvkid, 'gaat straks een rol spelen bij het imputeren')
    
    (_nrec, _nkey) = df_comp(df=df, key_lst=[dfid, dfvkbg])
    cols = [dfid, dfvkid, dfvkbg]
    _df = df[cols].drop_duplicates()
    (_nrec, _nkey) = df_comp(df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, u_may_change=True)
    
    debugprint(title='1. drie '+dfid, df=_df, colname=dfid,
               vals=test_d[dfid], sort_on=cols, debuglevel=_debuglevel)


   # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 2\n----',
          'Voeg van', groftype, 'de', gfvkbg, 'aan toe. Hiervoor',
          'hebben we de koppeling '+fijntype+'-'+groftype, 'nodig,\n plus',
          'de', gfvkbg, 'van', groftype)
    # #############################################################################
    
    _gf = pd.merge(gf[[gfid, gfvkbg]], 
                   df[[dfid, gfid]].drop_duplicates(),
                   how='inner', on=gfid)
    # print(_gf.head(50))
    
    cols = [dfid, gfvkbg]
    debugprint(title='2a. drie '+dfid+' met '+gfvkbg+' van de bijbehorende '+gfid,
               df=_gf, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)

    cols = [dfid, dfvkbg]
    _gf = _gf[[dfid, gfvkbg]].drop_duplicates().rename({gfvkbg: dfvkbg}, axis='columns')
    debugprint(title='2b. drie '+dfid+' met de unieke '+gfvkbg+' hernoemd naar '+dfvkbg,
               df=_gf, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)
    
    
    _df = pd.concat([_df, _gf]).drop_duplicates(subset=[dfid, dfvkbg], keep='first')
    (_nrec, _nkey) = df_comp(df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, u_may_change=True)
    
    debugprint(title='2c. drie '+dfid+' met nu ook de unieke '+dfvkbg+' erbij. De NaNs treden op\n\
               \t\t als de '+dfvkbg+' eerst een '+gfvkbg+' van de '+gfid+' was.',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)

        
    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 3\n----',
          'Voeg hier van', groftype, 'de', gfvkeg, 'aan toe, voor zover',
          'deze ongelijk is aan', future_date)
    print('\t\tHiervoor hebben we de koppeling\n\t\t', fijntype, '-', groftype,
          'nodig, plus de', gfvkeg, 'van', groftype)
    # #############################################################################
    print('\t\tTBD ----------------')
    
    '''
    _gf = pd.merge(gf[[gfid, gfvkeg]], 
                   df[[dfid, gfid]].drop_duplicates(),
                   how='inner', on=gfid)
    # print(_gf.head(50))
    debugprint(title='3a. vier wpl met die drie straatjes geeft 13 records door veel verschillende bg',
               df=_gf, 
               colname='oprid',
               vals=['0457300000000259', '0457300000000260', '0003300000116985'], 
               sort_on=['oprid', 'wplvkeg'], debuglevel=_debuglevel)
    
    
    _gf = _gf[[dfid, gfvkeg]].drop_duplicates().rename({gfvkeg: dfvkbg}, axis='columns')
    debugprint(title='3b. zonder woonplaats, duplicaten eruit, bg hernoemd',
               df=_gf,, 
               colname='oprid',
               vals=['0457300000000259', '0457300000000260', '0003300000116985'], 
               sort_on=['oprid', 'oprvkbg'], debuglevel=_debuglevel)
    
    _gf = _gf[_gf[dfvkbg] != future_date]
    
    _df = pd.concat([_df, _gf]).drop_duplicates(subset=[dfid, dfvkbg] , keep='first')
    (_nrec, _nkey) = df_comp(df=_df, key_lst=[dfid, dfvkbg], nrec=_nrec, nkey=_nkey, u_may_change=True)
    
    debugprint(title='3c. drie straatjes samengevoegde vkbg van opr en wpl. oprvkdi=nan is van wpl',
               df=_df, 
               colname='oprid',
               vals=['0457300000000259', '0457300000000260', '0003300000116985'], 
               sort_on=['oprid', 'oprvkbg'], debuglevel=_debuglevel)
    '''
    
    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 4\n----',
          'Opvullen van de nans van de', dfvkid, 'met ffill')
    # #############################################################################
    print('\n\t\t\tDe sortering luistert nauw:')
    # https://stackoverflow.com/questions/27012151/forward-fill-specific-columns-in-pandas-dataframe
 
    print('\n\t\t\tSorteer op', dfid, dfvkbg, '(nan last), waarna je met\n',
          '\t\t\tffill de nans kunt opvullen van de', dfvkid)
    cols = [dfid, dfvkbg]
    _df = _df.sort_values(by=cols, na_position='last')
    
    _df.loc[:,dfvkid].iat[0] = 1 # if the first record in NaN, then fill gives an error
    
    _df.loc[:,dfvkid] = _df.loc[:,dfvkid].ffill().astype({dfvkid:int})

    # print(_df.head(30))
    debugprint(title='4a. drie '+dfid+'. Deze stap vult NaNs verkeerd als de vk rij begint met een NaN',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)


    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 5\n----',
          'Voeg', dfvkeg, 'toe in twee stappen:\n')
    # #############################################################################

    print('\t\t\t5a. neem de', dfvkbg, 'van het volgende record (sortering uit stap 5)')
 
    _df[dfvkeg] = _df[dfvkbg].shift(periods=-1)
    (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkbg],
                             nrec=_nrec, nkey=_nkey,
                             u_may_change=False)

    debugprint(title='5a. drie '+dfid+' met hun '+dfvkeg+'. Deze staat verkeerd voor de laatste van de vk rij',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)

    # print(_df[_df[dfvkeg].isna()])

    print('\t\t\t5b. corrigeer de vbovkeg van het meest recente', fijntype, 'voorkomen')
    print('\t\t\tDit krijgt een datum in de toekomst:', future_date)
    idx = _df.groupby([dfid])[dfvkbg].transform(max) == _df[dfvkbg]
    _df.loc[idx, dfvkeg] = future_date
    _df = _df.astype({dfvkeg:int})

    debugprint(title='5b. drie '+dfid+' met de laatste '+dfvkeg+' uit hun vk rijtje op '+str(future_date)+' gezet.',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)

    (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkbg],
                           nrec=_nrec, nkey=_nkey,
                           u_may_change=False)


    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 6\n----',
          'Koppel', gfid, gfvkid, 'erbij.')
    # #############################################################################
    
    print('\n\t\t\t6a. voeg', gfid, 'toe met de input df van', fijntype)
    cols = [dfid, dfvkid, gfid]
    _df = pd.merge(_df, df[cols], how='inner', on=[dfid, dfvkid])
    
    debugprint(title='6a. drie '+dfid+' met uit het input df',
               df=df[cols], colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)


    debugprint(title='6a. drie '+dfid+' met hun '+gfid,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)
    (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkbg],
                           nrec=_nrec, nkey=_nkey,
                           u_may_change=True)
    
    print('\n\t\t\t6b. voeg eerst', gfvkid, gfvkbg, gfvkeg,
          'toe met de input dataframe van', groftype)
    print('\t\t\t', dfid, 'wordt nu gekoppeld met elk vk van zijn', gfid)
    cols = [gfid, gfvkid, gfvkbg, gfvkeg]
    _df = pd.merge(_df, gf[cols], how='inner', on=gfid)
    debugprint(title='6b. drie '+gfid,
               df=gf[cols], colname=gfid, vals=test_d[gfid], sort_on=cols, 
               debuglevel=_debuglevel)

    debugprint(title='6b. drie '+dfid+' met hun '+gfid+', '+gfvkid,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)

        
    print('\n\t\t\t6c. Filter zodat het midden van een', fijntype, 'vk binnen een',
          groftype, 'vk valt.\n\t\t\tDit kan nu dankzij het splitsen van het', 
          fijntype, 'vk')
    _df['midden'] = (_df[dfvkbg] + _df[dfvkeg] ) * 0.5
    msk = (_df[gfvkbg] < _df['midden']) & (_df['midden'] < _df[gfvkeg])
    cols = [dfid, dfvkid, gfid, gfvkid, dfvkbg, dfvkeg]
    # cols = [dfid, dfvkid, gfid, gfvkid, dfvkbg, gfvkbg, gfvkeg, 'midden']
    _df = _df[msk][cols]


    debugprint(title='6c. drie '+dfid+' met hun '+dfvkid+' gekoppeld met '+gfid+' en '+gfvkid,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)

    (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkbg],
                           nrec=_nrec, nkey=_nkey,
                           u_may_change=True)

    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 7\n----',
          'Maak een nieuwe teller voor',fijntype, 'genaamd', dfvkid2, 
          '\n\t\tom deze te kunnen onderscheiden van de bestaande', dfvkid)
    # #############################################################################
    cols = [dfid, dfvkbg]
    _df = _df.sort_values(by=cols, na_position='last')
    _df = makecounter(_df, grouper=dfid, newname=dfvkid2, sortlist=[dfid, dfvkbg, gfid])
    print('\t\t\tSchakel hierop over om voorkomens te identificeren')


    debugprint(title='7a. drie '+dfid+' met hun nieuwe vk tellers '+dfvkid2,
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)


    (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkid2], nrec=_nrec, 
                             nkey=_nkey, u_may_change=True)
 
 

    # #############################################################################
    print('\n----', fijntype+'vk-splitter stap 8\n----',
          'Toevoegen van de kolommen uit', fijntype,
          'die we nog misten')
    # #############################################################################
    
    _df = pd.merge(_df,
                   df.drop([gfid, dfvkbg, dfvkeg], axis=1).drop_duplicates(),
                   how='inner',
                   left_on=[dfid, dfvkid], right_on=[dfid, dfvkid])

    debugprint(title='8. drie '+dfid+' met nieuwe geimputeerde vks',
               df=_df, colname=dfid, vals=test_d[dfid], sort_on=cols, 
               debuglevel=_debuglevel)


    (_nrec, _nkey) = df_comp(_df, key_lst=[dfid, dfvkid2],
                             nrec=_nrec, nkey=_nkey,
                             u_may_change=False)



    print('\n-----------------------------------------------------------')   
    print('------ Einde vksplitter; fijntype:', fijntype, ', groftype:', 
          groftype)   
    print('------ Perc toegenomen', fijntype, 'voorkomens:', 
          round(100 * (_nkey/_nkey1 - 1), 1), '%')
    print('-----------------------------------------------------------')   

    return _df
    
