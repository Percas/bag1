#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Functions used in different python scripts
"""
# ################ import libraries ###############################
import pandas as pd
import polars as pl
import time
import os
import sys
import shutil
# import datetime
import logging
import numpy as np
import requests
from config import FUTURE_DATE, KOPPELVLAK2, FILE_EXT, LOGFILE

################## Init logger #########################################
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGFILE),
        logging.StreamHandler()])    
logit = logging.getLogger()
logit.setLevel(logging.DEBUG)


# ################ define datastructures ###############################
'''
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
'''




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


def get_args(arg_lst=[], ddir=''):
    '''Lees de argumenten van de aanroep. Dit moeten mappen zijn in ddir.'''
        
    if len(arg_lst) <= 1:
        # sys.exit('Usage: ' + arg_lst[0] + '  <month>, where <month> in '
        #         + str(_lst))
        return "testdata23"

    if ddir:
        _dir_lst = os.listdir(ddir)
        for _arg in arg_lst[1:]:
            if _arg not in _dir_lst:
                sys.exit('Usage: ' + arg_lst[0] + '  <month_lst>, where <month_lst> in '
                         + str(_dir_lst))
    return arg_lst[1:]

def get_args2(args=[], arg1_in = [], args2_in=[]):
    '''Controleer de argumenten args van een functie-aanroep obv de twee
    controles arg1_in en args2_in. 
    args[1] moet een functienaam zijn uit de lijst arg1_in
    args[2] moet een lijst zijn waarvan elk element een subdirectory moet zijn van args2_in.'''
        
    # _functie_lst = [x.__name__ for x in arg1_in ]
    _dir_lst = os.listdir(args2_in)
    if len(args) <= 2:
        sys.exit(f'Usage: {args[0]} <bag_functie>, <month>, where bag_functie in {arg1_in}, <month> in {args2_in}')

    if arg1_in:
        if not args[1] in arg1_in:
            sys.exit(f'first argument {args[1]} of function {args[0]} not in {arg1_in}')
    if args2_in:
        for _arg in args[2:]:
            if _arg not in _dir_lst:
                sys.exit(f'second or more arguments of {args[0]} not in {_dir_lst}')
    return args[1:]

def diff_df(df1, df2):
    '''Return tuple: (dfboth, df1not2, df2not1).'''
    # print('\tdiff_idx_df: in beide, in 1 niet 2, in 2 niet 1:')
    _df = pd.concat([df1, df2])
    _dfboth = _df[~_df.duplicated(keep='first')]
    _df = pd.concat([df1, _dfboth])
    _df1not2 = _df[~_df.duplicated(keep=False)]
    _df = pd.concat([df2, _dfboth])
    _df2not1 = _df[~_df.duplicated(keep=False)]
    return (_dfboth, _df1not2, _df2not1)


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

def fix_eendagsvlieg(df, b_str, e_str, logit, df_type='pandas'):
    """Return df with voorkomens removed that start and end on same day."""
    logit.info(f'start functie fix_eendagsvlieg voor {b_str[:3]}')
    if df_type == 'pandas':
        return df[df[b_str] < df[e_str]]
    else:
        return df.filter(pl.col(b_str) < pl.col(e_str))


'''
def print_omgeving(adir):
    if adir[-4:-1] == 'ont':
        print('\t---------------------------------')
        print('\t--------ONTWIKKELOMGEVING--------')
        print('\t---------------------------------\n')
    else:
        print('\t---------------------------------')
        print('\t--------PRODUCTIEOMGEVING--------')
        print('\t---------------------------------\n')
'''

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

    _nvk = df.shape[0]
    _navk = _df_active.shape[0]
    _davk = _navk / _nvk
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


def make_dirs(path, logit=logit):
    if not os.path.exists(path):
        logit.info(f'aanmaken outputmap {path}')
        os.makedirs(path)

def recast_df_floats(df, dict1, logit=logit):
    '''Recast the float64 types in df to types in dict1 afer df.fillna(0)'''
    _float_cols = df.select_dtypes(include=['float']).columns
    logit.debug(f'downcasting types of: {list(_float_cols)}')
    _type_dict = {k: dict1[k] for k in _float_cols}
    df[_float_cols] = df[_float_cols].fillna(0)
    # df[_float_cols] = df[_float_cols].astype(_type_dict)
    return df.astype(_type_dict)


def print_time(seconds, info, printit):
    '''Print toc-tic s if printit = True.'''
    if printit:
        print(info, seconds / 60, 'min\n')
        # print(info, time.strftime('%H:%M:%S', time.gmtime(int(seconds))))


def df_comp(df, key_lst=[], nrec=0, nvk=0, 
            u_may_change=True, toegestane_marge=1,
            logit=logit):
    '''
    Check if df has n_rec records and n_rec_u unique records.
    Use key_lst to determine the unique keys. If empty use df.index.
    Return the triple (number of records, unique nr of rec, percentage unique).
    '''
    _nrec = df.shape[0]
    _key_lst = key_lst
    
    if key_lst == []:
        _nvk = df.index.drop_duplicates().shape[0]
        _key_lst = df.index.names
    else:
        _nvk = df[key_lst].drop_duplicates().shape[0]
        # print('DEBUG df_comp')
        # print(df[key_lst].drop_duplicates())
        # print(df[key_lst].drop_duplicates().shape[0])
    
    if nrec ==  0:
        return (_nrec, _nvk)
    
    if nrec != _nrec:
        logit.debug(f'aantal records in: {nrec} aantal uit: {_nrec}')
    else:
        pass
        # logit.debug(f'aantal input records ongewijzigd: records in = uit = {nrec}')
    if nvk != _nvk:
        marge = round(_nvk/nvk - 1, 2)
        if abs(marge) > toegestane_marge:
            logit.warning(f'percentuele wijziging in voorkomens : {marge}')
            logit.warning(f'aantal unieke eenheden gewijzigd; in: {nvk}, uit: {_nvk}')
            logit.warning(f'aantal records in: {nrec}, uit: {_nrec}')
        else:
            logit.debug(f'wijziging aantal vk binnen toegestane marge. Marge: {marge}')
        if not u_may_change:
            logit.warning(f'aantal unieke eenheden gewijzigd; in: {nvk}, uit: {_nvk}')
    else:
        logit.debug(f'aantal {_key_lst} ongewijzigd: vk in = uit = {nvk}')
    # in_equals_out = (_n_rec == n_rec) and (_n_rec_u == n_rec_u) 
    # print('\tNr of records in equals out:', in_equals_out)
    return (_nrec, _nvk)






def df_compare(df, vk_lst=[], nrec=0, nvk=0, 
               u_may_change=True,
               nvk_marge=0,
               nrec_nvk_ratio_is_1=2,
               logit=logit):
    '''
    df_compare controleert een aantal zaken voor het dataframe df.
    vk_lst moet de twee kolommen bevatten die de voorkomens identificeren.
    nrec en nvk tellen records en voorkomens.
    als nrec en nvk leeg zijn dan returned df_compare nrec en nvk.
    als nrec en nvk gevuld zijn, dan vergelijkt df_compare ze met de nrec en nvk
    van df:
        
    1. als u_may_change == False geeft df_comp een warning als nvk is gewijzigd.
    2. de nvk_marge is de maat hoeveel het aantal voorkomens mag wijzigen:
        0 betekent geen wijziging
    3. nrec_nvk_ratio geeft aan hoeveel de verhouding tussen nrec en nvk mag wijzigen:
        1 betekent dat deze niet wijzigt
    '''
    _nrec = df.shape[0]
    _vk_lst = vk_lst
    
    if vk_lst == []:
        sys.exit('vk_lst moet de twee kolommen bevatten die de voorkomens identificeren')
    else:
        _nvk = df[vk_lst].drop_duplicates().shape[0]


    # eerst de verhouding van het aantal records met aantal voorkomens vergelijken. 
    # moet dit 1 zijn of juist niet
    _ratio = _nrec/_nvk
    logit.debug(f'bij {vk_lst[0]} is de verhouding records en vk {_ratio}')
    if nrec_nvk_ratio_is_1 != 2:
        if _ratio != 1 and nrec_nvk_ratio_is_1 == 1:
            logit.warning(f'verwacht was dat de nrec/nvk 1 zou zijn. Is echter {_ratio}')
        if _ratio == 1 and nrec_nvk_ratio_is_1 == 0:
            logit.warning(f'verwacht was dat de nrec/nvk niet 1 zou zijn. Is echter {_ratio}')


    if nrec ==  0 or nvk == 0:
        return (_nrec, _nvk)
    
    # input nrec en nvk hebben een waarde. vergelijk input nrec, nvk met output _nrec, _nvk
    if nrec != _nrec:
        logit.debug(f'aantal records in: {nrec} aantal uit: {_nrec}')
    else:
        pass
        # logit.debug(f'aantal input records ongewijzigd: records in = uit = {nrec}')
    if nvk != _nvk:
        _marge = _nvk/nvk - 1
        perc_wijziging = round(100 * _marge, 1)
        logit.info(f'percentuele wijziging voorkomens: {perc_wijziging}%')
        if abs(_marge) > abs(nvk_marge):
            logit.info(f'aantal unieke eenheden gewijzigd; in: {nvk}, uit: {_nvk}')
            logit.info(f'aantal records in: {nrec}, uit: {_nrec}')
        if not u_may_change:
            logit.warning(f'aantal unieke eenheden gewijzigd; in: {nvk}, uit: {_nvk}')
    else:
        logit.debug(f'aantal {_vk_lst} ongewijzigd: vk in = uit = {nvk}')
    # in_equals_out = (_n_rec == n_rec) and (_n_rec_u == n_rec_u) 
    # print('\tNr of records in equals out:', in_equals_out)
    return (_nrec, _nvk)


def dl_comp(dl, vk_lst=[], nrec=0, nvk=0, u_may_change=True,
            logit=logit):
    '''
    Check if polars dl has n_rec records and n_rec_u unique records.
    Use vk_lst to determine the unique keys.
    Return the tuple (number of records, unique nr of rec).
    '''
    _nrec = dl.shape[0]
    _nvk = dl[vk_lst].unique().shape[0]
    # print('DEBUG df_compare')
    # print(dl[vk_lst].unique())
    # print(dl[vk_lst].unique().shape[0])
    
    if nrec ==  0:
        return (_nrec, _nvk)
    
    if nrec != _nrec:
        logit.debug(f'aantal records in: {nrec} aantal uit: {_nrec}')
    else:
        logit.debug('aantal input records ongewijzigd: records in = uit = {nrec}')
    if nvk != _nvk:
        logit.debug(f'aantal eenheden in: {nvk}; aantal {vk_lst} uit: {_nvk}')
        if not u_may_change:
            logit.warning('aantal unieke eenheden gewijzigd!')
    else:
        logit.debug(f'aantal {vk_lst} ongewijzigd: vk in = uit = {_nvk}')
    # in_equals_out = (_n_rec == n_rec) and (_n_rec_u == n_rec_u) 
    # print('\tNr of records in equals out:', in_equals_out)
    return (_nrec, _nvk)

    
def ontdubbel_idx_maxcol(df, max_cols):
    '''Ontdubbel op de idx van df door de grootste waarde in de 
    kolommen uit max_cols te nemen.'''
    _df = df.sort_values(axis=0, by=max_cols, ascending=False)
    return _df[~_df.index.duplicated(keep='first')] 


def read_dict_of_df(file_d={}, bag_type_d={}, logit=logit):
    '''Read the files in the dictionary file_d and return them in a dict of
    dataframes. if present read .parquet otherwise try csv.'''
    
    _bdict = {}
    for _k, _f in file_d.items():
        logit.debug(f'inlezen {_k}...')
        _filepath = _f + '.parquet'
        if os.path.exists(_filepath):
            logit.debug('parquet file gevonden')
            _bdict[_k] = pd.read_parquet(_filepath)
            # take only the cols in dataframe _bdict[_k]:
            _astype_cols = {_i: bag_type_d[_i] for _i in list(_bdict[_k].columns)}
            _bdict[_k] = _bdict[_k].astype(dtype=_astype_cols)
        else:
            logit.debug('parquet file niet gevonden, probeer csv')
            _filepath = _f + '.csv'
            if os.path.exists(_filepath):
                _bdict[_k] = pd.read_csv(_filepath,
                                         dtype=bag_type_d,
                                         keep_default_na=False)
            else:
                sys.exit('Input panic: cant find' + _filepath)
        logit.debug(f'bestand {_filepath} gelezen')
        logit.debug(f'{_k} heeft {_bdict[_k].shape[0]} records')
        # print('DEBUG:', _bdict[_k].info())
        # print('DEBUG:', _bdict[_k].head())
    return _bdict


def read_parquet(input_file='', bag_type_d={}, output_file_type='pandas',
                 logit=logit):
    '''Read a parquet or csv file  and return a pandas or polars df. 
    If present read .parquet otherwise try csv. Default output is pandas.
    '''
    
    logit.debug(f'inlezen {input_file}')
    _filepath = input_file + '.parquet'
    if os.path.exists(_filepath):
        logit.debug(f'inlezen van {_filepath}')
        if output_file_type != 'polars':
            _df = pd.read_parquet(_filepath)
            _astype_cols = {_i: bag_type_d[_i] for _i in list(_df.columns)}
            _df = _df.astype(dtype=_astype_cols)
        else:
            _df = pl.read_parquet(_filepath)
        # take only the cols in dataframe _bdict[_k]:
    else:
        _filepath = input_file + '.csv'
        if os.path.exists(_filepath):
            logit.debug(f'csv gevonden. inlezen van {_filepath}')
            if output_file_type == 'pandas':
                _df = pd.read_csv(_filepath,
                                  dtype=bag_type_d,
                                  keep_default_na=False)
            else:
                _df = pl.read_csv(_filepath,
                                      dtype=bag_type_d,
                                      keep_default_na=False)
                
        else:
            sys.exit('Input panic: cant find' + _filepath)
    # logit.debug(f'bestand {_filepath} gelezen')
    logit.debug(f'dataframe heeft {_df.shape[0]} records')
    # print('DEBUG:', _bdict[_k].info())
    # print('DEBUG:', _bdict[_k].head())
    return _df


def save_df2file(df='vbo_df', outputfile='', file_ext='parquet', 
                 includeindex=True, append=False, logit=logit):
    '''Bewaar dataframe df in outputfile in ext=parquet of csv formaat'''
    if file_ext == 'parquet':
        logit.debug(f'bewaren in parquet formaat van {outputfile} met append={append}')
        df.to_parquet(outputfile + '.parquet', index=includeindex, engine='fastparquet', append=append)
    else:
        logit.debug(f'bewaren in csv formaat van {outputfile}')
        if append:
            _mode = 'a'
        else:
            _mode = 'w'
 
        _header = not append # don't need a header in append mode
        df.to_csv(outputfile + '.csv', index=includeindex, mode=_mode, header=_header)

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


def debugprint(title='', df='vbo_df', colname='vboid',
               vals=[], sort_on=[], loglevel=10):
    '''print a the lines of the df where df[colname]==val.'''
    if loglevel >= 40:
        print('\n\t\tDebug:', title)
        _df = df.loc[df[colname].isin(vals)].sort_values(by=sort_on)        
        print('\t\tAantal', colname+':\t', _df[colname].unique().shape[0])
        print('\t\tAantal records:\t', _df.shape[0], '\n')
        print(_df.to_string(index=False))
        print()

'''
def anastatus(df, overgang, loglevel=10):
     Bepaal de vbos in df waar de statusovergang overgang in zit.

    querystr = ''
    for colname, status in overgang.items():
        querystr.append(df[colname] == status)
        return = df.query(querystr)
'''

def make_counter(df, dfid, dfvkbg,
                 old_counter='',
                 new_counter='vboid', logit=logit):
     ''' Maak een nieuwe column newname die de voorkomens vk telt van dfid.
     dfid identificeert het bagobject. dfid + dfvkbg (voorkomen begindatum)
     identificeren vk van dit object. Een vk kan dubbel voorkomen. 
     Dan heeft het dezelfde dfid en vkbg
     
     voorbeeld:
       vboid,vbovkid,vbovkbg,  vbovkeg,     status,     pndid,  numid   
       vbo1, 1,      19980601, 20010201,    in_gebruik, pndid1, numid1
       vbo1, 3,      20230401, 88888888,    in_gebruik, pndid1, numid1
       vbo1, 3,      20230401, 88888888,    in_gebruik, pndid2, numid1

    de vbovkid moet netjes gaan tellen 1, 2... let op dubbele vk:         
       vboid,vbovkid,vbovkbg,  vbovkeg,     status,     pndid,  numid   
       vbo1, 1,      19980601, 20010201,    in_gebruik, pndid1, numid1
       vbo1, 2,      20230401, 88888888,    in_gebruik, pndid1, numid1
       vbo1, 2,      20230401, 88888888,    in_gebruik, pndid2, numid1
     
     tussenstap maak uniek op vboid en vbovkbg:
       vboid, vbovkbg,  
       vbo1,  19980601
       vbo1,  20230401
         
     voeg daarna de teller toe en koppel dan op vboid en vbovkbg
     ''' 

     _cols = [dfid, dfvkbg]
     logit.debug('make_counter: maak een nieuwe teller voor de vk')
     
     # maak het df uit de tussenstap, zie voorbeeld hierboven
     _df = df[_cols].sort_values(by=_cols, na_position='last')
     _df = _df[~_df.duplicated(keep='first')]

     # maak de teller
     _tmp = _df.groupby(dfid).cumcount()+1
     
     # voeg de teller toe
     
     _df[new_counter] = _tmp.to_frame()
     
     # koppel de rest er weer bij
     if old_counter == new_counter:
         _df = pd.merge(_df, df.drop(columns=old_counter))
     else:
         _df = pd.merge(_df, df).sort_values(by=[dfid, new_counter])
         
         
     # print(_df.info())
     return _df # .astype(dtype={new_counter: np.short})

def make_vkeg(df, bob, logit, df_type='pandas'):
    '''Maak de voorkomen einddatum geldigheid (vkeg), gegeven id en vkbg
    (voorkomen begindatum geldigheid) van een bob (bagobject).
    
    Conventies:
        de kolom bob + 'vkeg' bestaat niet

        bob + 'id' is een kolomnaam in df en identificeert het bob
        bob + 'vkbg' is een kolomnaam in df
        bob + 'vkeg' wordt aangemaakt als vervanger van de bestaande in df
        
    Stappen:
        1. Sorteer op id en vkbg
        2. doe een df.shift(periods = -1) om de begindatum van het volgende record te krijgen
        3. corrigeer de laatste vkeg van het vk rijtje. Maak deze gelijk aan future_date. 
        
    '''

    # init variabelen
    _dfid = bob + 'id'
    _dfvkbg = bob + 'vkbg'
    _dfvkeg = bob + 'vkeg'
    
    if df_type == 'pandas':
        _nrec, _nvk = df_compare(df=df, vk_lst=[_dfid, _dfvkbg], logit=logit)
    else:
        (_nrec, _nvk) = dl_comp(dl=df, vk_lst=[_dfid, _dfvkbg], logit=logit)
        
    logit.debug(f'make_vkeg input: aantal {bob} records: {_nrec} aantal {bob}vk: {_nvk}')
    # logit.debug(f'make_vkeg 1: sorteer {bob} op {_dfid}, {_dfvkbg}')
    if df_type == 'pandas':
        _df = df.sort_values(by=[_dfid, _dfvkbg])
    else:
        _df = df.sort([_dfid, _dfvkbg])
    # print(_df.info())
    
    # logit.debug(f'make_vkeg 2: neem voor {_dfvkeg} de {_dfvkbg} van het volgende record')
    if df_type == 'pandas':
        _df[_dfvkeg] = _df[_dfvkbg].shift(periods=-1)
    else:
        _df = df.with_column(pl.col(_dfvkeg), pl.col(_dfvkbg).shift(-1))
        print(_df.head())

    # logit.debug(f'make_vkeg 3: corrigeer de {bob}vkeg van het meest recente {bob} voorkomen')
    if df_type == 'pandas':
        _idx = _df.groupby([_dfid])[_dfvkbg].transform(max) == _df[_dfvkbg]
    else:
        _idx = _df.groupby([_dfid]).agg([_dfvkbg.max().alias('_dfvkbg_max')]).sort(_dfid).eq(_df[_dfvkbg])._cols[0]
        
    _df.loc[_idx, _dfvkeg] = FUTURE_DATE



    # logit.debug(f'make_vkeg 4: terugcasten van {_dfvkeg} naar int')
    _df = _df.astype({_dfvkeg:int})

    (_nrec, _nvk) = df_compare(df=df, vk_lst=[_dfid, _dfvkbg], nrec=_nrec, 
                             nvk=_nvk, u_may_change=False, logit=logit)
    
    return _df


def merge_vk(df=pd.DataFrame(), bob='vbo', relevant_cols=[], 
             logit=logit, df_type='pandas'):
    ''' Neem de voorkomens van df samen als voor een dfid de waarden van de
    relevante cols gelijk zijn. We noemen dit "gelijke opeenvolgende vk". Neem 
    de minimale dfvkbg van elke set gelijke vk. Maak nieuwe vk id in dfvkid2.
    
    Conventies:
        bob + 'id' is een kolomnaam in df en identificeert het bob
        bob + 'vkbg' is een kolomnaam in df
        bob + 'vkeg' wordt aangemaakt als vervanger van de bestaande in df
        
    Stappen:
        1. verwijder dubbele records vwb de relevante kolommen zie voorbeeld
        2. maak nieuwe dfvkeg
        3. maak nieuwe tellers dfvkid2
        4. return nieuwe ingekorte df
 
    
   # voorbeeld:
       
       je hebt identificerende kolommen en relevante kolommen. 
       
       identificerende kolommen:
           vboid, vbovkid, vbovkbg, vbovkeg
           om het voorkomen van een vboid te identificeren is vbovkbg voldoende
        
        relevante kolommen:
           status, pndid, numid

        
       input is een vbo met vboid = vbo1 met 3 voorkomens (waarvan het laatste
                                                           met twee panden)
       vboid,vbovkid,vbovkbg,  vbovkeg,     status,     pndid,  numid   
       vbo1, vkid1,  19980601, 20010201,    in_gebruik, pndid1, numid1
       vbo1, vkid2,  20010201, 20230331,    in_gebruik, pndid1, numid1
       vbo1, vkid3,  20230401, 88888888,    in_gebruik, pndid1, numid1
       vbo1, vkid3,  20230401, 88888888,    in_gebruik, pndid2, numid1
       


       zoek naar dubbelen in de relevante kolommen (dus status, pndid en numid)
       drop_duplicates dropt het tweede voorkomen met vkbg = 20010201

       vboid,vbovkid,vbovkbg,  vbovkeg,     status,     pndid,  numid   
       vbo1, vkid1,  19980601, 20010201,    in_gebruik, pndid1, numid1
       vbo1, vkid3,  20230401, 88888888,    in_gebruik, pndid1, numid1
       vbo1, vkid3,  20230401, 88888888,    in_gebruik, pndid2, numid1
   '''
 
    # init variabelen
    _dfid = bob + 'id'
    _dfvkid = bob + 'vkid'
    _dfvkid2 = bob + 'vkid2'
    _dfvkbg = bob + 'vkbg'
    _dfvkeg = bob + 'vkeg'
    _vk = [_dfid, _dfvkbg]
    _cols = relevant_cols + [_dfid]     # de relevante kolommen plus id van het object, zonder vkid of datums!
    # _cols2 = _cols + [_dfvkbg] # idem maar met de begindatum van het voorkomen
    
    _nrec, _nvk = df_compare(df=df, vk_lst=_vk, logit=logit)
    logit.info(f'start functie merge_vk met {_nrec} {bob} records en {_nvk} {bob} vk')



    logit.debug('merge_vk 1: verwijder dubbele opeenvolgende vk')
    if df_type == 'pandas':
        # vkeg is overbodig en wordt later opnieuw aangemaakt
        _df = df.drop(columns=_dfvkeg)
        _df = _df.sort_values(by=[_dfid, _dfvkbg])
        _df = _df.drop_duplicates(subset=_cols, keep='first')
        # _df = df[_cols2].sort_values(by=[_dfid, _dfvkbg]).drop_duplicates(subset=_cols, keep='first')
    else:
        logit.error('not implemented')
        # _df = df[_cols2].sort([_dfid, _dfvkbg]).unique(subset=_cols, keep='first')
 
    logit.debug(f'merge_vk 2: voeg {_dfvkeg} toe')
    _df = make_vkeg(_df, bob, logit, df_type=df_type)
    
    logit.debug('merge_vk 3: maak een nieuwe vk teller')
    _df = make_counter(df=_df, dfid=_dfid,
                       old_counter=_dfvkid,
                       new_counter=_dfvkid, 
                       dfvkbg=_dfvkbg, logit=logit)
    # print(_df.info())
    _nrec1, _nvk1 = df_compare(df=_df, vk_lst=_vk, nrec=_nrec, nvk=_nvk,
                               nvk_marge=0.1, logit=logit)

    logit.info(f'merge_vk: perc afgenomen {bob}vk: {round(100 * (_nvk1/_nvk - 1), 1)} %')

    return _df



def prev_month(month='testdata23'):
    '''For a month in format YYYYMM return the previous month.'''
    if month == 'testdata23':
        return 'testdata02'
    else:
        if str(month).endswith('01'):
            return str(int(month) - 89)
        else:
            return str(int(month) - 1)


def find_missing_vk(bob, current, previous, vk, future_date, loglevel=20, test_d=[]):
    '''Compare current df with previous df on vk.
    Controleer of afgesloten vk in previous ongewijzigd zijn in current.
    Aanpak:
        1. Neem de afgesloten vk in previous
        2. Check of deze ongewijzigd zijn in current
        3. print het aantal gewijzigde en return de gewijzigde als df
        
    '''
    _ll = loglevel
    aprint(_ll+20, '\n\tStart find_missing_vk' )
    # first get all closed vk of the previous month
    _closed_prevmonth = previous[previous[bob+'vkeg'] != future_date]

    debugprint(loglevel=loglevel, title='afgesloten vk uit vorige maand', 
               df=_closed_prevmonth, colname=vk[0], vals=test_d[vk[0]])

    
    # then get the same ones in the current month, as far as they exist
    # this should be exactly the same as previous month
    _same_vk_this_month = pd.merge(current[vk], _closed_prevmonth, how='inner', on=vk)
    debugprint(loglevel=loglevel, title='vk in huidige die vorige mnd afgesloten waren', 
               df=_same_vk_this_month, colname=vk[0], vals=test_d[vk[0]])
    
    # now find the differences. are there single vk in the concatenated df:
    _doubled = pd.concat([_closed_prevmonth, _same_vk_this_month])
    
    # so now if any of _idx is true, that is a missing vk in the current month:

    _missing = _doubled[~_doubled.duplicated(keep=False)]
    debugprint(loglevel=loglevel, title='vk die missen in deze maand', 
               df=_missing, colname=vk[0], vals=test_d[vk[0]])

    if _missing.empty:
        aprint(_ll+10, '\t\tfind_missing_vk heeft geen verdwenen', bob, 'vk gevonden')
    else:
        aprint(_ll+10, '\t\tfind_missing_vk heeft WEL verdwenen', bob, 'vk gevonden')
        aprint(_ll, 'verdwenen vk:', _missing.head())        
    aprint(_ll+20, '\tEnd find_missing_vk\n' )
    return _missing

'''

def diff_idx_df(df1, df2):
    # Return tuple: (dfboth, df1not2, df2not1).
    print('\tdiff_idx_df: in beide, in 1 niet 2, in 2 niet 1:')
    _df = pd.concat([df1, df2])
    _dfboth = _df[~_df.index.duplicated(keep='first')]
    _df = pd.concat([df1, _dfboth])
    _df1not2 = _df[~_df.index.duplicated(keep=False)]
    _df = pd.concat([df2, _dfboth])
    _df2not1 = _df[~_df.index.duplicated(keep=False)]
    return (_dfboth, _df1not2, _df2not1)
'''

def find_double_vk(df, bobid, bobvkid):
    '''Find the double voorkomen (vk) in df, identified by bobid, bobvkid.'''
    return df.groupby([bobid, bobvkid]).size().to_frame('aantal').sort_values(by='aantal', ascending=False)


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


def download_file(url):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    return local_filename

def make_month_lst(current_month, n):
    _current = str(current_month)
    _n = int(n)
    desc_month_lst = [_current]
    asc_month_lst = [_current]
    
    for i in range(_n-1):
        _previous = prev_month(_current)
        desc_month_lst.append(_previous)
        asc_month_lst.insert(0, _previous)
        _current = _previous
    return asc_month_lst

def make_subdict(df, dict1):
    '''Return subdictionary of dict1, containing only the cols in df.'''
    return {_i: dict1[_i] for _i in list(df.columns)}

'''
def rename_wplgem2wpl(dir1=KOPPELVLAK2, maand='202305',
                      file_ext=FILE_EXT, logit=logit):
    Hernoem wplgem naar wpl (met extensie file_ext). 
    Als wpl al bestaat hernoem wpl dan naar wpl_naam.
    _wpl_naam = os.path.join(dir1, maand, 'wpl_naam.'+file_ext)
    _wplgem = os.path.join(dir1, maand, 'wplgem.'+file_ext)
    _wpl = os.path.join(dir1, maand, 'wpl.'+file_ext)
    if os.path.exists(_wplgem):
        if os.path.exists(_wpl_naam):
            os.remove(_wpl_naam)
        if os.path.exists(_wpl):
            logit.debug('renaming wpl to wpl_naam')
            os.rename(_wpl, _wpl_naam)
        logit.debug('renaming wplgem to wpl')
        os.rename(_wplgem, _wpl)
'''
def copy_wplgem2wpl(dir1=KOPPELVLAK2, maand='202305',
                    file_ext=FILE_EXT, logit=logit):
    '''Kopieer wplgem naar wpl (met extensie file_ext). 
    Als wpl al bestaat hernoem wpl dan naar wpl_naam.'''
    _wpl_naam = os.path.join(dir1, maand, 'wpl_naam.'+file_ext)
    _wplgem = os.path.join(dir1, maand, 'wplgem.'+file_ext)
    _wpl = os.path.join(dir1, maand, 'wpl.'+file_ext)
    if os.path.exists(_wplgem):
        if os.path.exists(_wpl_naam):
            os.remove(_wpl_naam)
        if os.path.exists(_wpl):
            logit.debug('renaming wpl to wpl_naam')
            os.rename(_wpl, _wpl_naam)
        logit.debug('copying wplgem to wpl')
        shutil.copyfile(_wplgem, _wpl)
