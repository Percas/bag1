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
import requests

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


def get_arg1(arg_lst, ddir):
    '''Lees het eerste argument van de aanroep. Dit moet een mapnaam zijn.'''
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


def diff_df(df1, df2):
    '''Return tuple: (dfboth, df1not2, df2not1).'''
    print('\tdiff_idx_df: in beide, in 1 niet 2, in 2 niet 1:')
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

def fix_eendagsvlieg(df, b_str, e_str):
    """Return df with voorkomens removed that start and end on same day."""
    # print('\tVerwijderden van eendagsvliegen:')
    return df[df[b_str] < df[e_str]]
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


def make_dir(path, loglevel=10):
    if not os.path.exists(path):
        aprint(loglevel, '\n\tAanmaken outputmap', path)
        os.makedirs(path)

def recast_df_floats(df, dict1, loglevel=10):
    '''Recast the float64 types in df to types in dict1 afer df.fillna(0)'''
    _float_cols = df.select_dtypes(include=['float']).columns
    aprint(loglevel, '\tDowncasting types of:', list(_float_cols))
    _type_dict = {k: dict1[k] for k in _float_cols}
    df[_float_cols] = df[_float_cols].fillna(0)
    # df[_float_cols] = df[_float_cols].astype(_type_dict)
    return df.astype(_type_dict)


def print_time(seconds, info, printit):
    '''Print toc-tic s if printit = True.'''
    if printit:
        print(info, seconds / 60, 'min\n')
        # print(info, time.strftime('%H:%M:%S', time.gmtime(int(seconds))))

def df_comp(loglevel,
            df, key_lst=[], nrec=0, nkey=0, u_may_change=True):
    '''
    Check if df has n_rec records and n_rec_u unique records.
    Use key_lst to determine the unique keys. If empty use df.index.
    Return the triple (number of records, unique nr of rec, percentage unique).
    '''
    _nrec = df.shape[0]
    _key_lst = key_lst
    _ll = loglevel
    
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
        aprint(_ll, '\t\tAantal records in:', nrec, 'aantal uit:', _nrec)
    else:
        aprint(_ll, '\t\tAantal input records ongewijzigd: records in = uit =',
              nrec)
    if nkey != _nkey:
        aprint(_ll, '\t\tAantal eenheden in:', nkey,
              'aantal', _key_lst, 'uit:', _nkey)
        if not u_may_change:
            aprint(_ll+40, 'FOUT: aantal unieke eenheden gewijzigd!')
    else:
        aprint(_ll, '\t\tAantal', _key_lst, 'ongewijzigd: vk in = uit =',
              nkey)
    # in_equals_out = (_n_rec == n_rec) and (_n_rec_u == n_rec_u) 
    # print('\tNr of records in equals out:', in_equals_out)
    return (_nrec, _nkey)


    
def ontdubbel_idx_maxcol(df, max_cols):
    '''Ontdubbel op de idx van df door de grootste waarde in de 
    kolommen uit max_cols te nemen.'''
    _df = df.sort_values(axis=0, by=max_cols, ascending=False)
    return _df[~_df.index.duplicated(keep='first')] 


def read_input_csv(loglevel=10, file_d={}, bag_type_d={}):
    '''Read the input csv files in file_d dict and return them in a dict of
    df. Use the bag_type_d dict to get the (memory) minimal types.'''
    _bdict = {}
    for _k, _f in file_d.items():
        aprint(loglevel+10, '\tInlezen', _k, '...')
        _bdict[_k] = pd.read_csv(_f, 
                                 dtype=bag_type_d,
                                 keep_default_na=False)
        aprint(loglevel, '\t\t', _k, 'heeft', _bdict[_k].shape[0], 'records')
        # print('DEBUG:', _bdict[_k].info())
        # print('DEBUG:', _bdict[_k].head())
    return _bdict
        

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

def make_counter(loglevel, df, grouper, newname, cols):
     ''' Add a column with name newname that is a counter 
     for the column grouper.''' 
     # make a new counter for vbovkid. call it newname
     # tmp = df.groupby(grouper).cumcount()+1
     # print('DEBUG', df.info())
     # print('DEBUG', sortlist)
     # tmp = df.sort_values(by=sortlist, axis=0)
     
     aprint(loglevel, '\t\t\tmake_counter: maak een nieuwe teller voor de vk')
     _tmp = df.sort_values(by=cols, na_position='last')
     _tmp = _tmp.groupby(grouper).cumcount()+1
     df[newname] = _tmp.to_frame()
     return df


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


def aprint(*args):
    # _f = args.pop(0)
    if args[0] >= 40:
        print(*args[1:])


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


def printkop(loglevel=20, kop='Header 1'):
    _ll = loglevel
    # _fmt = '-------------', kop, '-----------'
    aprint(_ll, '')
    aprint(_ll-10, '-------------------------------------------')
    aprint(_ll, '----', kop)
    aprint(_ll-10,'-------------------------------------------\n')


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
