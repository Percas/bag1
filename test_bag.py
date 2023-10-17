#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 30 14:54:08 2023

@author: anton
"""


# import pytest
# from fix_vkvk import fix_vk
# import os


import logging

# from config import TESTDIR, KOPPELVLAK2, BAG_OBJECTEN, BAG_TYPE_DICT, DF_TYPE, RELEVANT_COLS_DICT
import k0_unzip
import baglib
import k2_fixvk

# import numpy as np
# from pandas.testing import assert_frame_equal
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def test_1_make_counter():
    '''test the function make_counter in baglib if old_counter==new_counter.'''

    # --- maak de input en de verwachte output
    # ------------------------------------------------

    in_df = pd.DataFrame( {
        'id':   ['id1',    'id1',    'id1', '  id2'],
        'vkid': [1,        2,        4,        1],
        'vkbg': [20130101, 20160202, 20190303, 20200404],
        'vkeg': [20160202, 20190303, 88888888, 88888888],
        'any' : ['a',      'b',      'c',      'd' ]
        })

    expected_df = pd.DataFrame( {
        'id':   ['id1',    'id1',    'id1', '  id2'],
        'vkid': [1,        2,        3,        1],        # <==== third item is changed
        'vkbg': [20130101, 20160202, 20190303, 20200404],
        'vkeg': [20160202, 20190303, 88888888, 88888888],
        'any' : ['a',      'b',      'c',      'd' ]
        })

    # --- berekenening output met functie make_counter
    # ------------------------------------------------
    logger.debug('making a new counter for wpl')
    out_df = baglib.make_counter(df=in_df, dfid='id', dfvkbg='vkbg',
                                 old_counter='vkid',
                                 new_counter='vkid',
                                 logit=logger)

        
    # --- Controleer of de verwachte output gelijk is aan de gemaakt output
    # --------------------------------------
    out_df = out_df.sort_values(by=['id', 'vkbg']).reset_index(drop=True)
    expected_df = expected_df.sort_values(by=['id', 'vkbg']).reset_index(drop=True)
    cols = list(out_df.columns) # .remove('index') # de index kolom is niet gelijk

    assert(expected_df[cols].equals(out_df[cols]))
    
def test_2_make_counter():
    '''test the function make_counter in baglib if old_counter!=new_counter.'''

    # --- maak de input en de verwachte output
    # ------------------------------------------------

    in_df = pd.DataFrame( {
        'id':   ['id1',    'id1',    'id1', '  id2'],
        'vkid': [1,        2,        4,        1],
        'vkbg': [20130101, 20160202, 20190303, 20200404],
        'vkeg': [20160202, 20190303, 88888888, 88888888],
        'any' : ['a',      'b',      'c',      'd' ]
        })

    expected_df = pd.DataFrame( {
        'id':   ['id1',    'id1',    'id1', '  id2'],
        'vkid': [1,        2,        4,        1],
        'vkid2':[1,        2,        3,        1],        # <==== third item is changed
        'vkbg': [20130101, 20160202, 20190303, 20200404],
        'vkeg': [20160202, 20190303, 88888888, 88888888],
        'any' : ['a',      'b',      'c',      'd' ]
        })

    # --- berekenening output met functie make_counter
    # ------------------------------------------------
    logger.debug('making a new counter for wpl')
    out_df = baglib.make_counter(df=in_df, dfid='id', dfvkbg='vkbg',
                                 old_counter='vkid',
                                 new_counter='vkid2',
                                 logit=logger)

        
    # --- Controleer of de verwachte output gelijk is aan de gemaakt output
    # --------------------------------------
    out_df = out_df.sort_values(by=['id', 'vkbg']).reset_index(drop=True)
    expected_df = expected_df.sort_values(by=['id', 'vkbg']).reset_index(drop=True)
    cols = list(out_df.columns) # .remove('index') # de index kolom is niet gelijk

    assert(expected_df[cols].equals(out_df[cols]))


def test_3_make_counter():
    '''test the function make_counter in baglib if old_counter='' and 
    old_counter !=new_counter.'''

    # --- maak de input en de verwachte output
    # ------------------------------------------------

    in_df = pd.DataFrame( {
        'id':   ['id1',    'id1',    'id1', '  id2'],
        'vkbg': [20130101, 20160202, 20190303, 20200404],
        'vkeg': [20160202, 20190303, 88888888, 88888888],
        'any' : ['a',      'b',      'c',      'd' ]
        })

    expected_df = pd.DataFrame( {
        'id':   ['id1',    'id1',    'id1', '  id2'],
        'vkid': [1,        2,        3,        1],        # just create the counter
        'vkbg': [20130101, 20160202, 20190303, 20200404],
        'vkeg': [20160202, 20190303, 88888888, 88888888],
        'any' : ['a',      'b',      'c',      'd' ]
        })

    # --- berekenening output met functie make_counter
    # ------------------------------------------------
    logger.debug('making a new counter for wpl')
    out_df = baglib.make_counter(df=in_df, dfid='id', dfvkbg='vkbg',
                                 old_counter='',
                                 new_counter='vkid',
                                 logit=logger)

        
    # --- Controleer of de verwachte output gelijk is aan de gemaakt output
    # --------------------------------------
    out_df = out_df.sort_values(by=['id', 'vkbg']).reset_index(drop=True)
    expected_df = expected_df.sort_values(by=['id', 'vkbg']).reset_index(drop=True)
    cols = list(out_df.columns) # .remove('index') # de index kolom is niet gelijk

    assert(expected_df[cols].equals(out_df[cols]))

def test_merge_vk():
    '''test the function make_counter in baglib if old_counter!=new_counter.'''

    # --- maak de input en de verwachte output
    # ------------------------------------------------

    in_df = pd.DataFrame( {
        'bagid':   ['id1',    'id1',    'id1', '  id2'],
        'bagvkid': [1,        2,        3,        1],
        'bagvkbg': [20130101, 20160202, 20190303, 20200404],
        'bagvkeg': [20160202, 20190303, 88888888, 88888888],
        'bagveld': ['a',      'a',      'c',      'd' ]
        })

    expected_df = pd.DataFrame( {
        'bagid':   ['id1',              'id1', '  id2'],
        'bagvkid': [1,                  2,        1],
        'bagvkbg': [20130101,           20190303, 20200404],
        'bagvkeg': [20190303,           88888888, 88888888],
        'bagveld' : ['a',              'c',      'd' ]
        })

    # --- berekenening output met functie make_counter
    # ------------------------------------------------
    logger.debug('making a new counter for wpl')
    out_df = baglib.merge_vk(df=in_df, bob='bag', relevant_cols=['bagveld'],
                             logit=logger, df_type='pandas')

        
    # --- Controleer of de verwachte output gelijk is aan de gemaakt output
    # --------------------------------------
    out_df = out_df.sort_values(by=['bagid', 'bagvkbg']).reset_index(drop=True)
    expected_df = expected_df.sort_values(by=['bagid', 'bagvkbg']).reset_index(drop=True)
    cols = list(out_df.columns) # .remove('index') # de index kolom is niet gelijk



    assert(expected_df[cols].equals(out_df[cols]))


def test_fixvk_fijn_grof():
    '''test the function make_counter in baglib if old_counter!=new_counter.'''

    # --- 1. maak de input en de verwachte output
    # ------------------------------------------------

    # testdoel fid1: check splits als er een pand wijzigt gedurende de doorlooptijd,
    #                het nieuwe vbovk moet aan het juiste pndvk gekoppeld worden
    # testdoel fid2: check of vbovkbg wordt verhoogd naar pndvkbg
    # testdoel fid3: als vbovkid niet bij 1 begint, dan wordt deze op 1 gezet
    # testdoel fid4: het vbovk heeft twee panden met verschillende begindatum
    
    fijn_df = pd.DataFrame( {
        'vboid':   ['fid1',   'fid2',  'fid3'],
        'vbovkid': [1,        1,        2],
        'vbovkbg': [20130101, 20160202, 20190303],
        'vbovkeg': [88888888, 88888888, 88888888],
        'pndid':   ['gid1',   'gid2',   'gid2'],
        'vboveld': ['a',      'b',      'c']
        })

    grof_df = pd.DataFrame( {
        'pndid':   ['gid1',  'gid1',    'gid2'  ],
        'pndvkid': [1,        2,        1,      ],
        'pndvkbg': [20130101, 20160202, 20190303],
        'pndvkeg': [20160202, 88888888, 88888888],
        })
    # excpected behaviour of vboid's fid1 to fid4:
    # fid1 split because of gid1 split
    # fid2 unchanged
    # fid3 reset counter vbovkid

    expected_df = pd.DataFrame( {
        'vboid':   ['fid1',   'fid1',   'fid2'  , 'fid3'],
        'vbovkid': [1,        2,        1,        1],
        'vbovkbg': [20130101, 20160202, 20190303, 20190303],
        'vbovkeg': [20160202, 88888888, 88888888, 88888888],
        'pndid':   ['gid1',   'gid1',   'gid2'  , 'gid2'],
        'pndvkid': [1,        2,        1,        1],
        'vboveld': ['a',      'a',      'b'     , 'c']
        })

    expected_df = expected_df.astype(dtype={'vbovkid': np.short,
                                            'vbovkbg': np.uintc,
                                            'vbovkeg': np.uintc})
    ls_dict = {}
    bobs = ['vbo', 'pnd']
    for bob in bobs:
        ls_dict[bob] = {}
    
    # --- 2. berekenening output met functie make_counter
    # ------------------------------------------------
    logger.debug('making a new counter for wpl')
    out_df, _, _ = k2_fixvk.fixvk_fijngrof(fijntype_df=fijn_df,
                                           groftype_df=grof_df,
                                           fijntype='vbo',
                                           groftype='pnd',
                                           maand='202401',
                                           logit=logger, ls_dict=ls_dict)

        
    # --- 3. Controleer of de verwachte output gelijk is aan de gemaakt output
    # --------------------------------------

    out_df = out_df.sort_values(by=['vboid', 'vbovkbg']).reset_index(drop=True)
    expected_df = expected_df.sort_values(by=['vboid', 'vbovkbg']).reset_index(drop=True)
    cols = list(out_df.columns)
    test_geslaagd = expected_df[cols].equals(out_df[cols])

    # debuggen van de testcase
    if not test_geslaagd:
        print('expected output and output')
        print(expected_df.info())
        print(out_df.info())
        print('\nexpected output and output')
        print(expected_df)
        print()
        print(out_df)

    assert(test_geslaagd)


def test_fixvk_fijn_grof2():
    '''test the function make_counter in baglib if old_counter!=new_counter.'''

    # --- 1. maak de input en de verwachte output
    # ------------------------------------------------

    # testdoel fid: complexe vbo-pnd koppeling

    fijn_df = pd.DataFrame({
        'vboid':   ['fid4',        'fid4'],
        'vbovkid': [1,             1],
        'vbovkbg': [20110906,      20110906],
        'vbovkeg': [88888888,      88888888],
        'pndid':   ['gid1',        'gid2'],
        'vboveld': ['d',           'd']
    })

    grof_df = pd.DataFrame({
        'pndid':   ['gid1',   'gid1',   'gid2'],
        'pndvkid': [1,        2,        1, ],
        'pndvkbg': [20130101, 20160202, 20190303],
        'pndvkeg': [20160202, 88888888, 88888888],
    })

    # fid4 with gid1: split in two because of gid2 split

    expected_df = pd.DataFrame({
        'vboid':   ['fid4',   'fid4',   'fid4',   'fid4'],
        'vbovkid': [1,        2,        3,        3],
        'vbovkbg': [20130101, 20160202, 20190303, 20190303],
        'vbovkeg': [20160202, 20190303, 88888888, 88888888],
        'pndid':   ['gid1',   'gid1',   'gid1',   'gid2'],
        'pndvkid': [1,        2,        2,        1],
        'vboveld': ['d',      'd',      'd',      'd']
    })

    expected_df = expected_df.astype(dtype={'vbovkid': np.short,
                                            'vbovkbg': np.uintc,
                                            'vbovkeg': np.uintc})
    ls_dict = {}
    bobs = ['vbo', 'pnd']
    for bob in bobs:
        ls_dict[bob] = {}

    # --- 2. berekenening output met functie make_counter
    # ------------------------------------------------
    logger.debug('making a new counter for wpl')
    out_df, _, _ = k2_fixvk.fixvk_fijngrof(fijntype_df=fijn_df,
                                           groftype_df=grof_df,
                                           fijntype='vbo',
                                           groftype='pnd',
                                           maand='202401',
                                           logit=logger, ls_dict=ls_dict)

    # --- 3. Controleer of de verwachte output gelijk is aan de gemaakt output
    # --------------------------------------

    out_df = out_df.sort_values(by=['vboid', 'vbovkbg']).reset_index(drop=True)
    expected_df = expected_df.sort_values(by=['vboid', 'vbovkbg']).reset_index(drop=True)
    cols = list(out_df.columns)
    test_geslaagd = expected_df[cols].equals(out_df[cols])

    # debuggen van de testcase
    if not test_geslaagd:
        print('expected output and output')
        print(expected_df.info())
        print(out_df.info())
        print('\nexpected output and output')
        print(expected_df)
        print()
        print(out_df)

    assert (test_geslaagd)


def test_maak_vastgoed_bestandsnaam1():
        '''unzip een bestand in de map \\cbsp.nl\Productie\primair\WOVOR\Beheer\_Archief\INPUT
        submap <jaar> van de vorm BAGNLDL-08MMYYYY.zip.'''

        _input_month = 202302
        _expected = r'\\cbsp.nl\Productie\primair\WOVOR\Beheer\_Archief\INPUT\2023\BAGNLDL-08022023.zip'
        _output = k0_unzip.maak_vastgoed_bestandsnaam(_input_month)
        print('expected:', _expected)
        print('output:', _output)
        assert(_expected == _output)
        
    
    
    
    
''' klad aantekeningen:
    test_maak_vastgoed_bestandsnaam1()
    test_fixvk_fijn_grof()
    print(expected_df.info())
    print(out_df.info())
    print()
    print(expected_df)
    print()
    print(out_df)
    print(list(expected_df.columns))
    print(list(out_df.columns))



    # assert(expected_df.equals(out_df))
    
    # assert(out_df.to_dict() == expected_df.to_dict())
    # cols = ['id', 'vkid', 'vkbg', 'vkeg', 'any']
    # cols = ['wplid', 'wplstatus', 'wplvkid', 'wplvkeg', 'wplvkbg'] # , 'gemid']
    # print(expected_df.info())
    # print(out_df.info())
    print()
    print(expected_df)
    print()
    print(out_df)
    print(list(expected_df.columns))
    print(list(out_df.columns))
    
    in_df = pd.DataFrame( {
        'wplid': {0: '1017', 1: '1017', 2: '1018', 3: '1018', 4: '1121', 5: '1190', 6: '1809', 7: '1809', 8: '1809'},
        # 'gemid': {0: '18', 1: '1952', 2: '18', 3: '1952', 4: '893', 5: '1598', 6: '694', 7: '1927', 8: '1978'},
        'wplstatus': {0: 'defi', 1: 'defi', 2: 'defi', 3: 'defi', 4: 'defi', 5: 'defi', 6: 'defi', 7: 'defi', 8: 'defi'},
        'wplvkbg': {0: 20101229, 1: 20180101, 2: 20101229, 3: 20180101, 4: 20100723, 5: 20100705, 6: 20101102, 7: 20130101, 8: 20190101},
        'wplvkid': {0: 1, 1: 3, 2: 1, 3: 3, 4: 1, 5: 1, 6: 1, 7: 3, 8: 4},
        'wplvkeg': {0: 20180101, 1: 88888888, 2: 20180101, 3: 88888888, 4: 88888888, 5: 88888888, 6: 20130101, 7: 20190101, 8: 88888888}}
        )
    in_df = in_df.astype(dtype=baglib.make_subdict(in_df, BAG_TYPE_DICT))
   
    
  
    
    expected_df = pd.DataFrame( {
        'wplid': {0: '1017', 1: '1017', 2: '1018', 3: '1018', 4: '1121', 5: '1190', 6: '1809', 7: '1809', 8: '1809'},
        # 'gemid': {0: '18', 1: '1952', 2: '18', 3: '1952', 4: '893', 5: '1598', 6: '694', 7: '1927', 8: '1978'},
        'wplstatus': {0: 'defi', 1: 'defi', 2: 'defi', 3: 'defi', 4: 'defi', 5: 'defi', 6: 'defi', 7: 'defi', 8: 'defi'},
        'wplvkbg': {0: 20101229, 1: 20180101, 2: 20101229, 3: 20180101, 4: 20100723, 5: 20100705, 6: 20101102, 7: 20130101, 8: 20190101},
        'wplvkid': {0: 1, 1: 2, 2: 1, 3: 2, 4: 1, 5: 1, 6: 1, 7: 2, 8: 3},
        'wplvkeg': {0: 20180101, 1: 88888888, 2: 20180101, 3: 88888888, 4: 88888888, 5: 88888888, 6: 20130101, 7: 20190101, 8: 88888888}})
    expected_df = expected_df.astype(dtype=baglib.make_subdict(expected_df, BAG_TYPE_DICT))


    cols = ['id'] # , 'wplstatus', 'wplvkid', 'wplvkeg', 'wplvkbg']
    print(expected_df.info())
    print(out_df.info())
    print(expected_df)
    print(out_df)
    # print(baglib.give_testdata_lst('int1', 4))
    # print(baglib.give_testdata_lst('wplid', 4))

    cols = list(expected_df.columns)
    print(cols)
    cols = list(out_df.columns)
    print(cols)

'''