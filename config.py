#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Config file
"""
# ################ import libraries ###############################

# import pandas as pd
# import time
# import logging
import os
# import sys
# import datetime
# import logging
import numpy as np


# ################ file locations ###############################


# ################ define datastructures ###############################

OMGEVING = 'Ontwikkelomgeving'
FUTURE_DATE = 88888888
IN_VOORRAAD = ['v3', 'v4', 'v8', 'v6']
    
DATADIR_IN = os.path.join('..', 'data')
DATADIR_OUT = os.path.join('..', 'data')
DATADIR = os.path.join('..', 'data')
DIR00 = os.path.join(DATADIR_IN, '00-zip')
DIR01 = os.path.join(DATADIR_OUT, '01-xml')
DIR02 = os.path.join(DATADIR_OUT, '02-csv')
DIR03 = os.path.join(DATADIR_OUT, '03-bewerktedata')
DIR04 = os.path.join(DATADIR_OUT, '04-aggr')

BAG_TYPE_DICT = {'vboid': 'string',
                 'pndid': 'string',
                 'numid': 'string',
                 'oprid': 'string',
                 'wplid': 'string',
                 'gemid': 'string',
                 'vbovkid': np.short,
                 'vbovkid_org': np.short,
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
                 'docnr': 'str',
                 'postcode': 'str',
                 'huisnr': 'str',
                 'oprnaam': 'str',
                 'wplnaam': 'str',
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

status_dict = {
    'Plaats ingetrokken':                           's2',
    'Plaats aangewezen':                            's1',
    'Naamgeving ingetrokken':                       's3',
    'Naamgeving aangewezen':                        's4',
    'Naamgeving uitgegeven':                        'w3',
    'Verblijfsobject gevormd':                      'v1',
    'Niet gerealiseerd verblijfsobject':            'v2',
    'Verblijfsobject in gebruik (niet ingemeten)':  'v3',
    'Verblijfsobject in gebruik':                   'v4',
    'Verblijfsobject ingetrokken':                  'v5',
    'Verblijfsobject buiten gebruik':               'v6',
    'Verblijfsobject ten onrechte opgevoerd':       'v7',
    'Verbouwing verblijfsobject':                   'v8',
    'Bouwvergunning verleend':                      'p1',
    'Niet gerealiseerd pand':                       'p2',
    'Bouw gestart':                                 'p3',
    'Pand in gebruik (niet ingemeten)':             'p4',
    'Pand in gebruik':                              'p5',
    'Sloopvergunning verleend':                     'p6',
    'Pand gesloopt':                                'p7',
    'Pand buiten gebruik':                          'p8',
    'Pand ten onrechte opgevoerd':                  'p9',
    'Verbouwing pand':                              'p0',
    'Woonplaats aangewezen':                        'w1',
    'Woonplaats ingetrokken':                       'w2'}



