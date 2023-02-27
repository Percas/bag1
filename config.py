#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Config file
"""
# ################ import libraries ###############################

# import pandas as pd
# import time
# import logging
# import os
# import sys
# import datetime
# import logging
# import numpy as np


# ################ file locations ###############################


# ################ define datastructures ###############################
LOCATION = {
    'OMGEVING': 'Productieomgeving',
    'DATADIR_IN': '/media/lin/fam/',
    'DATADIR_OUT': '../data/'}
FUTURE_DATE = 20321231
    
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
