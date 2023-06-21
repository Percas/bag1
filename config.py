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
from multiprocessing import cpu_count

# ################ file locations ###############################


# ################ define datastructures ###############################

OMGEVING = 'Ontwikkelomgeving'
FUTURE_DATE = 88888888
BATCH_SIZE = 500
FILE_EXT = 'parquet'
BAG_URL = 'https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip'
BAG_VASTGOEDMAP = '\\\\cbsp.nl\\Productie\\primair\\WOVOR\\Beheer\\_Archief\\INPUT\\'


# FILE_EXT = 'csv'

IN_VOORRAAD = ['v3', 'v4', 'v8', 'v6']
    
BASISDIR = os.path.join('..', 'data')
TESTDIR = 'testdata'

KOPPELVLAK0 = os.path.join(BASISDIR, '00-inkomend')
KOPPELVLAK1 = os.path.join(BASISDIR, '01-uitgepakt')
KOPPELVLAK2 = os.path.join(BASISDIR, '02-gestandaardiseerd')
KOPPELVLAK3a = os.path.join(BASISDIR, '3a-bewerkt')
KOPPELVLAK4 = os.path.join(BASISDIR, '04-aggr')
LOGFILE = os.path.join(BASISDIR, 'bag.log')
NR_WORKERS = 2 # cpu_count()

DF_TYPE = 'pandas'

BAG_OBJECTEN = ['vbo', 'pnd', 'num', 'lig', 'sta', 'opr', 'wpl']

COLS_DICT = {
    'vbo': ['vboid','vbovkid', 'vbovkbg', 'vbovkeg', 'vbostatus', 'numid',
            'oppervlakte', 'pndid', 
            # 'woon', 'gezo', 'indu', 'over', 'ondr' ,'logi', 'kant', 'wink',
            # 'bij1', 'celf', 'sprt',
            'vbogmlx', 'vbogmly'],
    'pnd': ['pndid', 'pndvkid', 'pndvkbg', 'pndvkeg',
            'pndstatus', 'bouwjaar', 'docnr', 'docdd', 'pndgmlx', 'pndgmly'],
    'lig': ['ligid', 'ligvkid', 'ligvkbg', 'ligvkeg', 'ligstatus', 
            'numid', 'docnr', 'docdd', 'liggmlx', 'liggmly'],
    'num': ['numid', 'numvkid', 'numvkbg', 'numvkeg', 'numstatus',
            'huisnr', 'postcode', 'typeao', 'oprid'],
    'opr': ['oprid', 'oprvkid', 'oprvkbg', 'oprvkeg', 'oprstatus', 
            'oprnaam', 'oprtype', 'wplid'],
    'sta': ['staid', 'stavkid', 'stavkbg', 'stavkeg', 'stastatus',
            'numid', 'docnr', 'docdd', 'stagmlx', 'stagmly'],
    'wpl': ['wplid', 'wplvkid', 'wplvkbg', 'wplvkeg', 'wplstatus', 'wplnaam']
    }

# Nu volgen de kolommen op basis waarvan een wijziging een nieuw voorkomen rechtvaardigt:

RELEVANT_COLS_DICT = {'vbo': ['vbostatus', 'numid', 'oppervlakte' , 'pndid', 
                              # 'woon', 'gezo' ,'indu', 'over', 'ondr', 'logi' ,'kant', 'wink', 'bij1' , 'celf' , 'sprt' , 
                              'vbogmlx' , 'vbogmly'],
                      'pnd': ['pndstatus', 'bouwjaar', 'docnr', 'docdd', 'pndgmlx', 'pndgmly'],
                      'num': ['numstatus', 'huisnr', 'postcode', 'typeao', 'oprid'],
                      'opr': ['oprstatus', 'oprnaam', 'oprtype', 'wplid'],
                      'wpl': ['wplstatus', 'gemid']}



BAG_TYPE_DICT = {'vboid': 'string',
                 'pndid': 'string',
                 'numid': 'string',
                 'oprid': 'string',
                 'wplid': 'string',
                 'gemid': 'string',
                 'staid': 'string',
                 'ligid': 'string',
                 'vbovkid': np.short,
                 # 'vbovkid_org': np.short,
                 'pndvkid': np.short,
                 'numvkid': np.short,
                 'oprvkid': np.short,
                 'wplvkid': np.short,
                 'vbovkid2': np.short,
                 'pndvkid2': np.short,
                 'numvkid2': np.short,
                 'oprvkid2': np.short,
                 'wplvkid2': np.short,
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
                 'indu': bool,                 'celf': bool,
                 'vbogmlx': float,
                 'vbogmly': float,
                 'pndgmlx': float,
                 'pndgmly': float,
                 'liggmlx': float,
                 'liggmly': float,
                 'stagmlx': float,
                 'stagmly': float,
                 'prio': float,
                 'midden': float,
                 'inliggend': np.uintc                 
                 }

STATUS_DICT = {
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
    'Woonplaats ingetrokken':                       'w2',
    'definitief':                                   'defi',
    'voorlopig':                                    'vrlg'
}

# shorthands to translate directory names to tags and so
SHORT = {'vbo': 'Verblijfsobject',
         'lig': 'Ligplaats',
         'sta': 'Standplaats',
         'pnd': 'Pand',
         'num': 'Nummeraanduiding',
         'opr': 'OpenbareRuimte',
         'wpl': 'Woonplaats'
         }

GEBRUIKSDOEL_DICT = {
    'woonfunctie':              'woon',
    'overige gebruiksfunctie':  'over',
    'kantoorfunctie':           'kant',
    'gezondheidszorgfunctie':   'gezo',
    'bijeenkomstfunctie':       'bij1',
    'onderwijsfunctie':         'ondr',
    'winkelfunctie':            'wink',
    'sportfunctie':             'sprt',
    'logiesfunctie':            'logi',
    'industriefunctie':         'indu',
    'celfunctie':               'celf'
}


LIGTYPE_DICT = {
        'Verblijfsobject':       0,
        'Standplaats':           1,
        'Ligplaats':             2,
        'Pand':                  3,
        'Nummeraanduiding':      4,
        'Openbareruimte':        5,
        'Woonplaats':            6
}

NS = {'Objecten': "www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601",
      'gml': "http://www.opengis.net/gml/3.2",
      'Historie': "www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601",
      'Objecten-ref':
          "www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601",
      }   

GEM_NS = {'gwr-bestand': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/gwr-deelbestand-lvc/v20200601",
          'selecties-extract': "http://www.kadaster.nl/schemas/lvbag/extract-selecties/v20200601",
          'bagtypes': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/bag-types/v20200601",
          'gwr-product': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/gwr-producten-lvc/v20200601",
          'DatatypenNEN3610': "www.kadaster.nl/schemas/lvbag/imbag/datatypennen3610/v20200601"}

TEST_D = {
    'wplid': ['1000', '1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010', '1011', '1012', '1013', '1014'],
    'oprid': ['612300000000923', '612300000000924', '612300000000925', '612300000000926', '612300000000927', '612300000000928', '612300000000929', '612300000000930', '612300000000931', '612300000000932', '612300000000933', '612300000000934', '612300000000935', '612300000000936', '612300000000937'],
    'numid': ['345200002027798', '575200000053445', '1705200000795207', '193200000047121', '228200000050877', '772200000033673', '9200007674074', '392200000063724', '297200000013612', '518200000625782', '971200000051289', '511200001970854', '363200000210105', '1708200000008690', '363200000256893'],
    'pndid': ['197100000013312', '197100000020048', '197100000028736', '197100000026532', '197100000030744', '197100000022290', '197100000031210', '197100000014200', '197100000020890', '197100000020762', '197100000020849', '197100000018727', '197100000027608', '197100000026012', '197100000025933'],
    'vboid': ['197010000002391', '197010000002444', '197010000002499', '197010000002510', '197010000002568', '197010000002586', '197010000002630', '197010000003238', '197010000003393', '197010000003517', '197010000003607', '197010000003684', '197010000003820', '197010000003824', '197010000004068'],
    'vkbg': [19910109, 19881005, 20080709, 19590514, 19860205, 19860326, 19800626, 19700312, 20100525, 19580410, 19920930, 19830309, 19660609, 19831207, 19810611]    
    }




