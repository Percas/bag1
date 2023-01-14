#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


Created on Sat Mar  5 12:40:26 2022
@author: anton
Purpose: convert WPLGEM BAG XML to CSV file
version 0.3


# #### Typical XML layout ##########################################

<gwr-product:GemeenteWoonplaatsRelatie>
  <gwr-product:tijdvakgeldigheid>
    <bagtypes:begindatumTijdvakGeldigheid>2010-07-23</bagtypes:begindatumTijdvakGeldigheid>
    <bagtypes:einddatumTijdvakGeldigheid>2010-07-24</bagtypes:einddatumTijdvakGeldigheid>
  </gwr-product:tijdvakgeldigheid>
  <gwr-product:gerelateerdeWoonplaats>
    <gwr-product:identificatie>1125</gwr-product:identificatie>
  </gwr-product:gerelateerdeWoonplaats>
  <gwr-product:gerelateerdeGemeente>
    <gwr-product:identificatie>0893</gwr-product:identificatie>
  </gwr-product:gerelateerdeGemeente>
  <gwr-product:status>definitief</gwr-product:status>
</gwr-product:GemeenteWoonplaatsRelatie>

"""

# ################ Import libraries ###############################
import xml.etree.ElementTree as ET
import os
import pandas as pd
import sys
import baglib
import time
from config import LOCATION

# ############### Define functions #################################
def bag_wplgem2csv(current_month='testdata',
                   koppelvlak1='../data/01-xml/',
                   koppelvlak2='../data/02-csv/',
                   loglevel=True):

    tic = time.perf_counter()
    print('-------------------------------------------')
    print('------------- Start bag_wplgem2csv ---------- ')
    print('-------------------------------------------')

    INPUTDIR = koppelvlak1 + current_month + '/'
    OUTPUTDIR = koppelvlak2 + current_month + '/'
    baglib.make_dir(OUTPUTDIR)
    
    print('Huidige maand (verslagmaand + 1):', current_month)

    # namespace stuff we have to deal with
    ns = {'gwr-bestand': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/gwr-deelbestand-lvc/v20200601",
          'selecties-extract': "http://www.kadaster.nl/schemas/lvbag/extract-selecties/v20200601",
          'bagtypes': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/bag-types/v20200601",
          'gwr-product': "www.kadaster.nl/schemas/lvbag/gem-wpl-rel/gwr-producten-lvc/v20200601",
          'DatatypenNEN3610': "www.kadaster.nl/schemas/lvbag/imbag/datatypennen3610/v20200601"}
    
    # shorthands to translate directory names to tags and so
    # short = {'wplgem': 'GemeenteWoonplaatsRelatie'}

    status_dict = {
        'definitief':                                   'defi',
        'voorlopig':                                    'vrlg'} 
    
    futureday_str = '20321231'

    # ######### works just for wplgem                ###########

    bagobject = 'wpl'
    subdir = 'wplgem/'
    ddir = INPUTDIR + subdir
    bag_files = os.listdir(ddir)
    print('\n\tGemeente-woonplaats map bevat', len(bag_files), 'bestand')
    output_dict = []           # list of dict containing output records
    input_bagobject_count = 0
    output_bagobject_count = 0
    file_count = 0
    report_count = 0

    # ######### Loop over files in a directory with same bag objects #####
    for inputfile in bag_files:
        bagtree = ET.parse(ddir + inputfile)
        root = bagtree.getroot()
        # tag = '{' + ns['gwr-product'] + '}' + short[bagobject]
        tag = '{' + ns['gwr-product'] + '}' + 'GemeenteWoonplaatsRelatie'
        input_bagobject_filecount = 0
        output_bagobject_filecount = 0
        file_count += 1
        report_count += 1
        # print(inputfile)
        # ######### Loop over bagobjects in one bag file       #####
        for level0 in root.iter(tag):   # level0 is the bagobject-tree
            input_bagobject_filecount += 1
            wpl = baglib.assigniffound(level0, ['gwr-product:gerelateerdeWoonplaats',
                                                'gwr-product:identificatie'], ns)
            gem = baglib.assigniffound(level0, ['gwr-product:gerelateerdeGemeente',
                                                'gwr-product:identificatie'], ns)
            status = status_dict[
                baglib.assigniffound(level0, ['gwr-product:status'], ns)]
    
            date_str = baglib.assigniffound(level0,
                                            ['gwr-product:tijdvakgeldigheid',
                                             'bagtypes:begindatumTijdvakGeldigheid'],
                                            ns)
            vkbg = baglib.date2int(date_str)
            date_str = baglib.assigniffound(level0, ['gwr-product:tijdvakgeldigheid',
                                                     'bagtypes:einddatumTijdvakGeldigheid'],
                                            ns, futureday_str)
            vkeg = baglib.date2int(date_str)
    
            output_record = {'wplid':   wpl,
                             'gemid':   gem,
                             'wplstatus':  status,
                             'wplvkbg':    vkbg,
                             'wplvkeg':    vkeg}
    
            output_dict.append(output_record.copy())
            output_bagobject_filecount += 1
    
        input_bagobject_count += input_bagobject_filecount
        output_bagobject_count += output_bagobject_filecount
        print(".", end='')
        if report_count == 100:
            print(file_count, 'of', len(bag_files))
            report_count = 0
    df = pd.DataFrame.from_dict(output_dict)
    df.index.name = 'idx'

    # print(df.info()) 
    
    print('\n\tVoeg wplvkid toe')
    print('\n\tDeze actie hoort thuis tussen koppelvlak 2 en 3, maar wordt\n',
          '\thier uitgevoerd zodat ook dit bestand een vkid heeft, net als alle andere.')
    df = df.sort_values(['wplid', 'wplvkbg'])
    # print(df.head(20))
    df = baglib.make_counter(20, df, 'wplid', 'wplvkid', ['wplid', 'wplvkbg'])
    # print(df[df['wplvkid']==2].head(20))
    # print(df.head(50))
    
    outputfile = OUTPUTDIR + bagobject + '.csv'
    print('\nOutputfile:', bagobject + '.csv',
          ', records in:', input_bagobject_count,
          ', records aangemaakt:', output_bagobject_count,
          bagobject, '\n')
    
    df.to_csv(outputfile, index=False)

# --------------------------------------------------------------------------
# ################ Main program ###########################
# --------------------------------------------------------------------------

if __name__ == '__main__':
    
    print('-------------------------------------------')
    print('-------------', LOCATION['OMGEVING'], '-----------')
    print('-------------------------------------------\n')

    DATADIR_IN = LOCATION['DATADIR_IN']
    DATADIR_OUT = LOCATION['DATADIR_OUT']
    DIR00 = DATADIR_IN + '00-zip/'
    DIR01 = DATADIR_OUT + '01-xml/'
    DIR02 = DATADIR_OUT + '02-csv/'
    DIR03 = DATADIR_OUT + '03-bewerktedata/'
    current_month = baglib.get_arg1(sys.argv, DIR01)

    printit=True

    bag_wplgem2csv(current_month=current_month,
                koppelvlak1=DIR01,
                koppelvlak2=DIR02,
                loglevel=printit)


