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
from config import OMGEVING, DIR01, DIR02, FUTURE_DATE


# ############### Define functions #################################
def bag_wplgem2csv(current_month='testdata02',
                   koppelvlak1=DIR01,
                   koppelvlak2=DIR02,
                   file_ext='parquet',
                   loglevel=20):

    tic = time.perf_counter()
    ll = loglevel
    baglib.printkop(ll+40, 'Start bag_wplgem2csv')

    INPUTDIR = os.path.join(koppelvlak1, current_month)
    OUTPUTDIR = os.path.join(koppelvlak2, current_month)
    baglib.make_dir(OUTPUTDIR)
    baglib.aprint(ll+30, 'Huidige maand (verslagmaand + 1):', current_month)


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
    
    # futureday_str = '20321231'

    # ######### works just for wplgem                ###########

    bagobject = 'wpl'
    subdir = 'wplgem/'
    ddir = os.path.join(INPUTDIR, subdir)
    bag_files = os.listdir(ddir)
    baglib.aprint(ll+20, '\n\tGemeente-woonplaats map bevat', len(bag_files), 'bestand')
    output_dict = []           # list of dict containing output records
    input_bagobject_count = 0
    output_bagobject_count = 0
    file_count = 0
    report_count = 0

    # ######### Loop over files in a directory with same bag objects #####
    for inputfile in bag_files:
        bagtree = ET.parse(os.path.join(ddir, inputfile))
        root = bagtree.getroot()
        # tag = '{' + ns['gwr-product'] + '}' + short[bagobject]
        tag = '{' + ns['gwr-product'] + '}' + 'GemeenteWoonplaatsRelatie'
        input_bagobject_filecount = 0
        output_bagobject_filecount = 0
        file_count += 1
        report_count += 1
        baglib.aprint(ll, inputfile)
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
                                            ns, str(FUTURE_DATE))
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
        if ll+20 >= 40:
            print(".", end='')
        if report_count == 100:
            baglib.aprint(ll, file_count, 'of', len(bag_files))
            report_count = 0
    df = pd.DataFrame.from_dict(output_dict)
    df.index.name = 'idx'

    # baglib.aprint(ll-10, df.info()) 
    


    baglib.aprint(ll+20, '\n\tVoeg wplvkid toe')
    baglib.aprint(ll+20, '\n\tDeze actie hoort thuis tussen koppelvlak 2 en 3, maar wordt\n',
          '\thier uitgevoerd zodat ook dit bestand een vkid heeft, net als alle andere.')
    df = df.sort_values(['wplid', 'wplvkbg'])
    # baglib.aprint(ll+40, df.head(20))
    df = baglib.make_counter(20, df, 'wplid', 'wplvkid', ['wplid', 'wplvkbg'])
    # baglib.aprint(ll+40, df[df['wplvkid']==2].head(20))
    # baglib.aprint(ll+40, df.head(50))
    


    outputfile = os.path.join(OUTPUTDIR, bagobject)
    baglib.aprint(ll+20, '\n\toutputfile:', bagobject + '.' + file_ext,
          ', records in:', input_bagobject_count,
          ', records aangemaakt:', output_bagobject_count,
          bagobject, '\n')
    
    baglib.save_df2file(df=df, outputfile=outputfile, file_ext=file_ext,
                        append=False, includeindex=False, loglevel=ll)
    # df.to_csv(outputfile, index=False)

    toc = time.perf_counter()
    baglib.aprint(ll+40, '\n*** Einde bag_wplgem2csv in', (toc - tic)/60, 'min ***\n')
# --------------------------------------------------------------------------
# ################ Main program ###########################
# --------------------------------------------------------------------------

if __name__ == '__main__':
    
    ll = 20
    # file_ext = 'csv'
    file_ext = 'parquet'
    
    baglib.printkop(ll+40, OMGEVING)    
    current_month = baglib.get_arg1(sys.argv, DIR01)

    
    bag_wplgem2csv(current_month=current_month,
                koppelvlak1=DIR01,
                koppelvlak2=DIR02,
                file_ext=file_ext,
                loglevel=ll)
    


