#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
9 juni, Anton
version: 0.2
doel: uitpakken van de gezipte BAG XML bestanden van koppelvlak 0 naar koppelvlak 1
        probeer zip bestand te downloaden van kadaster als niet in de inputmap staat
"""

# ################ import libraries ###############################
import sys
import os
import baglib
import zipfile
import time
from config import OMGEVING, DIR00, DIR01

# ############### Define functions ################################

# Main function for this package:

def bag_unzip(current_month='testdata02',
              koppelvlak0=DIR00,
              koppelvlak1=DIR01,
              loglevel=20):
    '''Uitpakken van door Kadaster gezipte XML bestanden. Eerst downloaden indien nodig.''' 

    tic = time.perf_counter()

    _ll = loglevel
    baglib.printkop(_ll+40, 'Start bag_unzip')
    
    inputdir = os.path.join(koppelvlak0, current_month)
    outputdir = os.path.join(koppelvlak1, current_month)
    unzip_files = os.listdir(inputdir)
    
    
    
    
    baglib.printkop(_ll+40, 'Stap 0. Check of er al zip files staan in koppelvlak 0')
    
    aantal_unzip = len(unzip_files)
    baglib.aprint(_ll+20, '\tAantal files in de unzip dir:', aantal_unzip)
    if aantal_unzip == 0:
        baglib.aprint(_ll+20, '\tInput dir leeg; probeer te downloaden van kadaster...')
        bagurl = 'https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip'
        bagfile = baglib.download_file(bagurl)
        # bagfile = 'lvbag-extract-nl.zip' if it's allready there
        baglib.aprint(_ll+20, '\tDownloaden van', bagfile, 'gereed. Nu unzippen naar inputmap')
        with zipfile.ZipFile(bagfile, 'r') as zip_ref:
            zip_ref.extractall(inputdir)
            baglib.aprint(_ll+20, '\tZojuist uitgepakt BAG bestand verwijderen')
            os.remove(bagfile)
            baglib.aprint(_ll+20, '\tUnzippen stap 0 gereed. Verder met unzippen in stap 1')
            # inputdir opnieuw lezen want nu staat er wel wat in de inputdir!
            unzip_files = os.listdir(inputdir)




    baglib.printkop(_ll+40, '\tStap 1. Unzippen van de gezipte bagobjecten naar xml')
    
    bagobj_starts_with = {'vbo': '9999VBO',
                          'lig': '9999LIG',
                          'sta': '9999STA',
                          'pnd': '9999PND',
                          'num': '9999NUM',
                          'opr': '9999OPR',
                          'wpl': '9999WPL',
                          'wplgem': 'GEM-WPL-RELATIE'}

    for bagobj in bagobj_starts_with.keys():
        for unzip_file_name in unzip_files:
            if unzip_file_name.startswith(bagobj_starts_with[bagobj]):
                unzip_dir = os.path.join(outputdir, bagobj)
                unzip_file = os.path.join(inputdir, unzip_file_name)
                baglib.make_dir(unzip_dir, loglevel=_ll)
                baglib.aprint(_ll+10, '\n\tUitpakken van bestand', unzip_file_name,
                              '\n\tin directory', inputdir,
                              '\n\tnaar directory', unzip_dir, '...')
                _ti = time.perf_counter()
                with zipfile.ZipFile(unzip_file, 'r') as zip_ref:
                    zip_ref.extractall(unzip_dir)
                _to = time.perf_counter()
                baglib.aprint(_ll+40,'\tfile', unzip_file_name, 'unzipped in', (_to - _ti)/60, 'min')
                                  
    toc = time.perf_counter()

    baglib.aprint(_ll+40, '\n*** Einde bag_unzip in', (toc - tic)/60, 'min ***\n')



# ########################################################################
# ########################################################################

if __name__ == '__main__':

    ll = 20
    baglib.printkop(ll+40, OMGEVING)
    current_month = baglib.get_arg1(sys.argv, DIR00)

    baglib.printkop(ll+30, 'Lokale aanroep')
    bag_unzip(current_month=current_month,
              koppelvlak0=DIR00,
              koppelvlak1=DIR01,
              loglevel=ll)
