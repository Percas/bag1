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
from config import OMGEVING, KOPPELVLAK0, KOPPELVLAK1, BAG_OBJECTEN

# ############### Define functions ################################

# Main function for this package:

def k0_unzip(maand, bagobject):
    '''Schil om bag_unzip t.b.v. multiprocessing.'''
    print('unzipping', bagobject, 'in maand', str(maand))    

    tic = time.perf_counter()
    baglib.printkop(40,'Start bag_unzip')
    
    dir_k0_maand = os.path.join(KOPPELVLAK0, str(maand))
    dir_k1_maand = os.path.join(KOPPELVLAK1, str(maand))
    unzip_files = os.listdir(dir_k0_maand)
    
    
    
    
    baglib.printkop(40, 'Stap 0. Check of er al zip files staan in koppelvlak 0')
    
    aantal_unzip = len(unzip_files)
    
    '''
    if aantal_unzip == 0:
        baglib.aprint(20, '\tInput dir leeg; probeer te downloaden van kadaster...')
        bagurl = 'https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip'
        bagfile = baglib.download_file(bagurl)
        # bagfile = 'lvbag-extract-nl.zip' if it's allready there
        baglib.aprint(20, '\tDownloaden van', bagfile, 'gereed. Nu unzippen naar inputmap')
        with zipfile.ZipFile(bagfile, 'r') as zip_ref:
            zip_ref.extractall(dir_k0_maand)
            baglib.aprint(20, '\tZojuist uitgepakt BAG bestand verwijderen')
            os.remove(bagfile)
            baglib.aprint(20, '\tUnzippen stap 0 gereed. Verder met unzippen in stap 1')
            # dir_k0_maand opnieuw lezen want nu staat er wel wat in de dir_k0_maand!
            unzip_files = os.listdir(dir_k0_maand)
    '''

    baglib.printkop(40, '\tStap 1. Unzippen van gezipt '+bagobject+' bestand naar xml')
    
    bagobj_starts_with = {'vbo': '9999VBO',
                          'lig': '9999LIG',
                          'sta': '9999STA',
                          'pnd': '9999PND',
                          'num': '9999NUM',
                          'opr': '9999OPR',
                          'wpl': '9999WPL',
                          'wplgem': 'GEM-WPL-RELATIE'}

    for unzip_file_name in unzip_files:
        if unzip_file_name.startswith(bagobj_starts_with[bagobject]):
            unzip_dir = os.path.join(dir_k1_maand, bagobject)
            unzip_file = os.path.join(dir_k0_maand, unzip_file_name)
            baglib.make_dir(unzip_dir, loglevel=10)
            baglib.aprint(10, '\n\tUitpakken van bestand', unzip_file_name,
                          '\n\tin directory', dir_k0_maand,
                          '\n\tnaar directory', unzip_dir, '...')
            _ti = time.perf_counter()
            with zipfile.ZipFile(unzip_file, 'r') as zip_ref:
                zip_ref.extractall(unzip_dir)
            _to = time.perf_counter()
            baglib.aprint(40,'\tfile', unzip_file_name, 'unzipped in', (_to - _ti)/60, 'min')
                                  
    toc = time.perf_counter()

    baglib.aprint(40, '\n*** Einde bag_unzip in', (toc - tic)/60, 'min ***\n')



# ########################################################################
# ########################################################################

if __name__ == '__main__':

    ll = 20
    baglib.printkop(ll+40, OMGEVING)
    maand = baglib.get_arg1(sys.argv, KOPPELVLAK0)


    baglib.printkop(ll+30, 'Lokale aanroep')
    for bagobject in BAG_OBJECTEN:
        k0_unzip(maand, bagobject)
