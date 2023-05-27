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
from pathlib import PureWindowsPath
import baglib
import zipfile
import time
from config import KOPPELVLAK0, KOPPELVLAK1, BAG_OBJECTEN, LOGFILE
# from k1_xml import k1_xml 
import logging

# ############### Define functions ################################

# Main function for this package:

def k0_download_and_unzip(url, maand, logit):
    '''Download zipped file from url.'''    
    dir_k0_maand = os.path.join(KOPPELVLAK0, str(maand))
    logit.info(f'downloaden bestand van {url}...')
    # url = 'https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip'
    downloaded_file = baglib.download_file(url)
    # bagfile = 'lvbag-extract-nl.zip' if it's allready there
    logit.info(f'downloaden van {downloaded_file} gereed. Nu unzippen')
    with zipfile.ZipFile(downloaded_file, 'r') as zip_ref:
        zip_ref.extractall(dir_k0_maand)
        logit.info('zojuist uitgepakt BAG bestand verwijderen')
        os.remove(downloaded_file)
        logit.info('unzippen gedownloaded bestand gereed')
        # dir_k0_maand opnieuw lezen want nu staat er wel wat in de dir_k0_maand!

def maak_vastgoed_bestandsnaam(maand):
    '''unzip een bestand in de map \\cbsp.nl\Productie\primair\WOVOR\Beheer\_Archief\INPUT
    submap <jaar> van de vorm BAGNLDL-08MMYYYY.zip.'''
    _maand = str(maand)
    if len(_maand) != 6:
        sys.exit(f'yyyymm verwacht, maar kreeg {maand}')
    _jaar = _maand[:4]
    _zip_file = 'BAGNLDL-08' + _maand[-2:] + _jaar + '.zip'
    return '\\\\cbsp.nl\\Productie\\primair\\WOVOR\\Beheer\\_Archief\\INPUT\\' +\
        _jaar + '\\' + _zip_file

    # return PureWindowsPath(os.path.join('cbsp.nl', 'Productie', 'primair',
    #                                     'WOVOR', 'Beheer', '_Archief', 'INPUT',
    #                                     _jaar, _zip_file))
    

def k0_unzip_vastgoed_bestand(maand, logit):
    '''unzip een door team Vastgoed gedownload bestand en zet het resultaat
    in koppelvlak0.'''
    logit.info(f'start k0_unzip_vastgoed_bestand met maand {maand}')
    te_unzippen_bestand = maak_vastgoed_bestandsnaam(maand)
    unzip_dir = os.path.join(KOPPELVLAK0, str(maand))
    baglib.make_dirs(unzip_dir, logit)
    with zipfile.ZipFile(te_unzippen_bestand, 'r') as zip_ref:
        zip_ref.extractall(unzip_dir)
    logit.debug(f'bestand van maand {maand} bewaard in {unzip_dir}')
    

def k0_unzip(bagobject, maand, logit):
    '''Unzip bagobject bestand van kadaster voor gegeven maand.'''
    tic = time.perf_counter()
    logit.debug(f'start functie k0_unzip met {bagobject} en {maand}')

    # input
    dir_k0_maand = os.path.join(KOPPELVLAK0, str(maand))
    unzip_files = os.listdir(dir_k0_maand)

    #output
    dir_k1_maand = os.path.join(KOPPELVLAK1, str(maand))
    unzip_dir = os.path.join(dir_k1_maand, bagobject)
    baglib.make_dirs(unzip_dir, logit)
    

    for unzip_file_name in unzip_files:
        unzip_file = os.path.join(dir_k0_maand, unzip_file_name)
        bagobj_starts_with = {'vbo': '9999VBO',
                              'lig': '9999LIG',
                              'sta': '9999STA',
                              'pnd': '9999PND',
                              'num': '9999NUM',
                              'opr': '9999OPR',
                              'wpl': '9999WPL',
                              'wplgem': 'GEM-WPL-RELATIE'}
        if unzip_file_name.startswith(bagobj_starts_with[bagobject]):
            with zipfile.ZipFile(unzip_file, 'r') as zip_ref:
                zip_ref.extractall(unzip_dir)
            logit.debug(f'bestand {unzip_file} bewaard in {unzip_dir}')
    toc = time.perf_counter()
    logit.info(f'einde k0_unzip {bagobject}, {maand} in {(toc - tic)/60} min')

# ########################################################################
# ########################################################################

if __name__ == '__main__':

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOGFILE),
            logging.StreamHandler()])    

    logit = logging.getLogger()
    logit.debug('start k0_unzip vanuit main')
    maand_lst = baglib.get_args(sys.argv, KOPPELVLAK0)
    url = 'https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip'


    bag_objecten_wplgem = BAG_OBJECTEN + ['wplgem']
    for maand in maand_lst:

        dir_k0_maand = os.path.join(KOPPELVLAK0, str(maand))
        unzip_files = os.listdir(dir_k0_maand)
        aantal_unzip = len(unzip_files)
    
        if aantal_unzip == 0:
            k0_download_and_unzip(url, maand, logit)

        for bagobject in bag_objecten_wplgem:
            k0_unzip(bagobject, maand, logit)
