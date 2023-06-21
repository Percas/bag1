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
# from pathlib import PureWindowsPath
import baglib
import zipfile
import time
from config import KOPPELVLAK0, KOPPELVLAK1, BAG_OBJECTEN, LOGFILE, BAG_URL, BAG_VASTGOEDMAP
# from k1_xml import k1_xml 
import logging

# ############### Define functions ################################

# Main function for this package:

def k0_unzip(bagobject, maand, logit):
    '''Unzip bagobject bestand van kadaster voor gegeven maand.'''
    tic = time.perf_counter()
    logit.info(f'start functie k0_unzip met {bagobject} en {maand}')

    # input
    dir_k0_maand = os.path.join(KOPPELVLAK0, str(maand))
    unzip_files = os.listdir(dir_k0_maand)

    aantal_unzip = len(unzip_files)

    if aantal_unzip == 0:
        logit.info('geen te unzippen bestanden gevonden. zoek main_bag_zip bestand...')
        get_main_bag_zip(maand, logit)

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


def get_main_bag_zip(maand, logit):
    '''Kopieer het bag input zip bestand van het netwerk of download het.'''

    # uit config.py
    # BAG_URL = 'https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip'
    # BAG_VASTGOEDMAP = '\\\\cbsp.nl\\Productie\\primair\\WOVOR\\Beheer\\_Archief\\INPUT\\'

    logit.info(f'start functie get_main_bag_zip voor {maand}')


    # outputmap:
    dir_k0_maand = os.path.join(KOPPELVLAK0, str(maand))

    # zoek of download het inputbestand bag_zip_file 
    if os.path.exists(BAG_VASTGOEDMAP):
        bag_zip_file = maak_vastgoed_bestandsnaam(maand, logit)
        logit.info(f'{bag_zip_file} gevonden op het netwerk. Nu unzippen')
    else:
        logit.info(f'probeer het bag input bestand van maand {maand} te downloaden')
        bag_zip_file = baglib.download_file(BAG_URL)
        logit.info(f'downloaden van {bag_zip_file} gereed. Nu unzippen')
    

    with zipfile.ZipFile(bag_zip_file, 'r') as zip_ref:
        zip_ref.extractall(dir_k0_maand)
        logit.info('zojuist uitgepakt BAG bestand verwijderen')
        # os.remove(downloaded_file)
        logit.info('unzippen bestand gereed')
        # dir_k0_maand opnieuw lezen want nu staat er wel wat in de dir_k0_maand!

   
def maak_vastgoed_bestandsnaam(maand):
    '''unzip een bestand in de map \\cbsp.nl\Productie\primair\WOVOR\Beheer\_Archief\INPUT
    submap <jaar> van de vorm BAGNLDL-08MMYYYY.zip.'''
    _maand = str(maand)
    if len(_maand) != 6:
        sys.exit(f'yyyymm verwacht, maar kreeg {maand}')
    _jaar = _maand[:4]
    _zip_file = 'BAGNLDL-08' + _maand[-2:] + _jaar + '.zip'
    # return '\\\\cbsp.nl\\Productie\\primair\\WOVOR\\Beheer\\_Archief\\INPUT\\' +\
    #     _jaar + '\\' + _zip_file
    return BAG_VASTGOEDMAP + _jaar + '\\' + _zip_file


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


    bag_objecten_wplgem = BAG_OBJECTEN + ['wplgem']
    for maand in maand_lst:

        for bagobject in bag_objecten_wplgem:
            k0_unzip(bagobject, maand, logit)
