#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
9 juni, Anton

doel: uitpakken van de gezipte BAG XML bestanden

todo: mapjes aanmaken als ze nog niet bestaan

"""

# ################ import libraries ###############################
import sys
import os
import baglib
import zipfile

# ############### Define functions ################################

# #############################################################################
# print('00.............Initializing variables...............................')
# #############################################################################
os.chdir('..')
BASEDIR = os.getcwd() + '/'
baglib.print_omgeving(BASEDIR)
DATADIR = BASEDIR + 'data/'
DIR00 = DATADIR + '00-zip/'
DIR01 = DATADIR + '01-xml/'
current_month = baglib.get_arg1(sys.argv, DIR00)
INPUTDIR = DIR00 + current_month + '/'
OUTPUTDIR = DIR01 + current_month + '/'

# if not os.path.exists(INPUTDIR):
#     sys.exit('Error: map niet gevonden:' + str(INPUTDIR))
# if not os.path.exists(OUTPUTDIR):
#     print('Aanmaken outputmap', OUTPUTDIR)
#     os.makedirs(OUTPUTDIR)

unzip_files = os.listdir(INPUTDIR)
bagobj_starts_with = {'vbo': '9999VBO',
                      'lig': '9999LIG',
                      'sta': '9999STA',
                      'pnd': '9999PND',
                      'num': '9999NUM',
                      'opr': '9999OPR',
                      'wpl': '9999WPL',
                      'gemwpl': 'GEM-WPL-RELATIE'}

for bagobj in bagobj_starts_with.keys():
    for unzip_file_name in unzip_files:
        if unzip_file_name.startswith(bagobj_starts_with[bagobj]):
            unzip_dir = OUTPUTDIR + bagobj + '/'
            unzip_file = INPUTDIR + unzip_file_name
            if not os.path.exists(unzip_dir):
                print('\nAanmaken outputmap', unzip_dir)
                os.makedirs(unzip_dir)
            print('\nUitpakken van bestand', unzip_file_name,
                  '\nin directory', INPUTDIR,
                  '\nnaar directory', unzip_dir, '...')
            with zipfile.ZipFile(unzip_file, 'r') as zip_ref:
                zip_ref.extractall(unzip_dir)

