#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1 april 2023, Anton
version: 0.1
doel: parquet bestand converteren naar csv bestand
"""

# ################ import libraries ###############################
import sys
import os
import baglib
import pandas as pd
import time
from config import OMGEVING

# ############### Define functions ################################

# Main function for this package:

def parquet2csv(inputfiles = '', loglevel=20):
    '''Converteert inputfile.parquet naar outputfile.csv.''' 

    tic = time.perf_counter()
    _ll = loglevel
    baglib.printkop(_ll+40, 'Start parquet2csv')
    
    for inputfile in inputfiles:
        inputfile_ext = inputfile + '.parquet'
        if not os.path.exists(inputfile_ext): 
            sys.exit('Fout: bestand of map ' + str(inputfile_ext) + ' bestaat niet')
        # bestand = os.path.basename(inputfile)
        baglib.aprint(ll+40, 'Te converteren bestand:', inputfile_ext)
        df = pd.read_parquet(inputfile_ext)
        outputfile = inputfile + '.csv'
        baglib.aprint(ll+20, 'Opslaan als', outputfile)
        df.to_csv(outputfile, index=False)
    toc = time.perf_counter()
    baglib.aprint(_ll+40, '\n*** Einde parquet2csv in', (toc - tic)/60, 'min ***\n')



# ########################################################################
# ########################################################################

if __name__ == '__main__':

    ll = 20
    baglib.printkop(ll+40, OMGEVING)
    inputfiles = sys.argv[1:]
    baglib.printkop(ll+30, 'Lokale aanroep')
    parquet2csv(inputfiles=inputfiles, loglevel=ll)
