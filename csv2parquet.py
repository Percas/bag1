#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1 april 2023, Anton
version: 0.1
doel: csv bestanden converteren naar parquet bestanden
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

def csv2parquet(inputfiles = '', loglevel=20):
    '''Converteert inputfile.parquet naar outputfile.csv.''' 

    tic = time.perf_counter()
    _ll = loglevel
    baglib.printkop(_ll+40, 'Start csv2parquet')
    
    for inputfile in inputfiles:
        if not os.path.exists(inputfile): 
            inputfile = inputfile + '.csv'
            if not os.path.exists(inputfile): 
                sys.exit('Fout: bestand of map ' + str(inputfile) + ' bestaat niet')
        baglib.aprint(ll+40, 'Te converteren bestand:', inputfile)
        df = pd.read_csv(inputfile)
        outputfile = os.path.splitext(inputfile)[0] + '.parquet'
        baglib.aprint(ll+20, 'Opslaan als', outputfile)
        df.to_parquet(outputfile, index=False)
    toc = time.perf_counter()
    baglib.aprint(_ll+40, '\n*** Einde csv2parquet in', (toc - tic)/60, 'min ***\n')



# ########################################################################
# ########################################################################

if __name__ == '__main__':

    ll = 20
    baglib.printkop(ll+40, OMGEVING)
    inputfiles = sys.argv[1:]
    baglib.printkop(ll+30, 'Lokale aanroep')
    csv2parquet(inputfiles=inputfiles, loglevel=ll)
