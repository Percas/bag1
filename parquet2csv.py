#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1 april 2023, Anton
version: 0.2
doel: parquet bestand converteren naar csv bestand
"""

# ################ import libraries ###############################
import sys
import os
import baglib
import pandas as pd
import time
# from config import OMGEVING, LOGFILE
import logging

# ############### Define functions ################################

# Main function for this package:

def parquet2csv(inputfiles = '', logit=logging.DEBUG):
    '''Converteert inputfile.parquet naar outputfile.csv.''' 

    tic = time.perf_counter()
    logging.debug('start functie parquet2csv')    
    for inputfile in inputfiles:
        inputfile_ext = inputfile + '.parquet'
        if not os.path.exists(inputfile_ext): 
            sys.exit('Fout: bestand of map ' + str(inputfile_ext) + ' bestaat niet')
        # bestand = os.path.basename(inputfile)
        logit.debug(f'te converteren bestand: {inputfile_ext}')
        df = pd.read_parquet(inputfile_ext)
        outputfile = inputfile + '.csv'
        logit.debug(f'opslaan als {outputfile}')
        df.to_csv(outputfile, index=False)
    toc = time.perf_counter()
    logit.debug(f'einde parquet2csv in {(toc - tic)/60} min')



# ########################################################################
# ########################################################################

if __name__ == '__main__':

    

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            # logging.FileHandler(LOGFILE),
            logging.StreamHandler()])    
    logit = logging.getLogger()
    logit.info('start parquet2csv vanuit main')
    # logit.warning(OMGEVING)
    logit.setLevel(logging.INFO)
    
    inputfiles = sys.argv[1:]
    parquet2csv(inputfiles=inputfiles, logit=logit)
