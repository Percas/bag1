#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
6 mei 2023, Anton
version: 0.1
doel: multiprocessen van bag bestanden die onafhankelijk van elkaar gedraaid 
kunnen worden en als argument alleen de extractmaand (en logger) hebben.

"""

# ################ import libraries ###############################
import time
# from multiprocessing import cpu_count
from multiprocessing import Queue, Process # , JoinableQueue
# import multiprocessing
from k0_unzip import k0_unzip
from k1_xml import k1_xml
# from k2_fixvk import k2_fixvk
from config import KOPPELVLAK0, BAG_OBJECTEN, LOGFILE, NR_WORKERS
import baglib
import sys
import logging


bag_functie_dict = {
    'k0_unzip': k0_unzip,
    'k1_xml': k1_xml}


def bag_do_maand_lst(bag_functie, maand_lst, logit):
    """gebruik multiprocessing om de functie bag_functie voor elke maand in 
    maand_list aan te roepen."""

    tic = time.perf_counter()
    logit.info(f'start bag_do_maand_list voor maanden {maand_lst}')
    bag_objecten = BAG_OBJECTEN + ['wplgem']
    
    aantal_werkjes = len(maand_lst) * len(bag_objecten)
    aantal_werkers = min(aantal_werkjes, NR_WORKERS)

    werk_queue = Queue(2 * aantal_werkjes)
    for maand in maand_lst:
        for bagobject in bag_objecten:
            werk_queue.put((bag_functie, bagobject, maand, logit))
    for _ in range(aantal_werkjes):
        werk_queue.put(None)

    logit.warning(f'start {aantal_werkers} werkers voor {aantal_werkjes} werkjes')

    workers = []
    for nr in range(aantal_werkers):
        p = Process(
            target=do_maand,
            name=f"Worker-{nr}",
            args=(werk_queue,)
        )
    
        workers.append(p)
        p.start()

    for p in workers:
        p.join()

    toc = time.perf_counter()
    logit.warning(f'einde bag_do_maand_list in {(toc - tic)/60} min')


def do_maand(werk_queue):
    '''In de werk_queue staat een tuple van de aan te roepen 
    functie en de twee argumenten maand en logger. 
    Bijvoorbeeld: (k0_unzip, 202304, logit).'''
    # worker_name = multiprocessing.current_process().name
    
    while True:

        werkje = werk_queue.get()
        
        if werkje == None:
            break
        
        else:
            bag_functie, bagobject, maand, logit = werkje
            logit.debug(f'{bag_functie.__name__}({bagobject}, {maand})')

            bag_functie(bagobject, maand, logit)



if __name__ == "__main__":
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOGFILE),
            logging.StreamHandler()])    

    logit = logging.getLogger()
    logit.setLevel(logging.INFO)
    logit.info('start bag_k02 vanuit main')

    args =\
        baglib.get_args2(sys.argv, 
                         arg1_in=bag_functie_dict.keys(),
                         args2_in=KOPPELVLAK0)
    bag_functie_naam = args[0]
    bag_functie = bag_functie_dict[bag_functie_naam]
    maand_lst = args[1:]
    # print('arg1:', bag_functie)
    # print('args2:', maand_lst)
    
    bag_do_maand_lst(bag_functie, maand_lst, logit)