#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
6 mei 2023, Anton
version: 0.2
doel: multiprocessen van bag bestanden die onafhankelijk van elkaar gedraaid 
kunnen worden en als argument de extractmaand (en logger) hebben.
In bag_functie_dict staan de functies die als argumenten kunnen worden opgegeven.
"""

# ################ import libraries ###############################
import time
# from multiprocessing import cpu_count
from multiprocessing import Queue, Process # , JoinableQueue
# import multiprocessing
from k0_unzip import k0_unzip
from k0_unzip import k0_unzip_vastgoed_bestand
from k1_xml import k1_xml
from k2_fixvk import k2_fixvk
from config import KOPPELVLAK0, BAG_OBJECTEN, LOGFILE, NR_WORKERS
import baglib
import sys
import logging

bag_functie_dict = {
    'k0_unzip_vastgoed_bestand': k0_unzip_vastgoed_bestand,
    'k0_unzip': k0_unzip,
    'k1_xml': k1_xml,
    'k2_fixvk': k2_fixvk}


def do_bag_functie_maand_lst_parallel(bag_functie_naam, maand_lst, logit):
    """gebruik multiprocessing om de functie bag_functie voor elke maand in 
    maand_list aan te roepen. In de koppelvlakken k0 en k1 kunnen ook de
    bagobjecten nog parallel. Daarna niet meer"""


    tic = time.perf_counter()
    logit.info(f'start bag_do_maand_list voor maanden {maand_lst}')

    cando_bagobject_in_parallel = {
        'k0_unzip': True,
        'k1_xml': True,
        'k2_fixvk': False}

    bag_functie = bag_functie_dict[bag_functie_naam]

    if cando_bagobject_in_parallel[bag_functie_naam]:
        
        bag_objecten = BAG_OBJECTEN + ['wplgem']
        aantal_werkjes = len(maand_lst) * len(bag_objecten)
        aantal_werkers = min(aantal_werkjes, NR_WORKERS)
    
        werk_queue = Queue(2 * aantal_werkjes)
        for maand in maand_lst:
            for bagobject in bag_objecten:
                werk_queue.put((bag_functie, bagobject, maand, logit))
        for _ in range(aantal_werkjes):
            werk_queue.put(None)

    else: # can only do months in parallel, not bagobject
        
        aantal_werkjes = len(maand_lst)
        aantal_werkers = min(aantal_werkjes, NR_WORKERS)
    
        werk_queue = Queue(2 * aantal_werkjes)
        for maand in maand_lst:
            werk_queue.put((bag_functie, maand, logit))
        for _ in range(aantal_werkjes):
            werk_queue.put(None)
    
    logit.warning(f'start {aantal_werkers} werkers voor {aantal_werkjes} werkjes')
    
    workers = []
    for nr in range(aantal_werkers):
        p = Process(
            target=do_parallel,
            name=f"Worker-{nr}",
            args=(werk_queue,)
        )
    
        workers.append(p)
        p.start()

    for p in workers:
        p.join()

    toc = time.perf_counter()
    logit.warning(f'einde bag_do_maand_list in {(toc - tic)/60} min')


def do_parallel(werk_queue):
    '''In de werk_queue staat een tuple van de aan te roepen 
    functie en de argumenten van deze functie: dit is ofwel:
        bagobject, maand en logger. ofwel
        maand en logger
    Bijvoorbeeld: (k0_unzip, 'vbo', '202304', logit) of
                  (k2_fixvk, '202304', logit)
    Deze "werkjes"  kunnen onafhankelijk van elkaar uitgevoerd worden'''
    # worker_name = multiprocessing.current_process().name
    
    while True:

        werkje = werk_queue.get()
        
        if werkje == None:
            break
        else:
            if len(werkje) == 4:
                bag_functie, bagobject, maand, logit = werkje
                logit.debug(f'{bag_functie.__name__}({bagobject}, {maand})')
                bag_functie(bagobject, maand, logit)
            else: # len(werkje) == 3
                bag_functie, maand, logit = werkje
                logit.debug(f'{bag_functie.__name__}({maand})')
                bag_functie(maand, logit)
                


if __name__ == "__main__":
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOGFILE),
            logging.StreamHandler()])    

    logit = logging.getLogger()
    logit.setLevel(logging.INFO)
    logit.info('start do_bag_functie_maand_lst_parallel vanuit main')

    args =\
        baglib.get_args2(sys.argv, 
                         arg1_in=bag_functie_dict.keys(),
                         args2_in=KOPPELVLAK0)
    bag_functie_naam = args[0]
    maand_lst = args[1:]
    # print('arg1:', bag_functie)
    # print('args2:', maand_lst)
    
    do_bag_functie_maand_lst_parallel(bag_functie_naam, maand_lst, logit)