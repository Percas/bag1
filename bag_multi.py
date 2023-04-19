import time
from multiprocessing import cpu_count
from multiprocessing import Queue, Process
from k0_unzip import k0_unzip
from k1_xml import k1_xml
# from k2_fixvk import k2_fixvk
# from k3_hoofdpnd import k3_hoofdpnd
from config import BAG_OBJECTEN

koppelvlak_functie_dict = {'K0': k0_unzip,
                           'K1': k1_xml}
                           # 'K2': k2_fixvk,
                           # 'K2.5': k2_fixvk,
                           # 'K3': k3_hoofdpnd}

def main(maand):
    """hoofdfunctie voor de reguliere verwerking."""
                 
    nr_workers = cpu_count()
    bagobject_queue = Queue(nr_workers)
    koppelvlak_queue = Queue(1)
    
    koppelvlak_queue.put({'vbo': 'K0', 
                          'pnd': 'K0', 
                          'num': 'K0', 
                          'lig': 'K0',
                          'sta': 'K0',
                          'opr': 'K0',
                          'wpl': 'K0'})
    
    for bagobject in BAG_OBJECTEN:
        bagobject_queue.put(bagobject)

    workers = []

    for nr in range(nr_workers):
        p = Process(
            target=breng_bagobject_naar_volgend_koppelvlak,
            name=f"Worker-{nr}",
            args=(bagobject_queue, koppelvlak_queue),
        )
        workers.append(p)
        p.start()

def update_queues(bagobject_queue, koppelvlak_queue, bagobject):
    '''
    '''

    if not koppelvlak_queue.empty():
        koppelvlak_info = koppelvlak_queue.get()
        if koppelvlak_info[bagobject] == 'K0':
            koppelvlak_info[bagobject] = 'K1'
            koppelvlak_queue.put(koppelvlak_info)
            bagobject_queue.put(bagobject)
        if koppelvlak_info[bagobject] == 'K1':
            koppelvlak_info[bagobject] = 'K2'
            koppelvlak_queue.put(koppelvlak_info)
            bagobject_queue.put(bagobject)
        if koppelvlak_info[bagobject] == 'K2':
            koppelvlak_info[bagobject] = 'K3'
            koppelvlak_queue.put(koppelvlak_info)
            
            # bagobject_queue.put(bagobject)
        
    else:
        time.sleep(0.01)


def breng_bagobject_naar_volgend_koppelvlak(bagobject_queue, koppelvlak_queue):
    '''Haal bagobject uit bagobject_queue, kijk in welk koppelvlak dit 
    bagobject zit met de koppelvlak_queue. Breng dit bagobject vervolgens
    naar het volgende koppelvlak en administreer dit in de koppelvlak queue.
    Zet ten slotte hetzelfde  bagobject weer in de bagobject_queue, tenzij
    er eerst een ander bagobject in een hoger koppelvlak moet zitten.'''

    while True:
        
        if not bagobject_queue.empty() and not koppelvlak_queue.empty():
            koppelvlak_info = koppelvlak_queue.get()
            koppelvlak_queue.put(koppelvlak_info)
            bagobject = bagobject_queue.get()
            if bagobject == None:
                break # None in the queue signals to end the proces

            koppelvlak_functie = koppelvlak_functie_dict[koppelvlak_info[bagobject]]  

            success = koppelvlak_functie(maand, bagobject)

            if success:
                while update_queues(bagobject_queue, koppelvlak_queue, bagobject):
                    pass
            else:
                print('Bug:', koppelvlak_functie, 'failed')                
        else:
            time.sleep(1)

if __name__ == "__main__":
    maand = ['testdata02']
    main(maand)
