import lstore.config as config
import matplotlib.pyplot as plt
import pandas as pd

from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

def benchmark(num_records: int, enable_indices=False, page_size = 4096, bufferpool_evict = 1 * 1024 * 1024 * 1024):
    
    #adjust configuration variables
    config.PAGE_SIZE = page_size
    config.BUFFERPOOL_MAX_FRAMES = bufferpool_evict // config.PAGE_SIZE
    
    
    results = {}
    results["record_size"] = num_records
    results["buffer_size"] = config.BUFFERPOOL_MAX_FRAMES
    results["page_size"] = config.PAGE_SIZE
    
    # Student Id and 4 grades
    db = Database()
    grades_table = db.create_table("Grades", 5, 0)
    query = Query(grades_table)
    keys = []

    insert_time_0 = process_time()
    for i in range(0, num_records):
        query.insert(906659671 + i, 93, 0, 0, 0)
        keys.append(906659671 + i)
    insert_time_1 = process_time()
    results["insert"] = round(insert_time_1 - insert_time_0, 4)

    # Measuring update Performance
    update_cols = [
        [None, None, None, None, None],
        [None, randrange(0, 100), None, None, None],
        [None, None, randrange(0, 100), None, None],
        [None, None, None, randrange(0, 100), None],
        [None, None, None, None, randrange(0, 100)],
    ]

    update_time_0 = process_time()
    for i in range(0, num_records):
        query.update(choice(keys), *(choice(update_cols)))
    update_time_1 = process_time()
    results["update"] = round(update_time_1 - update_time_0, 4)

    # Measuring Select Performance
    select_time_0 = process_time()
    for i in range(0, num_records):
        query.select(choice(keys), 0, [1, 1, 1, 1, 1])
    select_time_1 = process_time()
    results["select"] = round(select_time_1 - select_time_0, 4)

    # Measuring Aggregate Performance
    if enable_indices:
        for i in range(1, 5):
            grades_table.index.create_index(i)
    agg_time_0 = process_time()
    for i in range(0, num_records, int(num_records / 100)):
        start_value = 906659671 + i
        end_value = start_value + 100
        query.sum(start_value, end_value - 1, randrange(0, 5))
    agg_time_1 = process_time()
    results["agg"] = round(agg_time_1 - agg_time_0, 4)

    # Measuring Delete Performance
    delete_time_0 = process_time()
    for i in range(0, num_records):
        query.delete(906659671 + i)
    delete_time_1 = process_time()
    results["delete"] = round(delete_time_1 - delete_time_0, 4)

    return results

def plot(dictionary_list = list, metric = str):
    
    combined_data = {key: [d[key] for d in dictionary_list] for key in dictionary_list[0]}
    arrays = {key: [combined_data[key][i] for i in range(len(dictionary_list))] for key in combined_data}
    
    data = {'number_of_records': arrays['record_size'],
            'page_size': arrays['page_size'],
            'buffer_size': arrays['buffer_size'],
            'insert': arrays['insert'],
            'update': arrays['update'],
            'select': arrays['select'],
            'agg': arrays['agg'],
            'delete': arrays['delete']}

    df = pd.DataFrame(data)
    
    
    for i in ['insert', 'update', 'select', 'agg', 'delete']:
        
        #create plot
        plt.figure()
        
        for category, group in df.groupby(metric):
                plt.plot(group['number_of_records'], group[i], label=f'{metric} {category}')
        
        plt.xlabel('Number of Records')
        plt.ylabel(f'{i} Time')
        plt.title(f'{i} Time vs. Number of Records')
        plt.legend()
    
        plt.grid(True)
        plt.show()   

#for general benchmarking
for i in [1_000, 10_000, 100_000, 1_000_000]:
    print(f"{i: 8} records w/o indices: {benchmark(i)}")
    print(f"{i: 8} records w/  indices: {benchmark(i, True)}")

#graphs for different page size
page_sizes = []
for i in [1_000, 10_000, 100_000, 1_000_000]:
        for j in [4096/2, 4096, 4096*2]:
                page_sizes.append(benchmark(num_records = i))
        
plot(page_sizes, metric = 'page_size')
