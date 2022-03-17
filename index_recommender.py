import pandas as pd
import re
from sql_metadata import Parser
from collections import defaultdict
from itertools import combinations
import psycopg2 as pg
from pglast import parser
from random import shuffle, randint, choice
import os.path
import pickle
import time

verbose = True


def vprint(*kwargs):
    if verbose:
        print(*kwargs)


CONNECTION_STRING = "dbname='project1db' user='project1user' password='project1pass' host='localhost'"
MAX_WIDTH = 3

"""
    Drops all the non-constrained indexes
    'borrowed' from https://stackoverflow.com/a/48862822
"""
def drop_all_indexes(conn):
    vprint("--- Dropping all existing indexes ---")
    get_drop_index_queries_query = """SELECT format('drop index %I.%I;', s.nspname, i.relname) as drop_statement
                FROM pg_index idx
                JOIN pg_class i on i.oid = idx.indexrelid
                JOIN pg_class t on t.oid = idx.indrelid
                JOIN pg_namespace s on i.relnamespace = s.oid
                WHERE s.nspname in ('public')
                AND not idx.indisprimary;"""
    with conn.cursor() as cur:
        cur.execute(get_drop_index_queries_query)
        drop_queries = cur.fetchall()
        for query in drop_queries:
            cur.execute(query[0])
        cur.execute(get_drop_index_queries_query)
        drop_queries = cur.fetchall()
    vprint("--- Indexes dropped ---")


def get_table_column_map(conn):
    QUERY = "select table_name, column_name from information_schema.columns where table_schema='public';"

    table_columns_map = defaultdict(set)
    with conn.cursor() as cursor:
        cursor.execute(QUERY)
        db_columns = cursor.fetchall()
        for table, column in db_columns:
            table_columns_map[table].add(column)
    return table_columns_map


def filter_interesting_queries(queries):
    res = [re.sub(r'^statement: ', '', q)
           for q in queries if q.startswith('statement')]
    res = [q for q in res if 'pg_' not in q and not q.startswith('SHOW ALL') and not q.startswith(
        'COMMIT') and not q.startswith('SET') and not q.startswith('BEGIN')]
    res = [q for q in res if 'WHERE' in q or 'ORDER' in q or 'JOIN' in q or 'join' in q or 'where' in q or 'order' in q]
    return res


def cluster_queries(queries):
    cluster_frequencies = defaultdict(int)
    clusters = {}
    gqueries = set()
    for query in queries:
        try:
            generalized = parser.fingerprint(query)
            cluster_frequencies[generalized] += 1
            clusters[generalized] = query
            gqueries.add(generalized)
        except:
            pass
    return gqueries, cluster_frequencies, clusters


def get_relevant_columns(clusters, cluster_counts, table_column_map, max_width):
    table_column_combos = defaultdict(set)
    for qgroup, query in clusters.items():
        try:
            pq = Parser(query)
            columns = pq.columns_dict
            pq_tables = pq.tables
            groups = ['where', 'order_by', 'join']
            for group in groups:
                if group in columns:
                    group_table_columns = defaultdict(set)
                    for col in columns[group]:
                        if '.' not in col:
                            for table in pq_tables:
                                if table in table_column_map and col in table_column_map[table]:
                                    col = table + '.' + col
                                    break
                        tab, c = col.split('.')
                        group_table_columns[tab].add(col)
                    for tab, cols in group_table_columns.items():
                        for width in range(1, min(len(cols)+1, max_width+1)):
                            for col_combo in combinations(cols, width):
                                table_column_combos[tab].add(col_combo)
        except:
            pass
    return table_column_combos


def get_potential_indexes(table_column_combos):
    potential_indexes = []
    for v in table_column_combos.values():
        potential_indexes.extend(v)
    return potential_indexes


def get_combinations_list(potential_indexes):
    combs = []
    for i in range(1, len(potential_indexes)+1):
        combs.extend(combinations(potential_indexes, i))
    return combs


def get_random_indexes(potential_indexes):
    num_indexes = randint(1, len(potential_indexes))
    indexes_set = set()
    while len(indexes_set) < num_indexes:
        indexes_set.add(choice(potential_indexes))
    return indexes_set


def get_columns_from_logs(logs_path, max_width=2):
    QCOL = 13
    df = pd.read_csv(logs_path, header=None)
    queries = filter_interesting_queries(df[QCOL].tolist())
    with pg.connect(CONNECTION_STRING) as conn:
        table_column_map = get_table_column_map(conn)
#         print(table_column_map)
    gqueries, cluster_frequencies, clusters = cluster_queries(queries)
    table_column_combos = get_relevant_columns(
        clusters, cluster_frequencies, table_column_map, max_width=MAX_WIDTH)
    return table_column_combos, cluster_frequencies, clusters


def generate_index_creation_queries(columns, conditional=False):
    conditional_str = ''
    if conditional:
        conditional_str = 'IF NOT EXISTS'
    query_template = 'CREATE INDEX ON {} ({})'
    queries = []

    for column_group in columns:

        table_name = column_group[0].split('.')[0]

        columns = [column.split('.')[1] for column in column_group]

        queries.append(query_template.format(table_name, ', '.join(columns)))

    return queries


def create_hypothetical_indexes(index_queries, conn):
    hypo_template = "SELECT * FROM hypopg_create_index('{}');"
    with conn.cursor() as cur:
        for index_creation_query in index_queries:
            cur.execute(hypo_template.format(index_creation_query))
            res = cur.fetchall()


def get_scaled_loss(cluster_frequencies, costs):
    cost = 0.
    for cluster, count in cluster_frequencies.items():
        cost += count*costs[cluster]
    return cost


def remove_hypo_indexes(conn):
    reset_indexes_q = 'SELECT * FROM hypopg_reset();'
    with conn.cursor() as cur:
        cur.execute(reset_indexes_q)
        res = cur.fetchall()


def enable_hypopg(conn):
    with conn.cursor() as cur:
        cur.execute('CREATE EXTENSION IF NOT EXISTS hypopg;')


def get_query_costs(query_clusters, conn):
    costs = dict()
    with conn.cursor() as cur:
        for cluster, query in query_clusters.items():
            cur.execute('EXPLAIN (FORMAT JSON) '+query)
            explain_res = cur.fetchall()
            costs[cluster] = explain_res[0][0][0]['Plan']['Total Cost']
    return costs


def find_best_index(log_file_path, timeout, max_iterations=100000):
    TIMEOUT_BUFFER = 20

    start_time = time.time()
    timeout = int(timeout.replace('m', ''))*60
    with open('config.json', 'w') as f:
        f.write('{"VACUUM": false}')

    with pg.connect(CONNECTION_STRING) as conn:

        state_file = log_file_path + '.statefile'

        vprint(f"--- State being maintained in {state_file} ---")

        if (os.path.isfile(state_file)):
            with open(state_file, 'rb') as f:
                saved_objects = pickle.load(f)
                best_config = saved_objects['best_config']
                min_cost = saved_objects['min_cost']
                baseline_cost = saved_objects['baseline_cost']
                table_column_combos = saved_objects['table_column_combos']
                cluster_frequencies = saved_objects['cluster_frequencies']
                clusters = saved_objects['clusters']
        else:
            table_column_combos, cluster_frequencies, clusters = get_columns_from_logs(
                log_file_path)

            # calculate the costs with no indexes
            baseline_costs = get_query_costs(clusters, conn)
            baseline_cost = get_scaled_loss(cluster_frequencies, baseline_costs)
            min_cost = baseline_cost

            best_config = []

        # drop all existing indexes
        drop_all_indexes(conn)

        # ensure that hypopg is enabled
        enable_hypopg(conn)

        potential_indexes = get_potential_indexes(table_column_combos)
        for i in range(max_iterations):

            # generate a random combination of indexes
            cmb = get_random_indexes(potential_indexes)
            index_q = generate_index_creation_queries(cmb)
            create_hypothetical_indexes(index_q, conn)

            costs = get_query_costs(clusters, conn)
            # make sure that the total cost is proportional to the cluster frequencies
            cost = get_scaled_loss(cluster_frequencies, costs)

            remove_hypo_indexes(conn)

            if cost < min_cost or (cost == min_cost and len(cmb) < len(best_config)):
                min_cost = cost
                best_config = cmb

            time_elapsed = time.time() - start_time
            if i % 1000 == 0 or ((timeout - time_elapsed) <= TIMEOUT_BUFFER):
                #                 print(best_config, min_cost, baseline_cost)
                index_creation_queries = generate_index_creation_queries(
                    best_config)
                with open('actions.sql', 'w') as f:
                    for query in index_creation_queries:
                        f.write("{};\n".format(query))
                with open(state_file, 'wb') as f:
                    saved_objects = pickle.dump({
                        'best_config': best_config,
                        'min_cost': min_cost,
                        'baseline_cost': baseline_cost,
                        'table_column_combos': table_column_combos,
                        'cluster_frequencies': cluster_frequencies,
                        'clusters': clusters
                    }, f)
                if (timeout - time_elapsed) <= TIMEOUT_BUFFER:
                    vprint(f'--- {time_elapsed}s have elapsed, within {TIMEOUT_BUFFER}s of the timeout {timeout}s. Exiting ---')
                    return

        index_creation_queries = generate_index_creation_queries(best_config)
        vprint('--- Best Indexes ---')
        vprint(index_creation_queries)
        vprint('--- Best Indexes END ---')
        with open('actions.sql', 'w') as f:
            for query in index_creation_queries:

                f.write("{};\n".format(query))
        with open(state_file, 'wb') as f:
            saved_objects = pickle.dump({
                'best_config': best_config,
                'min_cost': min_cost,
                'baseline_cost': baseline_cost,
                'table_column_combos': table_column_combos,
                'cluster_frequencies': cluster_frequencies,
                'clusters': clusters
            }, f)
