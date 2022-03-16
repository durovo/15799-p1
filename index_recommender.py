import pandas as pd
import re
from sql_metadata import Parser
from collections import defaultdict
from itertools import combinations
import psycopg2 as pg
from pglast import parser
from random import shuffle, randint, choice

CONNECTION_STRING = "dbname='project1db' user='project1user' password='project1pass' host='localhost'"
MAX_WIDTH = 3


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
    group_counts = defaultdict(int)
    group_repr = {}
    gqueries = set()
    for query in queries:
        try:
            generalized = parser.fingerprint(query)
            group_counts[generalized] += 1
            group_repr[generalized] = query
            gqueries.add(generalized)
        except:
            pass
    return gqueries, group_counts, group_repr


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
    gqueries, group_counts, group_repr = cluster_queries(queries)
    table_column_combos = get_relevant_columns(
        group_repr, group_counts, table_column_map, max_width=MAX_WIDTH)
    return table_column_combos, group_counts, group_repr


def generate_index_creation_queries(columns):
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


def get_scaled_loss(group_counts, costs):
    cost = 0.
    for cluster, count in group_counts.items():
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


def find_best_index(log_file_path, max_iterations=3000):
    table_column_combos, group_counts, group_repr = get_columns_from_logs(
        log_file_path)
    best_config = []
    with open('config.json', 'w') as f:
        f.write('{"VACUUM": false}')
    with pg.connect(CONNECTION_STRING) as conn:
        baseline_costs = get_query_costs(group_repr, conn)
        baseline_cost = get_scaled_loss(group_counts, baseline_costs)
        min_cost = baseline_cost
        enable_hypopg(conn)
        potential_indexes = get_potential_indexes(table_column_combos)
        for i in range(max_iterations):
            cmb = get_random_indexes(potential_indexes)
            index_q = generate_index_creation_queries(cmb)
            create_hypothetical_indexes(index_q, conn)
            costs = get_query_costs(group_repr, conn)
            cost = get_scaled_loss(group_counts, costs)
            remove_hypo_indexes(conn)
            if cost < min_cost or (cost == min_cost and len(cmb) < len(best_config)):
                min_cost = cost
                best_config = cmb

            if i % 1000 == 0:
                #                 print(best_config, min_cost, baseline_cost)
                with open('actions.sql', 'w') as f:
                    for query in index_q:
                        f.write("{};\n".format(query))

    index_creation_queries = generate_index_creation_queries(best_config)
    with open('actions.sql', 'w') as f:
        for query in index_creation_queries:

            f.write("{};\n".format(query))
