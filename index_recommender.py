import pandas as pd
import os
import re
import json
from sql_metadata import Parser
from collections import defaultdict
from itertools import combinations


def filter_interesting_queries(queries):
    res = [re.sub(r'^statement: ', '', q) for q in queries if q.startswith('statement')]
    res = [q for q in res if 'pg_' not in q and not q.startswith('SHOW ALL') and not q.startswith('COMMIT') and not q.startswith('SET') and not q.startswith('BEGIN')]
    res = [q for q in res if 'WHERE' in q or 'ORDER' in q or 'JOIN' in q or 'join' in q]
    return res
def cluster_queries(queries):
    group_counts = defaultdict(int)
    group_repr = {}
    gqueries = set()
    for query in queries:
        try:
            generalized = Parser(query).generalize
            group_counts[generalized] += 1
            group_repr[generalized] = query
            gqueries.add(generalized)
        except:
            pass
    return gqueries, group_counts, group_repr
def get_relevant_columns(clusters, cluster_counts):
    column_usage_counts = defaultdict(int)
    tables = []
    table_columns = defaultdict(set)

    for qgroup, query in clusters.items():
        try:
            pq = Parser(query)
            tables.extend(pq.tables)
            columns = pq.columns_dict
            groups = ['where', 'order_by', 'join']
            for group in groups:
                if group in columns:
                    for col in columns[group]:
                        if '.' in col:
                            column_usage_counts[col] += cluster_counts[qgroup]
                            tab, c = col.split('.')
                            table_columns[tab].add(c)
        except:
            pass
    tables = set(tables)
#     print(table_columns)
    ordered_columns = list(reversed(sorted(list(column_usage_counts.items()), key=lambda x:x[1])))
#     print(ordered_columns)
    return ordered_columns

def get_combinations_list(ordered_columns):
    combs = []
    for i in range(1, len(ordered_columns)+1):
        combs.extend(combinations(ordered_columns, i))
    return combs
def get_columns_from_logs(logs_path):
    QCOL = 13
    df = pd.read_csv(logs_path, header=None)
    queries = filter_interesting_queries(df[QCOL].tolist()); queries
    gqueries, group_counts, group_repr = cluster_queries(queries)
    cols = get_relevant_columns(group_repr, group_counts)
    return cols, group_counts, group_repr
def index_creation_queries(columns):
    query_template = 'CREATE INDEX ON {} ({})'
    queries = []
    for column in columns:
        tab, col = column[0].split('.')
        queries.append(query_template.format(tab, col))
    return queries
def create_hypothetical_indexes(index_queries, conn):
    hypo_template = "SELECT * FROM hypopg_create_index('{}');";
    with conn.cursor() as cur:
        for index_creation_query in index_queries:
            cur.execute(hypo_template.format(index_creation_query))
            res = cur.fetchall()   
#             print(res)
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
