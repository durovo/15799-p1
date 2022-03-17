from index_recommender import find_best_index
def task_project1():
    def generate_actions(workload_csv, timeout):
        find_best_index(workload_csv, timeout)
    return {        
        # A list of actions. This can be bash or Python callables.
        "actions": [generate_actions],
        # Always rerun this task.
        "uptodate": [False],
        "params": [
            {
                'name': 'workload_csv',
                'default': './test.csv',
                'long': 'workload_csv'
            },
            {
                'name': 'timeout',
                'default': '1m',
                'long': 'timeout'
            }
        ]
    }


def task_project1_setup():
    return {
        "actions": [
            'sudo apt update',
            'pip install -r requirements.txt',
            'sudo apt install postgresql-14-hypopg'
        ]
    }
