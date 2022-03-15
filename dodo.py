def task_project1():
    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Faking action generation."',
            'echo "SELECT 1;" > actions.sql',
            'echo "SELECT 2;" >> actions.sql',
            'echo \'{"VACUUM": true}\' > config.json',
        ],
        # Always rerun this task.
        "uptodate": [False],
        "params": [
            {
                'name': 'timeout',
                'default': '1m',
                'short': 't',
                'long': 'timeout'
            },{
                'name': 'workload_csv',
                'default': './test.csv',
                'short': 'w',
                'long': 'workload_csv'
            }
        ]
    }


def task_project1_setup():
    return {
        "actions": [
            'pip install -r requirements.txt',
            ''
        ]
    }
