import requests
import json
import sys
import re
from datetime import datetime,timedelta
import logging
from decouple import config
import lib.es

CT_URL = config('bamboo_ct_url', default='https://api.clicktime.com/v2/')
CT_TOKEN = config('bamboo_ct_token_secret')
CT_PROJECTS=[
    20184,  # TaskFlow H2-2020
    20230,  # DXG TaskFlow for WK DE Panta Rhei
    20263,  # TaskFlow for UTD Pathways Reviewers Site - 2020
    19454,  # GPO TaskFlow for Emmi IVR - FPR - 2020
    19455,  # GPO TaskFlow for UTD Pathways - 2020
    19456,  # GPO TaskFlow for UTD Project Spectrum - 2020
    20182,  # Bamboo H2-2020
    19414,  # AnswerConnect Product Development 2020
    19415,  # AnswerConnect Canada Product Development 2020
    20148,  # CPM Interactive Dashboard Components Pilot
    13197,  # OSA Maintenance
    20197,  # OSA H2-2020
    20183,  # DevOps Tooling H2-2020
    20186,  # Embassy H2-2020
    20018,  # UX - Kleos Browser Design 2020
    11108,  # GPO - Non-Project Activities
    20045,  # UX - CSO Europe - Basecone Application Redesign
    19390,  # WKNL Navigator 2020 
    ]
CT_ES_INDEX_NAME = 'tf-dwh-clicktime'
cluster_arn = ""
secret_arn = ""
cache_jobs = {}
cache_clients = {}
cache_tasks = {}
cache_users = {}

def create_index_if_not_exist():
    if not es.indices.exists_alias(CT_ES_INDEX_NAME):
        es.indices.create(CT_ES_INDEX_NAME + '-000001', body={
            "aliases": {
                CT_ES_INDEX_NAME: {}
            }
        })

def main(argv):
    create_index_if_not_exist()
    project = argv[0] if len(argv) > 0 else None
    if project:
        handle_project(project)
    else:
        for p in CT_PROJECTS:
            handle_project(p)

def handle_project(id):
    print('Handle Project ' +id)
    job = get_job_by_number(id)
    existing_records = get_existing_records(id)

    is_last_page = False
    start = 0
    while not is_last_page:
        resp = requests.get(CT_URL + 'TimeEntries',
                            params={'jobid': job['id'], 'offset': start, 'limit': 500 
                                    },
                            headers={'Authorization': 'Token ' + CT_TOKEN})
        content = json.loads(resp.text)
        start = int(content['page']['offset']) + int(content['page']['limit'])
        is_last_page = True if start >= int(content['page']['count']) else False

        for entry in content['data']:
            doc={
            "id" = entry['ID']
            "comment" = str(entry.get('Comment').encode('ascii', 'ignore')).replace('b\'', "").replace('\'', "")
            "date" = datetime.strptime(entry['Date'], '%Y-%m-%d').date()
            "reported_hours" = float(entry['Hours'])
            "job_number" = job['number']
            "job_name" = job['name']
            "client" = get_client(job['client_id'])
            "client_name" = client['name']
            "task" = get_task(entry['TaskID'])
            "task_code" = task['code']
            "task_name" = task['name']
            "user" = get_user(entry['UserID'])
            "employee_name" = user['name']
            "employee_email" = user['email']
            }
            es.index(index=CT_ES_INDEX_NAME, id="{0}-{1}-{2}".format(entry['ID'], entry['UserID'], entry['TaskID']), body=doc)
            existing_records = find_and_remove_element(existing_records, str(doc["date"]), doc["task_code"], doc["employee_name"])

    print('Removing {0} items from DB for JOB {1}'.format(len(existing_records), job['number']))
    delete_records([i['id'] for i in existing_records])

def find_and_remove_element(records, date, task_code, user):
    return [r for r in records
            if (r['date'] != date or
                r['task_code'] != task_code or
                r['employee_name'] != user)]

def get_existing_records(job_number):
    try:
        res = es.search(index=CT_ES_INDEX_NAME, body={
            "sort": [{"timestamp": {"order": "desc"}}],
            "query": {
                "bool": {
                  "must": [
                    {"match_phrase": {"job_number": job_number}}
                  ]
                }
            }
        })
        if len(res['hits']['hits']) > 0:
            result = datetime.strptime(res['hits']['hits'][0]['_source']['timestamp'], '%Y-%m-%dT%H:%M:%S') \
                     - timedelta(days=1)
        else:
            result = datetime.now() - timedelta(days=2000)
    except Exception as e:
        print(e)
        result = datetime.now() - timedelta(days=2000)

    return result

def get_job_by_number(number):
    if number in cache_jobs:
        return cache_jobs[number]
    else:

        resp = requests.get(CT_URL + 'Jobs',
                            params={'JobNumber': number, 'limit': 1},
                            headers={'Authorization': 'Token ' + CT_TOKEN})
        content = json.loads(resp.text)
        cache_jobs[number] = {}
        cache_jobs[number]['id'] = content['data'][0]['ID']
        cache_jobs[number]['number'] = number
        cache_jobs[number]['name'] = content['data'][0]['Name']
        cache_jobs[number]['client_id'] = content['data'][0]['ClientID']
        return cache_jobs[number]

#if exists in es but not in clicktime, need to delete from es
def delete_records(records_id):
    for records in records_to_delete:
        try:
        es.delete(index=CT_ES_INDEX_NAME, body={
            "sort": [{"timestamp": {"order": "desc"}}],
            "query": {
                "bool": {
                  "must": [
                    {"match_phrase": {"id": records_id}}
                  ]
                }
            }
        })

def get_client(id):
    if id in cache_clients:
        return cache_clients[id]
    else:
        resp = requests.get(CT_URL + 'Clients/' + id,
                            headers={'Authorization': 'Token ' + CT_TOKEN})
        content = json.loads(resp.text)
        cache_clients[id] = {}
        cache_clients[id]['name'] = content['data']['Name']
        return cache_clients[id]


def get_task(id):
    if id in cache_tasks:
        return cache_tasks[id]
    else:
        resp = requests.get(CT_URL + 'Tasks/' + id,
                            headers={'Authorization': 'Token ' + CT_TOKEN})
        content = json.loads(resp.text)
        cache_tasks[id] = {}
        cache_tasks[id]['code'] = content['data'].get('TaskCode')
        cache_tasks[id]['name'] = content['data']['Name']
        return cache_tasks[id]


def get_user(id):
    if id in cache_users:
        return cache_users[id]
    else:
        resp = requests.get(CT_URL + 'Users/' + id,
                            headers={'Authorization': 'Token ' + CT_TOKEN})
        content = json.loads(resp.text)
        cache_users[id] = {}
        cache_users[id]['name'] = content['data']['Name'].replace(' ', '.')
        cache_users[id]['email'] = content['data']['Email']
        return cache_users[id]


def get_time_entry(id):
    resp = requests.get(CT_URL + 'TimeEntries/' + id,
                        headers={'Authorization': 'Token ' + CT_TOKEN})
    content = json.loads(resp.text)
    return content


print('ClickTime pipeline', CT_TOKEN)
print('Elasticsearc Info', es.es.info())
if __name__ == "__main__":
    main(sys.argv[1:])