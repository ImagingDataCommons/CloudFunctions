#
# Copyright 2018 Google LLC
# Copyright 2020, 2021 Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import json
import os
import time
import datetime

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from google.cloud import storage
from google.cloud import bigquery


def control_billing(data, context):
    pubsub_data = base64.b64decode(data['data']).decode('utf-8')
    pubsub_json = json.loads(pubsub_data)
    cost_amount = pubsub_json['costAmount']
    budget_amount = pubsub_json['budgetAmount']
    budget_name = pubsub_json["budgetDisplayName"]
    project_id = budget_name.replace('-budget', '')
    cis = pubsub_json["costIntervalStart"]
    print('Project {} cost this month: {} reports at {}: start: {} '.format(project_id, str(pubsub_json["costAmount"]),
                                                                            context.timestamp, str(cis)))

    STATE_BUCKET = os.environ["STATE_BUCKET"]
    PULL_MULTIPLIER = float(os.environ["PULL_MULTIPLIER"])
    COST_BQ = os.environ["COST_BQ"]
    EGRESS_NOTIFY_MULTIPLIER = float(os.environ["EGRESS_NOTIFY_MULTIPLIER"])
    MAX_CPUS = int(os.environ["MAX_CPUS"])
    message_root_fmt = "EXTERNAL BUDGET ALERT {}"

    project_name = 'projects/{}'.format(project_id)
    state_blob = "{}_state.json".format(project_id)
    week_num = datetime.datetime.now().strftime("%V")

    #
    # We get this pubsub message about every 20 minutes all month long. We don't really care
    # about the monthly billing here, we care about the cumulative amount over several months,
    # which is what will cause the shutdown.
    #

    #
    # Get the state for the budget:
    #

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(STATE_BUCKET)

    blob = bucket.blob(state_blob)
    blob_str = blob.download_as_string() if blob.exists() else b''
    raw_budget_state = {"messages": {}, "amounts": {cis: 0}} if (blob_str == b'') else json.loads(blob_str)
    if "messages" not in raw_budget_state:
        budget_super_state = {"messages": {}, "amounts": raw_budget_state}
    else:
        budget_super_state = raw_budget_state
    budget_state = budget_super_state['amounts']
    budget_state[cis] = cost_amount
    message_state = budget_super_state['messages']

    #
    # How much is being spent to move data around:
    #

    total_egress = float(_calc_egress(COST_BQ, project_id))
    print("Project {} total egress and download: {}".format(project_id, total_egress))
    egress_thresh = EGRESS_NOTIFY_MULTIPLIER * float(budget_amount)
    if total_egress > egress_thresh:
        message_key = message_root_fmt.format("1")
        fire_off = (message_key not in message_state) or message_state[message_key] != week_num
        if fire_off:
            print("{}: Project {} total egress and download ({}) exceeds threshold {} ".format(message_key, project_id, total_egress, egress_thresh))
            message_state[message_key] = week_num

    #
    # Sum up all months of expenses:
    #

    total_spend = 0
    for month, cost_amount in budget_state.items():
        total_spend += cost_amount
    fraction = float(total_spend) / float(budget_amount)

    need_to_act = (fraction >= 1.0)

    if need_to_act:
        message_key = message_root_fmt.format("2")
        fire_off = (message_key not in message_state) or message_state[message_key] != week_num
        if fire_off:
            print("{}: Project {} is now over budget! fraction {}".format(message_key, project_id, str(fraction)))
            message_state[message_key] = week_num

    #
    # Stash the latest cost data and message status back in the bucket before we wander off and do things, just in
    # case everything goes nuts:
    #

    # Seems we get a checksum complaint if we don't reinitialize the blob:
    blob = bucket.blob(state_blob)
    blob.upload_from_string(json.dumps(budget_super_state))

    #
    # We want to limit the number of instances that can be run without having to set a quota:
    #

    excess_vm_stop_list = []
    compute_inventory = {}
    is_enabled = _check_compute_services_for_project(project_id)
    if is_enabled:
        compute_inventory, total_cpus = _check_compute_engine_inventory(project_id)
        for k, v in compute_inventory.items():
            print("Project {} instance {} info {}".format(project_id, str(k), json.dumps(v)))
        print("Project {} cpu count {}".format(project_id, total_cpus))
        if total_cpus > MAX_CPUS:
            message_key = message_root_fmt.format("3")
            shutdown = total_cpus - MAX_CPUS
            print("{}: Project {} is over CPU limit: {} Shutting down {}".format(message_key, project_id, total_cpus, shutdown))
            excess_vm_stop_list = _prepare_shutdown_list(compute_inventory, shutdown)
    else:
        print("Project {} does not have compute enabled".format(project_id))

    #
    # If we need to act, do it:
    #

    have_appengine = _check_appengine(project_id)
    if need_to_act:
        full_stop_list = _prepare_shutdown_list(compute_inventory)
        if len(full_stop_list) > 0:
            print("Project {} turning off VMs".format(project_id))
            _process_shutdown_list(project_id, full_stop_list)
        if have_appengine:
            print("Project {} turning off App Engine".format(project_id))
            _turn_off_appengine(project_id)
            _check_appengine(project_id)
        print("Project {} finished turning off resources".format(project_id))
        if fraction >= PULL_MULTIPLIER:
            message_key = message_root_fmt.format("4")
            print("{}: Project {} pulling billing account".format(message_key, project_id))
            _pull_billing_account(project_id, project_name)
            print("Project {} completed pulling billing account".format(project_id))
    else:
        billing_on = _check_billing(project_id, project_name)
        if billing_on:
            print("Project {} billing enabled: {}".format(project_id, str(billing_on)))
        print("Project {} fraction {} still in bounds".format(project_id, str(fraction)))
        num_excess = len(excess_vm_stop_list)
        if num_excess > 0:
            print("Project {} turning off {} excess VMs".format(project_id, num_excess))
            _process_shutdown_list(project_id, excess_vm_stop_list)

    return

def _check_billing(project_id, project_name):
    billing = discovery.build('cloudbilling', 'v1', cache_discovery=False)
    projects = billing.projects()
    billing_enabled = _is_billing_enabled(project_name, projects)
    return billing_enabled

def _pull_billing_account(project_id, project_name):
    billing = discovery.build('cloudbilling', 'v1', cache_discovery=False)

    projects = billing.projects()

    billing_enabled = _is_billing_enabled(project_name, projects)

    if billing_enabled:
        _disable_billing_for_project(project_id, project_name, projects)
    else:
        print('Project {} billing already disabled'.format(project_id))
    return

def _is_billing_enabled(project_name, projects):
    try:
        res = projects.getBillingInfo(name=project_name).execute()
        billing_enabled = res['billingEnabled']
        return billing_enabled
    except KeyError:
        print("Check for billing enabled returns key error; billing not enabled.")
        # If billingEnabled isn't part of the return, billing is not enabled
        return False
    except Exception as e:
        print('Unable to determine if billing is enabled on specified project, assuming billing is enabled {}'.format(str(e)))
        return True

def _disable_billing_for_project(project_id, project_name, projects):
    body = {'billingAccountName': ''}  # Disable billing
    try:
        res = projects.updateBillingInfo(name=project_name, body=body).execute()
        print('Project {} billing response: {}'.format(project_id, json.dumps(res)))
        print('Project {} billing disabled'.format(project_id))
    except Exception:
        print('Project {} failed to disable billing, possibly check permissions'.format(project_id))
    return

def _list_services_for_project(project_id):
    service_u = discovery.build('serviceusage', 'v1', cache_discovery=False)

    service_list = []
    s_request = service_u.services().list(parent="projects/idc-external-000")
    while s_request is not None:
        response = s_request.execute()
        for serv in response['services']:
            service_list.append(serv["name"])
        s_request = service_u.services().list_next(previous_request=s_request, previous_response=response)

    return service_list

def _check_compute_services_for_project(project_id):
    service_u = discovery.build('serviceusage', 'v1', cache_discovery=False)

    s_request = service_u.services().get(name="projects/{}/services/compute.googleapis.com".format(project_id))
    response = s_request.execute()
    return response['state'] == "ENABLED"


# This does not appear to work! Do not use:

def _check_appengine_services_for_project(project_id):
    service_u = discovery.build('serviceusage', 'v1', cache_discovery=False)

    s_request = service_u.services().get(name="projects/{}/services/appengine.googleapis.com".format(project_id))
    response_1 = s_request.execute()

    s_request = service_u.services().get(name="projects/{}/services/appengineflex.googleapis.com".format(project_id))
    response_2 = s_request.execute()

    print("Project: {} appengine: {} appengineflex: {}".format(project_id, response_1['state'], response_2['state']))

    return response_1['state'] == "ENABLED"


def _check_compute_engine_inventory(project_id):
    compute = discovery.build('compute','v1',cache_discovery=False)
    zone_list = _list_zones(project_id, compute)
    instance_dict = {}
    total_cpus = 0
    if len(zone_list) > 0:
        instances = compute.instances()
        for zone in zone_list:
            total_cpus += _describe_running_instances(project_id, zone["name"], compute, instances, instance_dict)
    return instance_dict, total_cpus

def _prepare_shutdown_list(compute_inventory, stop_count=None):
    by_stamp = {}
    for k, v in compute_inventory.items():
        result = datetime.datetime.strptime(v['started'], '%Y-%m-%dT%H:%M:%S.%f%z')
        if result not in by_stamp:
            tuples_for_stamp = []
            by_stamp[result] = tuples_for_stamp
        else:
            tuples_for_stamp = by_stamp[result]
        tuples_for_stamp.append(k)

    count = 0
    retval = []
    for started in sorted(by_stamp.keys(), reverse=True):
        print("tuple {}".format(by_stamp[started]))
        for_stamp = by_stamp[started]
        for machine_tuple in for_stamp:
            retval.append(machine_tuple)
            count += k[2]
            if stop_count is not None and count > stop_count:
                break
        if stop_count is not None and count > stop_count:
            break

    return retval

def _process_shutdown_list(project_id, stop_list):

    if len(stop_list) > 0:
        names_by_zone = {}
        for zone_and_name in stop_list:
            zone = zone_and_name[0]
            if zone not in names_by_zone:
                names_in_zone = []
                names_by_zone[zone] = names_in_zone
            else:
                names_in_zone = names_by_zone[zone]
            names_in_zone.append(zone_and_name[1])

        compute = discovery.build('compute', 'v1', cache_discovery=False)
        instances = compute.instances()
        for zone, names in names_by_zone.items():
            _stop_instances(project_id, zone, names, instances)
    return


def _list_zones(project_name, compute):
    zone_list = []
    request = compute.zones().list(project=project_name)
    while request is not None:
        response = request.execute()
        for zone in response['items']:
            zone_list.append(zone)
        request = compute.zones().list_next(previous_request=request, previous_response=response)
    return zone_list


def _describe_running_instances(project_id, zone, compute, instances, instance_dict):

    res = instances.list(project=project_id, zone=zone).execute()

    zone_cpus = 0

    if 'items' not in res:
        return zone_cpus

    items = res['items']

    zone_instance_dict = {}
    types = {}
    for item in items:
        if item['status'] == 'RUNNING':
            instance_info = {'zone': zone, 'name': item['name'], 'status': item['status'],
                             'created': item['creationTimestamp'],
                             'started': item['lastStartTimestamp'],
                             'machineType': item['machineType'].split('/')[-1],
                             'cpus': 0}
            instance_key = (zone, item['name'])
            types[item['machineType'].split('/')[-1]] = 0
            zone_instance_dict[instance_key] = instance_info

    for key in types:
        request = compute.machineTypes().get(project=project_id, zone=zone, machineType=key)
        response = request.execute()
        print("machine type {} cpus {}".format(key, str(response['guestCpus'])))
        types[key] = int(response['guestCpus'])

    for key, instance_info in zone_instance_dict.items():
        instance_info['cpus'] = types[instance_info['machineType']]
        full_key = (key[0], key[1], instance_info['cpus'])
        zone_cpus += instance_info['cpus']
        instance_dict[full_key] = instance_info

    return zone_cpus

def _stop_instances(project_id, zone, instance_names, instances):

    if not len(instance_names):
        print('No running instances were found in zone {}.'.format(zone))
        return

    for name in instance_names:
        instances.stop(project=project_id, zone=zone, instance=name).execute()
        print('Instance stopped successfully: {}'.format(name))

    return


def _check_appengine(project_id):

    appengine = discovery.build('appengine', 'v1', cache_discovery=False)
    apps = appengine.apps()

    # Get the target app's serving status
    try:
        target_app = apps.get(appsId=project_id).execute()
    except HttpError as e:
        if e.resp.status == 404:
            print('Project {} does not have App Engine enabled'.format(project_id))
            return
        else:
            raise e

    current_status = target_app['servingStatus']

    print('Project {} App Engine status {}'.format(project_id, str(current_status)))

    return current_status == "SERVING"


def _turn_off_appengine(project_id):

    appengine = discovery.build('appengine', 'v1', cache_discovery=False)
    apps = appengine.apps()

    # Get the target app's serving status
    target_app = apps.get(appsId=project_id).execute()
    current_status = target_app['servingStatus']

    # Disable target app, if necessary
    if current_status == 'SERVING':
        print('Attempting to disable app {}...'.format(project_id))
        body = {'servingStatus': 'USER_DISABLED'}
        apps.patch(appsId=project_id, updateMask='serving_status', body=body).execute()

    return

'''
----------------------------------------------------------------------------------------------
Calculate egress charges
'''
def _calc_egress(table_name, project_id):
    sql = _egress_sql(table_name, project_id)
    results = _bq_harness_with_result(sql, False)
    data_move_costs = 0.0
    if results is not None:
        for row in results:
            data_move_costs += row.totalCost
    return data_move_costs

'''
----------------------------------------------------------------------------------------------
Download SQL
'''
def _egress_sql(table_name, project_id):

    # Download APAC = 1F8B-71B0-3D1B
    # Download Australia = 9B2D-2B7D-FA5C
    # Download China = 4980-950B-BDA6
    # Download Worldwide Destinations (excluding Asia & Australia) = 22EB-AAE8-FBCD
    #

    # Inter-region GCP Storage egress within NA
    #    * gsutil cp from us-west bucket to a VM running in us-central
    #    * gsutil cp from us-west bucket to a bucket in us-east
    #    * Both operations cost: 2.739579800516365 gibibyte -> $0.0224 each
    # Download Worldwide Destinations (excluding Asia & Australia):
    #    * gsutil cp from from us-west bucket to local laptop
    #    * Operation cost : 2.7395823346450907 gibibyte -> $0.209
    # All operations took place on Sunday evening and appeared in the BQ table ~ 1 PM Monday
    # In another test, download charges appeared in the budget total sent to cloud function (4 AM PST Tuesday)
    # approx 13 hours after the operation (2-3 PM PST Monday). Appeared in BQ table at 4:40 AM PST Tuesday.
    #  "GCP Storage egress between"

    sql = '''
        WITH
          t1 AS (
          SELECT
            project.id AS project_id,
            project.name AS project_name,
            service.description AS service,
            sku.description AS sku,
            cost,
            usage.amount AS usage_amount,
            usage.unit AS usage_unit,
            invoice.month AS invoice_yyyymm
          FROM
            `{0}`
          WHERE
            project.id = "{1}" AND (sku.description LIKE "%ownload%" OR sku.description LIKE "%gress%")),
          t2 AS (
          SELECT
            project_id,
            project_name,
            service,
            sku,
            SUM(cost) AS totalCost,
            SUM(usage_amount) AS totalUsage,
            usage_unit,
            invoice_yyyymm
          FROM
            t1
          GROUP BY
            1,
            2,
            3,
            4,
            7,
            8)
        SELECT
          *
        FROM
          t2
        ORDER BY
          totalCost DESC

        '''.format(table_name, project_id)
    return sql

'''
----------------------------------------------------------------------------------------------
Use to run queries where we want to get the result back to use (not write into a table)
'''
def _bq_harness_with_result(sql, do_batch):
    """
    Handles all the boilerplate for running a BQ job
    """

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH
    location = 'US'

    # API request - starts the query
    query_job = client.query(sql, location=location, job_config=job_config)

    # Query
    job_state = 'NOT_STARTED'
    while job_state != 'DONE':
        query_job = client.get_job(query_job.job_id, location=location)
        #print('Job {} is currently in state {}'.format(query_job.job_id, query_job.state))
        job_state = query_job.state
        if job_state != 'DONE':
            time.sleep(5)
    #print('Job {} is done'.format(query_job.job_id))

    query_job = client.get_job(query_job.job_id, location=location)
    if query_job.error_result is not None:
        print('Error result!! {}'.format(query_job.error_result))
        return None

    results = query_job.result()

    return results
