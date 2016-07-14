#
# Copyright (c) 2016 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This scripts automates deployment of gearpump-app application
(creates required gearpump instance, uploads package to gearpump using
instance's REST API, sends hbase and kafka instances' credentials).
"""

from app_deployment_helpers import cf_cli
from app_deployment_helpers import cf_helpers
from app_deployment_helpers import cf_api
from app_deployment_helpers import gearpump_helpers

import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

PARSER = cf_helpers.get_parser("gearpump-app")
PARSER.add_argument('-n', '--gearpump_instance', type=str, required=True,
                    help='Name of Gearpump instance to be created')
ARGS = PARSER.parse_args()

CF_INFO = cf_helpers.get_info(ARGS)
cf_cli.login(CF_INFO)

logging.info("Creating Gearpump service instance")
cf_cli.create_service('gearpump', '1 worker', ARGS.gearpump_instance)

logging.info("Preparing package")
PROJECT_DIR = cf_helpers.get_project_dir()
cf_helpers.prepare_package(work_dir=PROJECT_DIR)

logging.info('Getting Gearpump service credentials')

SERVICE_CREDENTIALS = \
    cf_api.get_temporary_key_data(ARGS.gearpump_instance)['entity']['credentials']
GEARPUMP_DASHBOARD_URL = SERVICE_CREDENTIALS['dashboardUrl']
USERNAME = SERVICE_CREDENTIALS['username']
PASSWORD = SERVICE_CREDENTIALS['password']

GEARPUMP_JAR_PATH = '../target/'+gearpump_helpers.get_jar_file_name()

#getting login cookie for authorizing deploy request
logging.info("Authenticating to Gearpump instance")
gearpump_helpers.gearpump_login(GEARPUMP_DASHBOARD_URL, USERNAME, PASSWORD)

#sending deploy request
logging.info("Deploying package to Gearpump")
DEPLOY_OUTPUT = gearpump_helpers.deploy_to_gearpump(GEARPUMP_DASHBOARD_URL,
                                                    GEARPUMP_JAR_PATH,
                                                    {"inputTopic":"topicIn",
                                                     "outputTopic":"topicOut",
                                                     "tableName":"pipeline",
                                                     "columnFamily":"message",
                                                     "columnName":"message"},
                                                    ["kafka-inst", "hbase1, kerberos-inst"])

logging.info(DEPLOY_OUTPUT)
