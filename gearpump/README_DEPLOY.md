#How to deploy ingestion-ws-kafka-gearpump-hbase on TAP:

##Manual deployment

To see the full instruction go to: https://github.com/trustedanalytics/platform-wiki-0.7/wiki/Example-Ingestion-Using-Apache-Gearpump
This example will use also: hbase-java-api-example, kafka2hdfs and ws2kafka applications.

1. Create kafka instance on platform. In this example we will use ws2kafka app, which needs instance named “kafka-instance”
    - In TAP console go to marketplace and choose “kafka”
    - Create new instance named "kafka-instance"
1. Create hbase instance on platform. In this example we will use hbase-java-api-example, which needs instance named “hbase1”
    - In TAP console go to marketplace and choose “hbase”
    - Create new instance named "hbase1"
1. Deploy applications ws2kafka, kafka2hdfs and hbase-java-api-example (see the documentation of this components) on TAP
    - As a KAFKA_TOPIC in ws2kafka environment variable set for example “topicIn”
1. Get zookeeper uri
    - In TAP console click Services > Instances,
    - Create key for “hbase1”,
    - Click ”Export Keys” in the right top corner,
    - Click “+ Add to exports” near just created key,
    - At the bottom, find “exported Keys” section, then your instance, credentials and copy the value of "ha.zookeeper.quorum" property
1. Now generate output topic table for ingestion-ws-kafka-gearpump-hbase example
    - Go the cdh node, and then cdh-master-0:
       `ssh ec2-user@cdh.domain.com -i ~/.ssh/yourKey.pem`
       `ssh cdh-master-0`
    - Create topics (instead “<>” paste uri copied in step 4, including ‘/kafka’ suffix)
       `kafka-topics --create --zookeeper <<ha.zookeeper.quorum>> --replication-factor 1 --partitions 1 --topic topicIn`
       `kafka-topics --create --zookeeper <<ha.zookeeper.quorum>> --replication-factor 1 --partitions 1 --topic topicOut`
    - If you want to check, if topic has been created
       `kafka-topics --list --zookeeper <<ha.zookeeper.quorum>>`
1. Create a table in hbase using hbase-java-api-example:

   `curl <protocol><hbase-reader-host>.<tap_domain.and:port>/api/tables -X POST -H "Content-Type: application/json" -d '{"tableName":"pipeline","columnFamilies":["message"]}'`

    where 'hbase-reader-host' is defined in manifest.yml of hbase-java-api-example app, 'tap_domain.and:port' is your TAP domain, e.g.:

   `curl http://hbase-reader.trustedanalytics.org:80/api/tables -X POST -H "Content-Type: application/json" -d '{"tableName":"pipeline","columnFamilies":["message"]}'`
1. Create GearPump instance or deploy on existing one
    - In TAP console, go to Data Science > GearPump tab,
    - If you don’t have an instance, you can create it right now,
    - Click link “Deploy App” on the right to chosen instance name,
    - Choose gearpump application jar (from ingestion-ws-kafka-gearpump-hbase/gearpump),
    - Add extra parameters:
        * **inputTopic**: topicIn
        * **outputTopic**: topicOut
        * **tableName**: pipeline
        * **columnFamily**: message
        * **columnName**: message
    - Check hbase instance called “hbase1” and kafka instance “kafka-instance” from the list and deploy the application.
1. Check if the information flow is working – messages should be visible in hbase:
       `curl http://domain.and:port/api/tables/pipeline/head`
1. You can also check output kafka topic:
   - Go to cdh and cdh-master-0, then use command (instead “<>” paste uri copied in step 4):
       `kafka-console-consumer --zookeeper <<ha.zookeeper.quorum>> --topic topicIn --from-beginning`



## Automated deployment
* Deploy applications needed by the flow to work (when deploying remember to provide the params to match the params given automatically to the app being deployed to Gearpump):
    * Deploy ws2kafka app on TAP platform using automated deployment procedure described here: https://github.com/trustedanalytics/ingestion-ws-kafka-hdfs/blob/master/ws2kafka/README.md
    * Deploy hbase-java-api-example app on TAP platform using automated deployment procedure described here: https://github.com/trustedanalytics/hbase-java-api-example/blob/master/README.md
         - After deployment create a table in hbase using hbase-java-api-example:

             `curl <protocol><hbase-reader-host>.<tap_domain.and:port>/api/tables -X POST -H "Content-Type: application/json" -d '{"tableName":"pipeline","columnFamilies":["message"]}'`

              where 'hbase-reader-host' is defined in manifest.yml of hbase-java-api-example app, 'tap_domain.and:port' is your TAP domain, e.g.:

              `curl http://hbase-reader.trustedanalytics.org:80/api/tables -X POST -H "Content-Type: application/json" -d '{"tableName":"pipeline","columnFamilies":["message"]}'`
    * Deploy kafka2hdfs app on TAP platform using automated deployment procedure described here: https://github.com/trustedanalytics/ingestion-ws-kafka-hdfs/blob/master/kafka2hdfs/README.md
* Switch to `/gearpump/deploy` directory: `cd gearpump/deploy`
* Install tox: `sudo -E pip install --upgrade tox`
* Run: `tox`
* Activate virtualenv with installed dependencies: `. .tox/py27/bin/activate`
* Assure that there are proper applications (and service instances) running on TAP platform:
    * ws2kafka (kafka service named **kafka-inst**)
    * hbase-java-api-example (hbase service named **hbase1**)
    * kafka2hdfs (zookeeper service named **zookeeper-inst**, kerberos instance named **kerberos-inst**, hdfs instance named **hdfs-inst**)
* Run deployment script: `python deploy.py` providing required parameters when running script (`python deploy.py -h` to check script parameters with their descriptions).

Application will be deployed with predefined extra parameters:
* **inputTopic**: topicIn
* **outputTopic**: topicOut
* **tableName**: pipeline
* **columnFamily**: message
* **columnName**: message
