import base64
import os
import math
import json
import functions_framework
from google.cloud import dataproc_v1
from google.cloud import storage
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

PROJECT_ID=os.environ.get('PROJECT_ID', 'Specified environment variable is not set.')
REGION=os.environ.get('REGION', 'Specified environment variable is not set.')
ZONE=os.environ.get('ZONE', 'Specified environment variable is not set.')
BUCKET_NAME=os.environ.get('BUCKET_NAME', 'Specified environment variable is not set.')

def load_dataproc_machine_type_info(machine_type):
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    filter_string = f"name:{machine_type}"
    request = service.machineTypes().list(project=PROJECT_ID, zone=ZONE, filter=filter_string)
    
    dataproc_machine_types = []
    while request is not None:
        response = request.execute()
        for machine_type in response['items']:
            dataproc_machine_types.append(machine_type)
        request = service.machineTypes().list_next(previous_request=request, previous_response=response)
    return dataproc_machine_types

def upload_blob(bucket_name, destination_blob_name, data_from_flowfile_as_string):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data_from_flowfile_as_string)

def evaluate_spark_properties(cluster):
    """Evaluate each cluster's spark properties based on standardized best practices
    
    return report of necessary changes.
    """
    worker_map = cluster.config.worker_config
    property_map = cluster.config.software_config.properties

    machine_type_str = worker_map.machine_type_uri.split("/")[len(worker_map.machine_type_uri.split("/"))-1]
    machine_type_info = load_dataproc_machine_type_info(machine_type_str)[0]

    machine_type = machine_type_info['name']
    nodes = int(worker_map.num_instances)
    ram_per_node = machine_type_info['memoryMb']
    vcores = machine_type_info['guestCpus']

    spark_executor_cores = str(property_map['spark:spark.executor.cores'])
    spark_driver_cores = str(property_map['spark:spark.driver.cores'])
    spark_executor_instances = str(property_map['spark:spark.executor.instances'])
    spark_executor_memory = str(property_map['spark:spark.executor.memory'])
    spark_driver_memory = str(property_map['spark:spark.driver.memory'])
    spark_executor_memory_overhead = str(property_map['spark:spark.executor.memoryOverhead'])
    spark_default_parallelism = str(property_map['spark:spark.default.parallelism'])
    spark_sql_shuffle_partitions = str(property_map['spark:spark.sql.shuffle.partitions'])
    spark_shuffle_spill_compress = str(property_map['spark:spark.shuffle.spill.compress'])
    spark_checkpoint_compress = str(property_map['spark:spark.checkpoint.compress'])
    spark_io_compression_codec = str(property_map['spark:spark.io.compression.codec'])
    spark_dynamic_allocation_enabled = str(property_map['spark:spark.dynamicAllocation.enabled'])
    spark_shuffle_service_enabled = str(property_map['spark:spark.shuffle.service.enabled'])
    
    if 'g' in spark_executor_memory:
        spark_executor_memory = gb_to_mb_property(spark_executor_memory)
    if 'g' in spark_driver_memory:
        spark_driver_memory = gb_to_mb_property(spark_driver_memory)
    if 'g' in spark_executor_memory_overhead:
        spark_executor_memory_overhead = gb_to_mb_property(spark_executor_memory_overhead)

    executor_per_node = round((vcores-1)/5) if round((vcores-1)/5) > 0 else 1
    
    rec_spark_executor_instances = (executor_per_node * nodes)-1 if (executor_per_node * nodes)-1 > 0 else 1
    rec_spark_executor_memory = str(math.floor((ram_per_node -1024) / executor_per_node * 0.9))+"m"
    rec_spark_driver_memory = rec_spark_executor_memory
    rec_spark_executor_memory_overhead = str(math.ceil((ram_per_node -1024) / executor_per_node * 0.1))+"m"
    rec_spark_default_parallelism = ((executor_per_node * nodes)-1) * 10
    rec_spark_sql_shuffle_partitions = rec_spark_default_parallelism

    recommendations = {}

    if(spark_executor_cores != 5):
        recommendations['spark:spark.executor.cores'] = 5

    if (spark_driver_cores != 5):
        recommendations['spark:spark.driver.cores'] = 5

    if (spark_shuffle_spill_compress != 'true'):
        recommendations['spark:spark.shuffle.spill.compress'] = 'true'

    if (spark_checkpoint_compress != 'true'):
        recommendations['spark:spark.checkpoint.compress'] = 'true'
    
    if (spark_io_compression_codec == ''):
        recommendations['spark:spark.io.compression.codec'] = 'snappy (if splittable files), lz4 (otherwise)'

    if (spark_shuffle_service_enabled == ''):
        recommendations['spark:spark.shuffle.service.enabled'] = 'true (if multiple spark apps on cluster), false (otherwise)'
	
    if (spark_dynamic_allocation_enabled == ''):
	    recommendations['spark:spark.dynamicAllocation.enabled'] = 'true (if multiple spark apps on cluster), false (otherwise)'

    if (spark_executor_instances != rec_spark_executor_instances):
        recommendations['spark:spark.executor.instances'] = rec_spark_executor_instances

    if (spark_executor_memory != rec_spark_executor_memory):
        recommendations['spark:spark.executor.memory'] = rec_spark_executor_memory
    if (spark_driver_memory != rec_spark_driver_memory):
        recommendations['spark:spark.driver.memory'] = rec_spark_driver_memory

    if (spark_executor_memory_overhead != rec_spark_executor_memory_overhead):
        recommendations['spark:spark.executor.memoryOverhead'] = rec_spark_executor_memory_overhead

    if (spark_default_parallelism != rec_spark_default_parallelism):
        recommendations['spark:spark.default.parallelism'] = rec_spark_default_parallelism

    if (spark_sql_shuffle_partitions != rec_spark_sql_shuffle_partitions):
        recommendations['spark:spark.sql.shuffle.partitions'] = rec_spark_sql_shuffle_partitions
    
    current_config = {}
    current_config['cluster_name'] = cluster.cluster_name
    current_config['machine_type'] = machine_type
    current_config['nodes'] = nodes
    current_config['vcores'] = vcores
    current_config['ram_per_node'] = ram_per_node
    current_config['spark.executor.cores'] = spark_executor_cores
    current_config['spark.driver.cores'] = spark_driver_cores
    current_config['spark.executor.instances'] = spark_executor_instances
    current_config['spark.executor.memory'] = spark_executor_memory
    current_config['spark.driver.memory'] = spark_driver_memory
    current_config['spark.executor.memoryOverhead'] = spark_executor_memory_overhead
    current_config['spark.default.parallelism'] = spark_default_parallelism
    current_config['spark.sql.shuffle.partitions'] = spark_sql_shuffle_partitions
    current_config['spark.shuffle.spill.compress'] = spark_shuffle_spill_compress
    current_config['spark.io.compression.codec'] = spark_io_compression_codec
    current_config['spark.dynamicAllocation.enabled'] = spark_dynamic_allocation_enabled
    current_config['spark.shuffle.service.enabled'] = spark_shuffle_service_enabled

    report = {}
    report['current_configuration'] = current_config
    report['recommendations'] = recommendations

    json_report = json.dumps(report, indent=4)
    
    return json_report

def gb_to_mb_property(prop):
    tmp = int(prop[:-1])*1024
    return str(tmp)+"m"

def evaluate_persistent_disk(cluster):
    #TODO
    print()

def evaluate_dataproc_clusters():
    # Create a client
    client = dataproc_v1.ClusterControllerClient(
        client_options={'api_endpoint': f'{REGION}-dataproc.googleapis.com:443'}
    )

    # Initialize request argument(s)
    request = client.list_clusters(
        project_id=PROJECT_ID,
        region=REGION,
    )

    # Handle the response
    for response in request:
        upload_blob(BUCKET_NAME, "dataproc-cluster-configuration-library/"+str(response.cluster_name), str(response))
        upload_blob(BUCKET_NAME,"dataproc-cluster-spark-recommendations/"+str(response.cluster_name), str(evaluate_spark_properties(response)))

@functions_framework.cloud_event
def execute(cloud_event):
    evaluate_dataproc_clusters()
