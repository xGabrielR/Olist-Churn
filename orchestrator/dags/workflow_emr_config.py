ICEBERG_WAREHOUSE_PATH = "s3a://grc-lh-bronze"

SPARK_S3_SCRIPTS = [
    's3://grc-lh-scripts/jobs/spark_bronze.py',
    's3://grc-lh-scripts/jobs/spark_silver.py',
    's3://grc-lh-scripts/jobs/spark_gold.py',
    's3://grc-lh-scripts/jobs/spark_analytics.py'
]

DEFAULT_SPARK_EMR_CONFIGS = [
    '--conf', 'spark.dynamicAllocation.enabled=true',
    '--conf', 'spark.shuffle.service.enabled=true',
]

JOB_FLOW_OVERRIDES = {
    'Name': 'grc-lh-emr-cluster',
    'ReleaseLabel': 'emr-7.0.0',
    'VisibleToAllUsers': True,

    # Default role names for EMR, created with terraform
    'JobFlowRole': 'EmrEc2DefaultRole_profile',
    'ServiceRole': 'EmrDefaultRole',

    'Applications': [{ 'Name': 'Spark' }],

    'AutoTerminationPolicy': {
        'IdleTimeout': 3600
    },

    'Instances': {
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,

        # YOU NEED TO PROVIDE
        'Ec2KeyName': '',
        'Ec2SubnetId': '',
        'LogUri': 's3://grc-lh-scripts/emr-logs',
    },

    "Configurations": [
        {
            "Classification": "spark-env",
            "Properties": {},
            "Configurations": [{
                "Classification": "export",
                "Properties": {
                    "PYSPARK_PYTHON": "/usr/bin/python3",
                    "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                }
            }]
        },
        {
            "Classification": "iceberg-defaults",
            "Properties": {
                "iceberg.enabled": "true"
            }
        },
        {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.4.3",
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.defaultCatalog": "catalog",
                "spark.sql.catalog.catalog": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                "spark.sql.catalog.catalog.warehouse": ICEBERG_WAREHOUSE_PATH, # Default apache iceberg warehouse
                "spark.sql.catalog.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                "spark.sql.sources.partitionOverwriteMode": "dynamic",
                "spark.submit.deployMode": "cluster",
                "spark.speculation": "false",
                "spark.sql.adaptive.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.driver.extraJavaOptions": 
                    "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                    "spark.storage.level": "MEMORY_AND_DISK_SER",
                    "spark.rdd.compress": "true",
                    "spark.shuffle.compress": "true",
                    "spark.shuffle.spill.compress": "true"
            }
        },
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        },
    ]
}


SPARK_ICEBERG_STEPS = []
for spark_s3_scripts in SPARK_S3_SCRIPTS:
    SPARK_ICEBERG_STEPS.append(
        {
            'Name': f'process_iceberg_{spark_s3_scripts.split("/")[-1].replace(".py", "")}',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit'] + \
                    DEFAULT_SPARK_EMR_CONFIGS + \
                    ['--master', 'yarn',
                    '--deploy-mode', 'cluster'] + \
                    [spark_s3_scripts]
            }
        }
    )