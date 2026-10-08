import kfp
from kfp import dsl
from kfp.dsl import Output, Markdown, Metrics, component

@component(base_image="python:3.9", packages_to_install=["kubernetes"])
def run_spark_job(
    name: str,
    namespace: str,
    executor_instances: int,
    output_json: Output[dsl.Artifact]
):
    import time
    from kubernetes import client, config
    
    config.load_incluster_config()
    api = client.CustomObjectsApi()
    
    # Create SparkApplication dictionary
    spark_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": name,
            "namespace": namespace
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": "docker.io/apache/spark:3.5.3-python3",
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": "local:///opt/spark/scripts/log_analysis.py",
            "sparkVersion": "3.5.3",
            "restartPolicy": {
                "type": "Never"
            },
            "volumes": [
                {
                    "name": "spark-scripts",
                    "configMap": {
                        "name": "spark-log-analysis-script"
                    }
                }
            ],
            "driver": {
                "cores": 1,
                "coreLimit": "1200m",
                "memory": "512m",
                "labels": {
                    "version": "3.5.3"
