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
                },
                "serviceAccount": "spark",
                "volumeMounts": [
                    {
                        "name": "spark-scripts",
                        "mountPath": "/opt/spark/scripts"
                    }
                ]
            },
            "executor": {
                "cores": 1,
                "instances": executor_instances,
                "memory": "512m",
                "labels": {
                    "version": "3.5.3"
                },
                "volumeMounts": [
                    {
                        "name": "spark-scripts",
                        "mountPath": "/opt/spark/scripts"
                    }
                ]
            }
        }
    }
    
    try:
        print(f"Submitting SparkApplication {name} in namespace {namespace}...")
        api.create_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=namespace,
            plural="sparkapplications",
            body=spark_app
        )
        
        # wait for completion
        while True:
            res = api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
                name=name
            )
            state = res.get("status", {}).get("applicationState", {}).get("state", "UNKNOWN")
            print(f"SparkApplication state: {state}")
            if state in ["COMPLETED", "FAILED"]:
                break
            time.sleep(5)
            
        if state != "COMPLETED":
            raise Exception(f"SparkApplication failed with state: {state}")
            
        # extract logs
        core_v1 = client.CoreV1Api()
        driver_pod = f"{name}-driver"
        print(f"Extracting logs from driver pod {driver_pod}...")
        logs = core_v1.read_namespaced_pod_log(name=driver_pod, namespace=namespace)
        
        result_json = None
        for line in logs.splitlines():
            if line.startswith("RESULT_JSON:"):
                result_json = line.replace("RESULT_JSON:", "")
                break
                
        if not result_json:
            raise Exception("RESULT_JSON not found in driver logs")
            
        with open(output_json.path, "w") as f:
