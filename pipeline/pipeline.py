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
            f.write(result_json)
            
    finally:
        try:
            print(f"Cleaning up SparkApplication {name}...")
            api.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
                name=name
            )
        except Exception as e:
            print(f"Error during cleanup: {e}")

@component(base_image="python:3.9")
def extract_error_rate(input_json: dsl.Input[dsl.Artifact]) -> float:
    import json
    with open(input_json.path, "r") as f:
        data = json.load(f)
    return float(data.get("overall_error_rate", 0.0))

@component(base_image="python:3.9")
def build_report(
    input_json: dsl.Input[dsl.Artifact],
    markdown_output: Output[Markdown],
    metrics_output: Output[Metrics]
):
    import json
    with open(input_json.path, "r") as f:
        data = json.load(f)
        
    overall_error_rate = data.get("overall_error_rate", 0.0)
    spark_elapsed_s = data.get("spark_elapsed_s", 0.0)
    endpoint_stats = data.get("endpoint_stats", [])
    
    # Metrics
    metrics_output.log_metric("overall_error_rate", overall_error_rate)
    metrics_output.log_metric("spark_elapsed_s", spark_elapsed_s)
    
    # Markdown
    md_content = f"# Spark Log Analysis Report\n\n"
    md_content += f"**Overall Error Rate**: {overall_error_rate:.4f}\n"
    md_content += f"**Execution Time**: {spark_elapsed_s:.2f} seconds\n\n"
    
    md_content += "| Endpoint | Request Count | 5xx Rate | p95 Latency (ms) |\n"
    md_content += "|---|---|---|---|\n"
    
    for stat in endpoint_stats:
        md_content += f"| {stat['endpoint']} | {stat['request_count']} | {stat['5xx_rate']:.4f} | {stat['p95_latency_ms']:.2f} |\n"
        
    with open(markdown_output.path, "w") as f:
        f.write(md_content)

@component(base_image="python:3.9")
def send_alert(error_rate: float, threshold: float):
    print(f"ALERT! The overall error rate {error_rate:.4f} has exceeded the threshold {threshold:.4f}.")

@dsl.pipeline(
    name="spark-log-pipeline",
    description="A pipeline that runs a PySpark job and analyzes logs."
)
def spark_log_pipeline(
    error_threshold: float = 0.025,
    executor_instances: int = 2,
    namespace: str = "kubeflow"
):
    import uuid
    # Use a unique name for the spark application to avoid collisions
    spark_job = run_spark_job(
        name=f"log-analysis-{uuid.uuid4().hex[:6]}",
        namespace=namespace,
        executor_instances=executor_instances
    )
    
    # Setting cache to false for the spark job so it always runs
    spark_job.set_caching_options(False)
    
    error_rate_task = extract_error_rate(input_json=spark_job.outputs["output_json"])
    
    report_task = build_report(input_json=spark_job.outputs["output_json"])
    
    with dsl.If(error_rate_task.output > error_threshold):
        send_alert(error_rate=error_rate_task.output, threshold=error_threshold)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, help="KFP API endpoint")
    args = parser.parse_args()
    
    compiler = kfp.compiler.Compiler()
    yaml_file = "spark_log_pipeline.yaml"
    compiler.compile(pipeline_func=spark_log_pipeline, package_path=yaml_file)
    print(f"Pipeline compiled to {yaml_file}")
    
    if args.host:
        client = kfp.Client(host=args.host)
        run = client.create_run_from_pipeline_func(
            spark_log_pipeline,
            arguments={},
            enable_caching=False
        )
        print(f"Pipeline run submitted: {run.url}")
