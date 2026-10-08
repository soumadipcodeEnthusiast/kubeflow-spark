# Kubeflow Spark Operator + Pipelines: a hands-on lab

## The mental model (read this first)

| Piece | What it really is |
|---|---|
| **Spark Operator** | A Kubernetes controller + a CRD called `SparkApplication`. You describe a Spark job in YAML; the operator launches the driver pod, which launches executor pods, tracks status, and cleans up. |
| **Kubeflow Pipelines (KFP)** | A workflow engine. Each pipeline step is a container/pod. It passes small values/artifacts between steps, records runs, and shows a DAG + results in a UI. |
| **How they connect** | A KFP step is just a pod. Our `run_spark_job` step creates a `SparkApplication`, waits for it, reads the result, and passes it downstream. KFP = the *orchestrator*, Spark Operator = the *Spark launcher*. |

```
 KFP run
  │
  ├─ run_spark_job (pod) ──creates──► SparkApplication (CR)
  │                                      │ watched by
  │                                      ▼
  │                              Spark Operator (controller)
  │                                      │ launches
  │                                      ▼
  │                         driver pod ──► executor pods x N
  │      ◄── reads RESULT_JSON from driver logs ──┘
  │
  ├─ extract_error_rate ─► dsl.If(rate > threshold) ─► send_alert
  └─ build_report ─► Markdown + Metrics in the UI
```

The Spark job generates 2M fake web logs, computes per-endpoint request count, 5xx rate and p95 latency,
and prints the result as one JSON line. (Reading logs keeps this lab storage-free; real pipelines
would write Parquet to S3/MinIO/PVC.)

## 0. Prerequisites

A Kubernetes cluster (kind/minikube is fine: 4 CPU / 8 GB), `kubectl`, `helm`, Python 3.9+.

```bash
# Spark Operator (jobs will run in namespace "kubeflow" for this lab)
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  --set "spark.jobNamespaces={kubeflow}"

# Kubeflow Pipelines standalone (check the KFP releases page for the latest version)
export PIPELINE_VERSION=2.5.0
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=$PIPELINE_VERSION"
kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic?ref=$PIPELINE_VERSION"

# UI
kubectl -n kubeflow port-forward svc/ml-pipeline-ui 8080:80
```

Full Kubeflow (multi-user)? Use your profile namespace, `PIPELINE_SA=default-editor`, and add that
namespace to `spark.jobNamespaces`.

## 1. Install lab resources

```bash
pip install -r requirements.txt
./scripts/setup.sh        # ConfigMap with the Spark script + RBAC for the pipeline's service account
```

## 2. Learn the Spark Operator alone

```bash
kubectl -n kubeflow apply -f k8s/sparkapplication-manual.yaml
kubectl -n kubeflow get sparkapplications -w          # SUBMITTED -> RUNNING -> COMPLETED
kubectl -n kubeflow get pods -w                       # 1 driver + 2 executors appear
kubectl -n kubeflow logs log-analysis-manual-driver | grep RESULT_JSON
kubectl -n kubeflow delete -f k8s/sparkapplication-manual.yaml
```

Look at `kubectl -n kubeflow describe sparkapplication log-analysis-manual`: the Events and
`status.applicationState` are what our pipeline polls.

## 3. Wrap it in a pipeline

```bash
python pipeline/pipeline.py                               # compiles spark_log_pipeline.yaml
python pipeline/pipeline.py --host http://localhost:8080  # compile + submit
```

Or upload `spark_log_pipeline.yaml` in the UI (Pipelines -> Upload) and create a run.
Watch `kubectl -n kubeflow get sparkapplications,pods -w` at the same time. You will see the SparkApplication
appear and disappear while the step runs.

In the run UI, open `build_report` -> Visualizations for the Markdown table and metrics.
`send_alert` should run (default overall error ~3% > threshold 2.5%).

## 4. Exercises (this is where it sticks)

1. Set `error_threshold` to `0.05` -> `send_alert` is skipped. Why? (`dsl.If` on a step's output.)
2. Set `executor_instances` to 1, then 4. Compare `spark_elapsed_s`.
3. Break `spark/log_analysis.py` (raise an exception), re-run `./scripts/setup.sh`, run the pipeline.
   Follow how the Spark failure becomes a failed KFP step.
4. Comment out the `finally:` delete in `run_spark_job` and inspect driver/executor pods afterwards.
5. Replace the log-parsing hand-off with writing Parquet to MinIO/S3 and reading it in `build_report`.
6. Add `restartPolicy: {type: OnFailure, onFailureRetries: 2}` and watch the operator retry.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Step fails with 403 creating SparkApplication | Wrong `PIPELINE_SA`/`NS` in setup.sh. |
| SparkApplication stuck in SUBMITTED / no driver pod | Namespace missing from `spark.jobNamespaces`, or driver service account name wrong (`kubectl -n kubeflow get sa`). |
| Driver can't find `/opt/spark/scripts/log_analysis.py` | ConfigMap missing, or the operator webhook is disabled (needed for `configMaps`). |
| Pods Pending | Not enough cluster CPU/memory; lower `executor_instances`. |
| `ImagePullBackOff` | Check the Spark image tag exists, or use `apache/spark:3.5.3-python3`. |

## Files

- `spark/log_analysis.py`: the PySpark job
- `k8s/sparkapplication-manual.yaml`: Spark Operator on its own
- `k8s/rbac.yaml`: permissions for the pipeline step
- `pipeline/pipeline.py`: the KFP pipeline (4 components, one conditional)
- `scripts/setup.sh`: creates ConfigMap and RBAC
