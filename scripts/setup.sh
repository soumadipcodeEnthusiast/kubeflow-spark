#!/bin/bash
set -e

NAMESPACE=${NS:-kubeflow}
PIPELINE_SA=${PIPELINE_SA:-pipeline-runner} # Use pipeline-runner for standalone KFP

echo "Creating ConfigMap for Spark script in namespace $NAMESPACE..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap spark-log-analysis-script \
  --from-file=log_analysis.py=spark/log_analysis.py \
  -n $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

echo "Applying RBAC for pipeline service account $PIPELINE_SA..."
# Replace ServiceAccount name in rbac.yaml before applying
sed "s/name: default-editor/name: $PIPELINE_SA/" k8s/rbac.yaml | kubectl -n $NAMESPACE apply -f -

echo "Creating spark service account for driver..."
kubectl create sa spark -n $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=$NAMESPACE:spark --dry-run=client -o yaml | kubectl apply -f -

echo "Setup complete!"
