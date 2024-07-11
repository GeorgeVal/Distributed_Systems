#!/usr/bin/env bash

#Script that deploys whole application to kubernetes (tested on Minikube)
#Ui is port forwarded to 5003 , you can access ui from http://localhost:5003

set -exuo pipefail


# Clean up anything from a prior run:
kubectl delete statefulsets,persistentvolumes,persistentvolumeclaims,services,poddisruptionbudget,deployments --all

# Make persistent volumes and (correctly named) claims. We must create the
# claims here manually even though that sounds counter-intuitive. For details
# see https://github.com/kubernetes/contrib/pull/1295#issuecomment-230180894.
for i in $(seq 0 3); do
  cat <<EOF | kubectl create -f -
kind: PersistentVolume
apiVersion: v1
metadata:
  name: pv${i}
  labels:
    type: local
    app: minio
spec:
  capacity:
    storage: 2Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: "/minio/data-store/${i}"
EOF

  cat <<EOF | kubectl create -f -
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: data-minio-${i}
  labels:
    app: minio
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 2Gi
EOF
done;

kubectl create -f ./yaml/statefulset.yaml
kubectl apply -f ./yaml/service-account.yaml -f ./yaml/service-role.yaml -f ./yaml/service-binding.yaml
kubectl apply -f ./yaml/mongo-pvc.yaml -f ./yaml/mongo-deployment.yaml -f ./yaml/mongo-service.yaml
kubectl apply -f ./yaml/auth-deployment.yaml -f ./yaml/auth-service.yaml
kubectl apply -f ./yaml/ui-deployment.yaml -f ./yaml/ui-service.yaml

sleep 10
echo "!!! Wait until all pods are running before using the ui !!!"
kubectl port-forward service/ui-service 5003:5003
