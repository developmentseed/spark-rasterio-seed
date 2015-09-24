# usage:
# ./run-emr.sh s3://path/to/request/arguments.json

APP_NAME="My App"
LOG_BUCKET=s3://my-bucket/logs
CONFIG_BUCKET=s3://my-bucket/config
KEY_NAME=your-key-name
REQUEST=$1

MASTER_INSTANCE=m3.xlarge
MASTER_PRICE=0.15

WORKER_INSTANCE=m3.xlarge
WORKER_PRICE=0.15

# This is how many works beyond the 2 reserve instances
WORKER_COUNT=8

DRIVER_MEMORY=4G
NUM_EXECUTORS=20
EXECUTOR_MEMORY=5G
EXECUTOR_CORES=2

aws emr create-cluster \
  --name "$APP_NAME" \
  --log-uri $LOG_BUCKET \
  --release-label emr-4.0.0 \
  --use-default-roles \
  --auto-terminate \
  --ec2-attributes KeyName=$KEY_NAME \
  --applications Name=Spark \
  --instance-groups \
    Name=Master,InstanceCount=1,InstanceGroupType=MASTER,InstanceType=$MASTER_INSTANCE \
    Name=ReservedWorkers,InstanceCount=2,InstanceGroupType=CORE,InstanceType=$WORKER_INSTANCE \
    Name=SpotWorkers,InstanceCount=$WORKER_COUNT,BidPrice=$WORKER_PRICE,InstanceGroupType=TASK,InstanceType=$WORKER_INSTANCE \
  --bootstrap-action Path=$CONFIG_BUCKET/bootstrap.sh \
  --configurations file://./emr.json \
  --steps \
  Name=CHUNK,ActionOnFailure=CONTINUE,Type=Spark,Args=[--deploy-mode,cluster,--driver-memory,$DRIVER_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-memory,$EXECUTOR_MEMORY,--executor-cores,$EXECUTOR_CORES,$CONFIG_BUCKET/worker.py,$REQUEST]
