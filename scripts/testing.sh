export PROJECT_ID=""
export REGION=""
export CLUSTER_NAME=""

gcloud config set project $PROJECT_ID

echo "===================================================="
echo " Setting external IP access ..."

echo "{
  \"constraint\": \"constraints/compute.vmExternalIpAccess\",
	\"listPolicy\": {
	    \"allValues\": \"ALLOW\"
	  }
}" > external_ip_policy.json

gcloud resource-manager org-policies set-policy external_ip_policy.json --project=$PROJECT_ID

gcloud dataproc clusters create $CLUSTER_NAME-generic \
    --region=$REGION

