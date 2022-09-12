gcloud init

export PROJECT_ID=""
export PROJECT_NUMBER=""
export REGION=""
export ZONE=""

gcloud config set project $PROJECT_ID

gcloud services enable storage-component.googleapis.com 
gcloud services enable compute.googleapis.com  
gcloud services enable servicenetworking.googleapis.com 
gcloud services enable iam.googleapis.com 
gcloud services enable dataproc.googleapis.com
gcloud services enable cloudbilling.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable logging.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable eventarc.googleapis.com

git clone https://github.com/CYarros10/dataproc-spark-property-monitoring.git

cd dataproc-spark-property-monitoring/terraform

# edit the variables.tf
sed -i "s|%%PROJECT_ID%%|$PROJECT_ID|g" variables.tf
sed -i "s|%%PROJECT_NUMBER%%|$PROJECT_NUMBER|g" variables.tf
sed -i "s|%%REGION%%|$REGION|g" variables.tf
sed -i "s|%%ZONE%%|$ZONE|g" variables.tf

terraform init
terraform plan
terraform apply
