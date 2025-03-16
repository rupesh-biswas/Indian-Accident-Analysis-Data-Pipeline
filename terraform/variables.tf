variable "credentials" {
  description = "My Credentials"
  default     = "../keys/creds.json"
}

variable "project" {
  description = "Project"
  default     = "kestra-sandbox-449108"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "Indian Road Accidents Analysis"
  default     = "indian_road_accidents"
}

variable "gcs_bucket_name" {
  description = "Indian Road Accidents Analysis bucket"
  default     = "indian_road_accidents-sandbox-449108"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "dataproc_name" {
  description = "Dataproc cluster name"
  default     = "indian-road-accidents-cluster" 
}

variable "dataproc_bucket" {
  description = "Dataproc bucket name"
  default     = "dataproc-staging-bucket-sandbox-449108" 
}

variable "dataproc_cluster_zone" {
  description = "Dataproc cluster zone"
  default     = "us-central1-f"
  
}

variable "dataproc_service_account" {
  description = "The email of an existing Google Cloud service account"
  default     = "zoomcamp@kestra-sandbox-449108.iam.gserviceaccount.com"
}

