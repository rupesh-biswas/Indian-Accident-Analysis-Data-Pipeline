terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "indian_road_accidents" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "indian_road_accidents" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

resource "google_storage_bucket" "dataproc_staging" {
  name          = var.dataproc_bucket
  location      = var.location
  force_destroy = true
}

resource "google_dataproc_cluster" "indian_road_accidents_cluster" {
  name     = var.dataproc_name
  region   = var.region
  graceful_decommission_timeout = "120s"
  labels = {
    usage = "zoomcamp-project",
    usecase = "indian-road-accidents"
  }

  cluster_config {
    staging_bucket = var.dataproc_bucket

    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 30
      }
    }

    worker_config {
      num_instances    = 2
      machine_type     = "n2-standard-4"
      disk_config {
        boot_disk_type = "pd-ssd"
        boot_disk_size_gb = 30
      }
    }

    preemptible_worker_config {
        num_instances = 0
    }

    # Override or set some custom properties
    software_config {
      image_version = "2.2.49-debian12"
      optional_components = ["JUPYTER", "DOCKER"]
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      zone = var.dataproc_cluster_zone
      internal_ip_only = true
      network = "default"
      service_account = var.dataproc_service_account
    }

    # You can define multiple initialization_action blocks
    initialization_action {
      script      = "gs://dataproc-initialization-actions/stackdriver/stackdriver.sh"
      timeout_sec = 500
    }

    autoscaling_config {
      policy_uri = google_dataproc_autoscaling_policy.asp.id
    }
  }
}

resource "google_dataproc_autoscaling_policy" "asp" {
  policy_id = "dataproc-policy"
  location  = var.region

  basic_algorithm {
    cooldown_period = "120s"
    
    yarn_config {
      graceful_decommission_timeout = "30s"

      scale_up_factor   = 0.5
      scale_down_factor = 0.5
    }
  }

  worker_config {
    min_instances = 0
    max_instances = 3
  }
}



