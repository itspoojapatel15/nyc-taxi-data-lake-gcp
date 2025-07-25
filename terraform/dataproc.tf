resource "google_dataproc_cluster" "taxi_cluster" {
  name   = "taxi-processing-${var.environment}"
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n2-standard-4"
      disk_config {
        boot_disk_size_gb = 100
        boot_disk_type    = "pd-ssd"
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "n2-standard-4"
      disk_config {
        boot_disk_size_gb = 100
        boot_disk_type    = "pd-ssd"
      }
    }

    autoscaling_config {
      policy_uri = google_dataproc_autoscaling_policy.taxi_policy.name
    }

    software_config {
      image_version = "2.1-debian11"
      override_properties = {
        "spark:spark.sql.parquet.compression.codec" = "snappy"
        "spark:spark.sql.adaptive.enabled"          = "true"
      }
    }

    gce_cluster_config {
      internal_ip_only = false
    }
  }
}

resource "google_dataproc_autoscaling_policy" "taxi_policy" {
  policy_id = "taxi-autoscaling"
  location  = var.region

  basic_algorithm {
    yarn_config {
      scale_up_factor   = 1.0
      scale_down_factor = 1.0
      scale_up_min_worker_fraction = 0.0
      graceful_decommission_timeout = "3600s"
    }
    cooldown_period = "120s"
  }

  worker_config {
    min_instances = 2
    max_instances = 10
    weight        = 1
  }
}
