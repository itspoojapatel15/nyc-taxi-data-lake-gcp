resource "google_bigquery_dataset" "taxi" {
  dataset_id = "taxi_analytics"
  location   = "US"
}

resource "google_bigquery_table" "yellow_trips_external" {
  dataset_id = google_bigquery_dataset.taxi.dataset_id
  table_id   = "yellow_trips_external"
  deletion_protection = false

  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.processed.name}/yellow_trips/*"]

    hive_partitioning_options {
      mode              = "AUTO"
      source_uri_prefix = "gs://${google_storage_bucket.processed.name}/yellow_trips/"
    }
  }
}

resource "google_bigquery_table" "zone_aggregates_external" {
  dataset_id = google_bigquery_dataset.taxi.dataset_id
  table_id   = "zone_aggregates_external"
  deletion_protection = false

  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.processed.name}/zone_aggregates/*"]

    hive_partitioning_options {
      mode              = "AUTO"
      source_uri_prefix = "gs://${google_storage_bucket.processed.name}/zone_aggregates/"
    }
  }
}
