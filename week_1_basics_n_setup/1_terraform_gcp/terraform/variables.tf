variable "credentials"{
    description = "My Credentials"
    default = <Key_Path>
} 

variable "region"{
    description = "Region"
    default = "us-cemtral1"
} 

variable "project"{
    description = "Project Description"
    default = "dezoomcamp-411909"
} 

variable "location"{
    description = "Project Location"
    default = "US"
} 

variable "gcs_bucket_name"{
    description = "My Storage Name"
    default = "dezoomcamp-411909-terra-bucket"
} 

variable "gcs_storage_class"{
    description = "Bucket Storage Class"
    default = "STANDARD"
}

variable "bq_dataset_name"{
    description = "My BigQuery Dataset Name"
    default = "dezoomcamp_411909_terra_dataset"
}



