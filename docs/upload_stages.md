# Data Upload Stages

For v2 schema users, variant data will no longer be directly uploaded intp
BigQuery designated table. Instead, as an intermediary step, data is converted
into Avro files and stored on GCS in the
`{TEMP_LOCATION}/avro/{JOB_NAME}/{PIPELINE_INITIATION_TIME_BASED_ID}/*`
directory, from where it's uploaded into BQ. This approach serves several
purposes:

 - Speed up writing stage.
   - In v1 approach, data is converted into JSON files instead of AVRO, which
   takes toll on the operation speed of the pipeline.
 - Allow partitioning and clustering of the output BQ tables.
   - At the moment of writing this document, BigQuery sink did not provide
   functionality to partition output tables, which had to be done via bq CLI on
   empty tables.
 - Provide backup functionality
   - Uploading Avro files into BigQuery is a free and fast opertation, with the
   operation cost falling on the Avro generation stage. Therefore, these avro
   files can be useful substitution for backing up data.

## Generating Avro files

Prior to variants being generated, for each sample name a unique ID is
generated. A mapping of these sample IDs, names, input file names as well as
creation time (to minute) are converted to Avro format and stored on the
aforementioned directory. Then for each variant, its sample names are converted
to sample IDs instead, sharded into corresponding cohorts defined in the
provided sharding config, and stored in the corresponding Avro files. Alongside
them, a schema for the BQ tables is stored.

Note that Apache Beam splits data into multiple files if their size is too
large, so for each provided `suffix` in the partition config, N tables will be
generated with a `suffix-K-of-N` name format.

## Creating BQ Tables

Once the pipeline finishes, for each suffix an empty BigQuery table is
generated, with the corresponding schema. Each tables is partitioned based on
the variant start position, to improve query speeds. Then, Avro files are
uploaded into the generated BQ tables, in a multi threaded way.

Once the upload of the files is complete, Avro directory is automatically
deleted. However, if the upload stage fails for one reason or another, all the
generated BigQuery tables are deleted, while Avro files are not. This will allow
users, using the provided schema, to retry uploading Avro files back to BigQuery
at no cost, instead of regenerating data from the VCF files.

## Generating Sample-ID based tables.

The partitioning logic in the first table optimizes querying variants by
clustering and partitioning on the  start position. However, if users anticipate
heave usage of the sample based queries, they have an option of duplicating
their BigQuery tables and partition them based on the sample IDs, by enabling
--

## Backing up data