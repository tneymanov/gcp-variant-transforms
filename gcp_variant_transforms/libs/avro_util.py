# Copyright 2020 Google Inc.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Constants and simple utility functions related to AVRO files."""

import logging
import time

from google.cloud import bigquery

from gcp_variant_transforms.libs import bigquery_util
_MAX_NUM_CONCURRENT_AVRO_LOAD_JOBS = 4


class LoadAvro():
  """Loads AVRO files from Cloud Storage to already created BigQuery tables."""
  def __init__(self,
               avro_root_path,  # type: str
               output_table,  # type: str
               suffixes,  # type: List[str]
               delete_empty_tables  # type: bool
              ):
    """Initializes `LoadAvro` object.

    This class loads AVRO files generated by Dataflow pipeline into BigQuery.
    In our default sharding config we have 25 output tables, so here `suffixes`
    will have 25 + 1 (sample_info) values. For each of those suffixes this class
    will load destination BigQuery table with its AVRO files, for example:
    for `chr1` suffix:
       gs://TEMP_LOCATION/avro/JOB_NAME/YYYYMMDD_HHMMSS/chr1-*
    will be loaded to:
       PROJECT_ID.DATASET_ID.BASE_TABLE_ID__chr1

    After loading, if 0 rows were loaded (ie empty AVRO files) then destination
    table will be deleted if `delete_empty_tables` is set.

    Note1: This class assumes the destination table is already created. This is
    because integer range partitioning and clustering of columns must be done
    when the table is created.
    Note2: If we run all 26 jobs in parallel BigQuery will be overwhelmed and
    jobs fail randomly, thus we use _MAX_NUM_CONCURRENT_AVRO_LOAD_JOBS limit.

    Args:
      avro_root_path: Location of AVRO files on Google Cloud Storage (GCS).
      output_table: Base table_name `__` + suffixes will be added to it.
      suffixes: List of table suffixes: `__chr1`, `__chr2`, ... `sample_info`.
      delete_empty_tables: Whether or not to delete tables with 0 rows loaded.
    """
    self._avro_root_path = avro_root_path
    project_id, dataset_id, table_id = bigquery_util.parse_table_reference(
        output_table)
    self._table_base_name = '{}.{}.{}'.format(project_id, dataset_id, table_id)

    self._num_load_jobs_retries = 0
    self._suffixes_to_load_jobs = {}  # type: Dict[str, bigquery.job.LoadJob]
    self._remaining_load_jobs = suffixes[:]

    self._delete_empty_tables = delete_empty_tables
    self._not_empty_suffixes = []

    self._client = bigquery.Client(project=project_id)

  def start_loading(self):
    # We run _MAX_NUM_CONCURRENT_AVRO_LOAD_JOBS load jobs in parallel.
    for _ in range(min(_MAX_NUM_CONCURRENT_AVRO_LOAD_JOBS,
                       len(self._remaining_load_jobs))):
      self._start_one_load_job(self._remaining_load_jobs.pop())

    self._monitor_load_jobs()
    return self._not_empty_suffixes

  def _start_one_load_job(self, suffix):
    # After issue #582 is resolved we can remove the create_disposition flag.
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO,
        create_disposition='CREATE_NEVER')
    uri = self._avro_root_path + suffix + '-*'
    table_id = bigquery_util.compose_table_name(self._table_base_name, suffix)
    load_job = self._client.load_table_from_uri(
        uri, table_id, job_config=job_config)
    self._suffixes_to_load_jobs.update({suffix: load_job})

  def _cancel_all_running_load_jobs(self):
    for load_job in self._suffixes_to_load_jobs.values():
      load_job.cancel()

  def _handle_failed_load_job(self, suffix, load_job):
    table_id = bigquery_util.compose_table_name(self._table_base_name, suffix)
    logging.warning('Failed to load AVRO to BigQuery table: %s', table_id)
    exception_str = ''
    if load_job.exception():
      exception_str = str(load_job.exception())
      logging.warning('Load job exception: %s', exception_str)
    if self._num_load_jobs_retries < bigquery_util.BQ_NUM_RETRIES:
      logging.warning('Retrying the failed job...')
      self._num_load_jobs_retries += 1
      time.sleep(300)
      self._start_one_load_job(suffix)
    else:
      logging.error('AVRO load jobs have failed more than BQ_NUM_RETRIES.')
      self._cancel_all_running_load_jobs()
      raise ValueError(
          'Failed to load AVRO to BigQuery table {} \n state: {} \n '
          'job_id: {} \n exception: {}.'.format(table_id, load_job.state,
                                                load_job.path, exception_str))
  def _monitor_load_jobs(self):
    # Waits until current jobs are done and then add remaining jobs one by one.
    while self._suffixes_to_load_jobs:
      time.sleep(60)
      processed_suffixes = list(self._suffixes_to_load_jobs.keys())
      for suffix in processed_suffixes:
        load_job = self._suffixes_to_load_jobs.get(suffix)
        if load_job.done():
          del self._suffixes_to_load_jobs[suffix]
          if load_job.state != 'DONE' or load_job.exception() is not None:
            self._handle_failed_load_job(suffix, load_job)
          else:
            self._delete_empty_table(suffix, load_job)
            if self._remaining_load_jobs:
              next_suffix = self._remaining_load_jobs.pop()
              self._start_one_load_job(next_suffix)

  def _delete_empty_table(self, suffix, load_job):
    api_repr_dic = load_job.destination.to_api_repr()
    output_table = '{}:{}.{}'.format(api_repr_dic['projectId'],
                                     api_repr_dic['datasetId'],
                                     api_repr_dic['tableId'])
    logging.info('%s rows was loaded to table: `%s`',
                 load_job.output_rows, output_table)
    if load_job.output_rows == 0:
      if self._delete_empty_tables:
        if bigquery_util.delete_table(output_table) == 0:
          logging.info('Table with 0 row was deleted: %s', output_table)
        else:
          logging.error('Not able to delete table with 0 row: %s', output_table)
      else:
        logging.info('Table with 0 added row is preserved: %s', output_table)
    else:
      self._not_empty_suffixes.append(suffix)
