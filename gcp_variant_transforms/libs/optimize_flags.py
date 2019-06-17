# Copyright 2019 Google LLC.
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

"""Util class used to optimize values for flags, based on provided input size.

If any of the flags were manually supplied during the command's invocation,
they will not be overriden.

The class uses 5 signals extracted from input, for flag adjustment:
 - estimated total number of variants.
 - estimated total number of samples.
 - estimated number of values (variant data for sample).
 - total size of the input.
 - amount of supplied files.
"""

import operator

from apache_beam.runners import runner  # pylint: disable=unused-import


class Dimensions(object):
  """Contains dimensions of the input data."""

  def __init__(self,
               line_count,  # type: int
               sample_count,  # type: int
               value_count,  # type: int
               files_size,  # type: int
               file_count,  # type: int
              ):
    # type: (...) -> None
    self.line_count = line_count
    self.sample_count = sample_count
    self.value_count = value_count
    self.files_size = files_size
    self.file_count = file_count


class Threshold(Dimensions):
  """Describes the limits the input needs to pass to enable a certain flag."""
  def __init__(self,
               line_count=None,  # type: int
               sample_count=None,  # type: int
               value_count=None,  # type: int
               files_size=None,  # type: int
               file_count=None,  # type: int
               custom_cond=None,  # type: bool
              ):
    super(Threshold, self).__init__(line_count,
                                    sample_count,
                                    value_count,
                                    files_size,
                                    file_count)
    self.custom_cond = custom_cond

  def hard_pass(self, input_dimensions, cond=operator.gt):
    # type: (Dimensions, Callable) -> bool
    """Verifies that all of set dimensions of the threshold are satisfied."""
    return (self.custom_cond(self, input_dimensions) if self.custom_cond else (
        (not self.line_count or
         cond(input_dimensions.line_count, self.line_count)) and
        (not self.sample_count or
         cond(input_dimensions.sample_count, self.sample_count)) and
        (not self.value_count or
         cond(input_dimensions.value_count, self.value_count)) and
        (not self.files_size or
         cond(input_dimensions.files_size, self.files_size)) and
        (not self.file_count or
         cond(input_dimensions.file_count, self.file_count))))

  def soft_pass(self, input_dimensions, cond=operator.gt):
    # type: (Dimensions, Callable) -> bool
    """Verifies that at least one of the set dimensions is satisfied."""
    return (self.custom_cond(self, input_dimensions) if self.custom_cond else (
        (self.line_count and
         cond(input_dimensions.line_count, self.line_count)) or
        (self.sample_count and
         cond(input_dimensions.sample_count, self.sample_count)) or
        (self.value_count and
         cond(input_dimensions.value_count, self.value_count)) or
        (self.files_size and
         cond(input_dimensions.files_size, self.files_size)) or
        (self.file_count and
         cond(input_dimensions.file_count, self.file_count))))


OPTIMIZE_FOR_LARGE_INPUTS_TS = Threshold(
    value_count=3000000000,
    file_count=50000
)
INFER_HEADERS_TS = Threshold(
    value_count=5000000000
)
INFER_ANNOTATION_TYPES_TS = Threshold(
    value_count=5000000000
)
NUM_BIGQUERY_WRITE_SHARDS_TS = Threshold(
    value_count=1000000000,
    files_size=500000000000
)
NUM_WORKERS_TS = Threshold(
    value_count=1000000000
)
COMP = Threshold(
    value_count=1000000000
)
SHARD_VARIANTS_TS = Threshold(
    value_count=10000000,
    custom_cond=(
        lambda ts, inp: (inp.value_count < ts.value_count or
                         (inp.line_count / inp.file_count) < 20000))
)

def _optimize_known_args(known_args, input_dimensions, supplied_args):
  if ('optimize_for_large_inputs' not in supplied_args and
      OPTIMIZE_FOR_LARGE_INPUTS_TS.soft_pass(input_dimensions)):
    known_args.optimize_for_large_inputs = True
  if ('infer_headers' not in supplied_args and
      INFER_HEADERS_TS.soft_pass(input_dimensions, operator.le)):
    known_args.infer_headers = True
  if ('num_bigquery_write_shards' not in supplied_args and
      NUM_BIGQUERY_WRITE_SHARDS_TS.soft_pass(input_dimensions)):
    known_args.num_bigquery_write_shards = 20
  if ('infer_annotation_types' not in supplied_args and
      INFER_ANNOTATION_TYPES_TS.soft_pass(input_dimensions, operator.le)):
    known_args.infer_annotation_types = True
  if ('shard_variants' not in supplied_args and
      SHARD_VARIANTS_TS.soft_pass(input_dimensions, operator.le)):
    known_args.shard_variants = False

def _optimize_pipeline_args(pipeline_args,
                            known_args,
                            input_dimensions,
                            supplied_args):
  if ('num_workers' not in supplied_args and
      NUM_WORKERS_TS.hard_pass(input_dimensions)):
    pipeline_args.num_workers = 100
  if ('num_workers' not in supplied_args and
      known_args.run_annotation_pipeline):
    pipeline_args.num_workers = 400
  if 'worker_machine_type' not in supplied_args:

    pipeline_args.worker_machine_type = 'n1-standard-4'

def optimize_flags(supplied_args, known_args, pipeline_args, input_dimensions):
  # type: (Namespace, List[str]) -> None
  _optimize_known_args(known_args, input_dimensions, supplied_args)
  _optimize_pipeline_args(pipeline_args,
                          known_args,
                          input_dimensions,
                          supplied_args)
