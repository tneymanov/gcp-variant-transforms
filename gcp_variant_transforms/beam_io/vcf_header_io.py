# Copyright 2017 Google Inc.  All Rights Reserved.
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

"""A source for reading VCF file headers."""

from __future__ import absolute_import

import collections
from functools import partial
from typing import Dict, Iterable, List  # pylint: disable=unused-import
from pysam import libcbcf


import apache_beam as beam
from apache_beam.io import filebasedsource
from apache_beam.io import range_trackers  # pylint: disable=unused-import
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.iobase import Read
from apache_beam.transforms import PTransform

from gcp_variant_transforms.beam_io import bgzf
from gcp_variant_transforms.beam_io import vcfio

LAST_HEADER_LINE_PREFIX = '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO'
HEADER_TYPES = ['Integer', 'Float', 'Flag', 'Character', 'String', '.']
HEADER_NUMBERS = ['A', 'R', 'G', '.']


class VcfHeaderFieldTypeConstants(object):
  """Constants for types from VCF header."""
  FLOAT = 'Float'
  INTEGER = 'Integer'
  STRING = 'String'
  FLAG = 'Flag'
  CHARACTER = 'Character'


class VcfParserHeaderKeyConstants(object):
  """Constants for header fields from the parser."""
  ID = 'id'
  NUM = 'num'
  TYPE = 'type'
  DESC = 'desc'
  SOURCE = 'source'
  VERSION = 'version'
  LENGTH = 'length'

def CreateInfoField(info_id,
                    number,
                    info_type,
                    description='',
                    source=None,
                    version=None):
  return VariantHeaderMetadataMock(
      info_id,
      number,
      info_type,
      description,
      {
          'Type': info_type,
          'Number': str(number),
          'Source': None if source is None else str(source),
          'Version': None if version is None else str(version)
      })

def CreateFormatField(info_id, number, info_type, description=''):
  return VariantHeaderMetadataMock(info_id, number, info_type, description,
                                   {'Type': info_type, 'Number': number})

# Mock of PySam VariantHeaderMetadata field
VariantHeaderMetadataMock = collections.namedtuple(
    'VariantHeaderMetadata', ['id', 'number', 'type', 'description', 'record'])

class VcfHeader(object):
  """Container for header data."""

  def __init__(self,
               infos=None,  # type: libcbcf.VariantHeaderMetadata
               filters=None,  # type: libcbcf.VariantHeaderMetadata
               alts=None,  # type: Dict[str, libcbcf.VariantHeaderRecord]
               formats=None,  # type: libcbcf.VariantHeaderMetadata
               contigs=None,  # type: libcbcf.VariantHeaderContigs
               samples=None,  # type: str
               file_path=None  # type: str
              ):
    # type: (...) -> None
    """Initializes a VcfHeader object.

    It keeps the order of values in the input dictionaries. Order is important
    in some fields like `contigs` and for ensuring order is unchanged in
    VCF->VCF pipelines.

    Args:
      infos: A dictionary mapping info keys to vcf info metadata values.
      filters: A dictionary mapping filter keys to vcf filter metadata values.
      alts: A dictionary mapping alt keys to vcf alt metadata values.
      formats: A dictionary mapping format keys to vcf format metadata values.
      contigs: A dictionary mapping contig keys to vcf contig metadata values.
      samples: A list of sample names.
      file_path: The full file path of the vcf file.
    """
    self.infos = self._get_infos_pysam(infos or {})
    self.filters = self._get_filters_pysam(filters or {})
    self.alts = self._get_alts_pysam(alts or {})
    self.formats = self._get_formats_pysam(formats or {})
    self.contigs = self._get_contigs_pysam(contigs or {})
    self.samples = self._get_samples_pysam(samples or '')
    self.file_path = file_path

  def __eq__(self, other):
    return (self.infos == other.infos and
            self.filters == other.filters and
            self.alts == other.alts and
            self.formats == other.formats and
            self.contigs == other.contigs)

  def __repr__(self):
    return ', '.join([str(header) for header in [self.infos,
                                                 self.filters,
                                                 self.alts,
                                                 self.formats,
                                                 self.contigs]])
  def _get_infos_pysam(self, infos):
    self.verify_infos(infos)
    results = collections.OrderedDict()
    for item in infos.items():
      result = collections.OrderedDict()
      result['id'] = item[0]
      result['num'] = item[1].number
      result['type'] = item[1].record['Type']
      result['desc'] = item[1].description
      # Pysam doesn't return these fields in info
      result['source'] = (item[1].record['Source'].strip("\"")
                          if 'Source' in item[1].record and
                          item[1].record['Source'] is not None else None)
      result['version'] = (item[1].record['Version'].strip("\"")
                           if 'Version' in item[1].record and
                           item[1].record['Version'] is not None else None)
      results[item[0]] = result
    return results

  def _get_filters_pysam(self, filters):
    results = collections.OrderedDict()
    for item in filters.items():
      result = collections.OrderedDict()
      result['id'] = item[0]
      result['desc'] = item[1].description
      results[item[0]] = result
    # PySAM adds default PASS value to its filters
    if 'PASS' in results:
      del results['PASS']
    return results

  def _get_alts_pysam(self, alts):
    results = collections.OrderedDict()
    for item in alts.items():
      result = collections.OrderedDict()
      result['id'] = item[0]
      result['desc'] = item[1]['Description'].strip("\"")
      results[item[0]] = result
    return results

  def _get_formats_pysam(self, formats):
    results = collections.OrderedDict()
    for item in formats.items():
      result = collections.OrderedDict()
      result['id'] = item[0]
      result['num'] = item[1].number
      result['type'] = item[1].record['Type']
      result['desc'] = item[1].description
      results[item[0]] = result
    return results

  def _get_contigs_pysam(self, contigs):
    results = collections.OrderedDict()
    for item in contigs.items():
      result = collections.OrderedDict()
      result['id'] = item[0]
      result['length'] = item[1].length
      results[item[0]] = result
    return results

  def _get_samples_pysam(self, sample_line):
    sample_tags = sample_line.split('\t')
    if len(sample_tags) > 9:
      return sample_tags[9:]
    else:
      return []

  def verify_infos(self, fields):
    for k, v in fields.iteritems():
      # ID, Description, Type and Number are mandatory fields.
      if not k:
        raise ValueError('Corrupt ID at header line {}.'.format(v.id))
      if v.description is None:
        raise ValueError(
            'Corrupt Description at header line {}.'.format(v.id))
      # Type can only be Integer, Float, Flag, Character or String
      if not v.type or v.record['Type'] not in HEADER_TYPES:
        raise ValueError('Corrupt Type at header line {}'.format(v.id))
      # Number can only be a number or one of 'A', 'R', 'G' and '.'.
      if not v.record['Number'] or (
          v.record['Number'] not in HEADER_NUMBERS and
          not v.record['Number'].isdigit()):
        raise ValueError('Unknown Number at header line {}.'.format(v.id))


class VcfHeaderSource(filebasedsource.FileBasedSource):
  """A source for reading VCF file headers.

  Parses VCF files.
  """

  def __init__(self,
               file_pattern,
               compression_type=CompressionTypes.AUTO,
               validate=True,):
    # type: (str, str, bool) -> None
    super(VcfHeaderSource, self).__init__(file_pattern,
                                          compression_type=compression_type,
                                          validate=validate,
                                          splittable=False)
    self._compression_type = compression_type

  def read_records(
      self,
      file_path,  # type: str
      unused_range_tracker,  # type: range_trackers.UnsplittableRangeTracker
      ):
    # type: (...) -> Iterable[VcfHeader]
    header = libcbcf.VariantHeader()
    lines = self._read_headers(file_path)
    sample_line = None
    header.add_line('##fileformat=VCFv4.0')
    for line in lines:
      if line.startswith('##'):
        header.add_line(line.strip())
      elif line.startswith('#'):
        sample_line = line
      elif line:
        if not sample_line:
          raise ValueError('Header line is missing')
      else:
        if not sample_line:
          sample_line = LAST_HEADER_LINE_PREFIX

    yield VcfHeader(infos=header.info,
                    filters=header.filters,
                    alts=header.alts,
                    formats=header.formats,
                    contigs=header.contigs,
                    samples=sample_line,
                    file_path=file_path)

  def _read_headers(self, file_path):
    with self.open_file(file_path) as file_to_read:
      record = None
      while True:
        record = file_to_read.readline()
        while record and not record.strip():  # Skip empty lines.
          record = file_to_read.readline()
        if record and record.startswith('#'):
          yield record.strip()
        else:
          break
      yield record.strip()

  def open_file(self, file_path):
    if self._compression_type == CompressionTypes.GZIP:
      return bgzf.open_bgzf(file_path)
    else:
      return FileSystems.open(file_path,
                              compression_type=self._compression_type)


class ReadVcfHeaders(PTransform):
  """A PTransform for reading the header lines of VCF files.

  Parses VCF files.
  """

  def __init__(
      self,
      file_pattern,  # type: str
      compression_type=CompressionTypes.AUTO,  # type: str
      validate=True,  # type: bool
      **kwargs  # type: **str
      ):
    # type: (...) -> None
    """Initialize the :class:`ReadVcfHeaders` transform.

    Args:
      file_pattern: The file path to read from either as a single file or a glob
        pattern.
      compression_type: Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
      validate: Flag to verify that the files exist during the pipeline creation
        time.
    """
    super(ReadVcfHeaders, self).__init__(**kwargs)
    self._source = VcfHeaderSource(
        file_pattern,
        compression_type,
        validate=validate)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)


def CreateVcfHeaderSource(
    file_pattern=None,
    compression_type=None):
  return VcfHeaderSource(file_pattern=file_pattern,
                         compression_type=compression_type)


class ReadAllVcfHeaders(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading the
  header lines of :class:`~apache_beam.pvalue.PCollection` of VCF files.

  Reads a :class:`~apache_beam.pvalue.PCollection` of VCF files or file patterns
  and produces a PCollection :class:`VcfHeader` objects.

  This transform should be used when reading from massive (>70,000) number of
  files.
  """

  DEFAULT_DESIRED_BUNDLE_SIZE = 64 * 1024 * 1024  # 64MB

  def __init__(
      self,
      desired_bundle_size=DEFAULT_DESIRED_BUNDLE_SIZE,
      compression_type=CompressionTypes.AUTO,
      **kwargs):
    # type: (int, str, **str) -> None
    """Initialize the :class:`ReadAllVcfHeaders` transform.

    Args:
      desired_bundle_size: Desired size of bundles that should be generated when
        splitting this source into bundles. See
        :class:`~apache_beam.io.filebasedsource.FileBasedSource` for more
        details.
      compression_type: Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
    """
    super(ReadAllVcfHeaders, self).__init__(**kwargs)
    source_from_file = partial(
        CreateVcfHeaderSource,
        compression_type=compression_type)
    self._read_all_files = filebasedsource.ReadAllFiles(
        False,  # splittable (we are just reading the headers)
        CompressionTypes.AUTO, desired_bundle_size,
        0,  # min_bundle_size
        source_from_file)

  def expand(self, pvalue):
    return pvalue | 'ReadAllFiles' >> self._read_all_files


class HeaderTypeConstants(object):
  INFO = 'INFO'
  FILTER = 'FILTER'
  ALT = 'ALT'
  FORMAT = 'FORMAT'
  CONTIG = 'contig'


class _HeaderFieldKeyConstants(object):
  ID = 'ID'
  NUMBER = 'Number'
  TYPE = 'Type'
  DESCRIPTION = 'Description'
  SOURCE = 'Source'
  VERSION = 'Version'
  LENGTH = 'length'


class WriteVcfHeaderFn(beam.DoFn):
  """A DoFn for writing VCF headers to a file."""

  HEADER_TEMPLATE = '##{}=<{}>\n'
  FINAL_HEADER_LINE = '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT\n'

  def __init__(self, file_path):
    # type: (str) -> None
    self._file_path = file_path
    self._file_to_write = None

  def process(self, header, vcf_version_line=None):
    # type: (VcfHeader, str) -> None
    with FileSystems.create(self._file_path) as self._file_to_write:
      if vcf_version_line:
        self._file_to_write.write(vcf_version_line)
      self._write_headers_by_type(HeaderTypeConstants.INFO, header.infos)
      self._write_headers_by_type(HeaderTypeConstants.FILTER, header.filters)
      self._write_headers_by_type(HeaderTypeConstants.ALT, header.alts)
      self._write_headers_by_type(HeaderTypeConstants.FORMAT, header.formats)
      self._write_headers_by_type(HeaderTypeConstants.CONTIG, header.contigs)
      self._file_to_write.write(self.FINAL_HEADER_LINE)

  def _write_headers_by_type(self, header_type, headers):
    # type: (str, Dict[str, Dict[str, Union[str, int]]]) -> None
    """Writes all VCF headers of a specific type.

    Args:
      header_type: The type of `headers` (e.g. INFO, FORMAT, etc.).
      headers: Each value of headers is a dictionary that describes a single VCF
        header line.
    """
    for header in headers.values():
      self._file_to_write.write(
          self._to_vcf_header_line(header_type, header))

  def _to_vcf_header_line(self, header_type, header):
    # type: (str, Dict[str, Union[str, int]]) -> str
    """Formats a single VCF header line.

    Args:
      header_type: The VCF type of `header` (e.g. INFO, FORMAT, etc.).
      header: A dictionary mapping header field keys (e.g. id, desc, etc.) to
        their corresponding values for the header line.

    Returns:
      A formatted VCF header line.
    """
    formatted_header_values = self._format_header(header)
    return self.HEADER_TEMPLATE.format(header_type, formatted_header_values)

  def _format_header(self, header):
    # type: (Dict[str, Union[str, int]]) -> str
    """Formats all key, value pairs that describe the header line.

    Args:
      header: A dictionary mapping header field keys (e.g. id, desc, etc.) to
        their corresponding values for the header line.

    Returns:
      A formatted string composed of header keys and values.
    """
    formatted_values = []
    for key, value in header.iteritems():
      if self._should_include_key_value(key, value):
        formatted_values.append(self._format_header_key_value(key, value))
    return ','.join(formatted_values)

  def _should_include_key_value(self, key, value):
    return value is not None or (key != 'source' and key != 'version')

  def _format_header_key_value(self, key, value):
    # type: (str, Union[str, int]) -> str
    """Formats a single key, value pair in a header line.

    Args:
      key: The key of the header field (e.g. num, desc, etc.).
      value: The header value corresponding to the key in a specific
        header line.

    Returns:
      A formatted key, value pair for a VCF header line.
    """
    key = self._format_header_key(key)
    if value is None:
      value = vcfio.MISSING_FIELD_VALUE
    elif key == _HeaderFieldKeyConstants.NUMBER:
      value = self._format_number(value)
    elif (key == _HeaderFieldKeyConstants.DESCRIPTION
          or key == _HeaderFieldKeyConstants.SOURCE
          or key == _HeaderFieldKeyConstants.VERSION):
      value = self._format_string_value(value)
    return '{}={}'.format(key, value)

  def _format_header_key(self, key):
    if key == VcfParserHeaderKeyConstants.ID:
      return _HeaderFieldKeyConstants.ID
    elif key == VcfParserHeaderKeyConstants.NUM:
      return _HeaderFieldKeyConstants.NUMBER
    elif key == VcfParserHeaderKeyConstants.DESC:
      return _HeaderFieldKeyConstants.DESCRIPTION
    elif key == VcfParserHeaderKeyConstants.TYPE:
      return _HeaderFieldKeyConstants.TYPE
    elif key == VcfParserHeaderKeyConstants.SOURCE:
      return _HeaderFieldKeyConstants.SOURCE
    elif key == VcfParserHeaderKeyConstants.VERSION:
      return _HeaderFieldKeyConstants.VERSION
    elif key == VcfParserHeaderKeyConstants.LENGTH:
      return _HeaderFieldKeyConstants.LENGTH
    else:
      raise ValueError('Invalid VCF header key {}.'.format(key))

  def _format_number(self, number):
    # type: (int) -> Optional[str]
    """Returns the string representation of number field.

    Args:
      number: An integer representing the number of fields in INFO

    Returns:
      A string representation of field_count.

    Raises:
      ValueError: if the number is not valid.
    """
    if number is None:
      return None
    if number >= 0:
      return str(number)
    else:
      raise ValueError('Invalid value for number: {}'.format(number))

  def _format_string_value(self, value):
    # type: (str, unicode) -> str
    if isinstance(value, unicode):
      return '"{}"'.format(value.encode('utf-8'))
    return '"{}"'.format(value)


class WriteVcfHeaders(PTransform):
  """A PTransform for writing VCF header lines."""

  def __init__(self, file_path):
    # type: (str) -> None
    self._file_path = file_path

  def expand(self, pcoll):
    return pcoll | beam.ParDo(WriteVcfHeaderFn(self._file_path))
