# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Data Commons Bio Data API Extension.

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import OrderedDict
from types import MethodType
from .datacommons import DCFrame
from . import utils

# Root url of the ENCODE Project experiment targets.
_ENCODE_TARGET_URL = 'https://www.encodeproject.org/targets/{}'

# Default maximum number of rows to return in a query
_MAX_ROWS = 100

# Default bedline properties to query for. Each line has the new column name,
# the property to query for, and the type mapped to by the property.
DEFAULT_BEDLINE_PROPS = [
    ('Chromosome', 'chromosome', 'Text'),
    ('StartPos', 'chromosomeStart', 'Integer'),
    ('EndPos', 'chromosomeEnd', 'Integer'),
    ('BedName', 'bedName', 'Text'),
    ('BedScore', 'bedScore', 'Integer'),
    ('Strand', 'chromosomeStrand', 'Text')
]


def BioExtension(frame):
  """ The DataCommons bio extension API. """
  frame.get_experiments = MethodType(get_experiments, frame)
  frame.get_bed_files = MethodType(get_bed_files, frame)
  frame.get_bed_lines = MethodType(get_bed_lines, frame)
  return frame

def get_experiments(self, new_col_name, **kwargs):
  """ Creates a column of experiments in the dataframe.

  See argument section for valid keyword arguments.

  Args:
    new_col_name: The name of the new column created
    assay_category: A list of assay categories investigated by the experiment.
    assay_target: A list of assay targets investigated by the experiment.
    bio_class: A list of biosample classes to filter by. This argument must be
      provided with a biosample type. The i-th biosample class with the i-th
      biosample type describes one biosample to filter for.
    bio_term: A list of biosample types. This argument must be provided with
      a biosample class. The i-th biosample class with the i-th biosample class
      describes one biosample to filter for.
    lab_name: A list of lab names that published the experiment.
    rows: The maximum number of rows to query for
  """
  if new_col_name in self._dataframe:
    raise ValueError('{} is already a column.'.format(new_col_name))
  if ('bio_class' in kwargs) != ('bio_term' in kwargs):
    raise ValueError('Only one of bio_class or bio_term is specified.')
  if 'bio_class' in kwargs and len(kwargs['bio_class']) != len(kwargs['bio_term']):
    raise ValueError('Number of bio_class and bio_term parameters must match.')

  # Get the query variable and type hint
  new_col_var = '?' + new_col_name.replace(' ', '_')
  labels = {new_col_var: new_col_name}
  type_hint = {new_col_var: 'EncodeExperiment'}

  # Get the row limit
  rows = _MAX_ROWS
  if 'rows' in kwargs:
    rows = kwargs['rows']

  # Row selection and table post processing needed if bio summary provided.
  select = None
  process = None

  # Construct the query
  query = utils.DatalogQuery()
  query.add_variable(new_col_var)
  query.add_constraint(new_col_var, 'typeOf', 'EncodeExperiment')
  if 'assay_category' in kwargs:
    categories = ['"{}"'.format(val) for val in kwargs['assay_category']]
    query.add_constraint(new_col_var, 'assaySlims', categories)
  if 'assay_target' in kwargs:
    target_urls = [_ENCODE_TARGET_URL.format(target) in kwargs['assay_target']]
    targets = ['"{}"'.format(url) for url in target_urls]
    query.add_constraint(new_col_var, 'target', targets)
  if 'assembly' in kwargs:
    assemblies = ['"{}"'.format(val) for val in kwargs['assembly']]
    query.add_constraint(new_col_var, 'assembly', assemblies)
  if 'bio_class' in kwargs and 'bio_term' in kwargs:
    classes = ['"{}"'.format(val) for val in kwargs['bio_class']]
    terms = ['"{}"'.format(val) for val in kwargs['bio_term']]
    query.add_variable('?bioClass', '?bioTerm')
    query.add_constraint(new_col_var, 'biosampleOntology', '?bioNode')
    query.add_constraint('?bioNode', 'classification', classes)
    query.add_constraint('?bioNode', 'termName', terms)
    query.add_constraint('?bioNode', 'classification', '?bioClass')
    query.add_constraint('?bioNode', 'termName', '?bioTerm')

    # Specify select and process functions to filter for biosample class and
    # terms. This enforces the paired-ness of term and class
    select = select_biosample_summary('?bioClass', '?bioTerm', classes, terms)
    process = delete_column('?bioClass', '?bioTerm')
  if 'lab_name' in kwargs:
    lab_names = ['"{}"'.format(name) for name in kwargs['lab_name']]
    query.add_constraint(new_col_var, 'lab', '?labNode')
    query.add_constraint('?labNode', 'name', lab_names)

  # Perform the query and merge
  new_frame = DCFrame(datalog_query=query,
                      labels=labels,
                      type_hint=type_hint,
                      select=select,
                      process=process,
                      rows=rows)
  self.merge(new_frame)

def get_bed_files(self, seed_col_name, new_col_name, **kwargs):
  """ Adds a column of bed file DCIDs associated with a column of experiments.

  If the seed_col_type is Text, the function will infer the column to contain
  lab names.

  Args:
    seed_col_name: The name of the experiment column
    new_col_name: The name of the new column created
    lab_name: Names of labs publishing the bed files
  """
  if seed_col_name not in self._dataframe:
    raise ValueError('{} is not a column in the frame.'.format(seed_col_name))
  if new_col_name in self._dataframe:
    raise ValueError('{} is already a column.'.format(new_col_name))

  # Get the row limit
  rows = _MAX_ROWS
  if 'rows' in kwargs:
    rows = kwargs['rows']

  # Get the variables
  seed_col_var = '?' + seed_col_name.replace(' ', '')
  new_col_var = '?' + new_col_name.replace(' ', '')
  labels = {seed_col_var: seed_col_name, new_col_var: new_col_name}

  # Get typing information
  seed_col_type = self._col_types[seed_col_name]
  new_col_type = 'EncodeBedFile'
  type_hint = {seed_col_var: seed_col_type, new_col_var: new_col_type}

  # Construct the query
  query = utils.DatalogQuery()
  query.add_variable(seed_col_var, new_col_var)
  query.add_constraint(new_col_var, 'fromExperiment', '?experimentNode')

  # Add constraints based on if the seed is a column of experiments or lab names
  seed_col = list(self._dataframe[seed_col_name])
  if not seed_col:
    raise ValueError('Seed column {} is empty.'.format(seed_col_name))
  if seed_col_type == 'EncodeExperiment':
    query.add_constraint('?experimentNode', 'dcid', seed_col_var)
    query.add_constraint('?experimentNode', 'dcid', seed_col)
  elif seed_col_type == 'Text':
    lab_names = ['"{}"'.format(name) for name in seed_col]
    query.add_constraint(new_col_var, 'lab', '?labNode')
    query.add_constraint('?labNode', 'name', seed_col_var)
    query.add_constraint('?labNode', 'name', lab_names)
  else:
    raise ValueError('Invalid seed column type {} for column {}.'
        .format(seed_col_type, seed_col_name))

  # Perform the query and merge
  new_frame = DCFrame(datalog_query=query, labels=labels, type_hint=type_hint, rows=rows)
  self.merge(new_frame)

def get_bed_lines(self, seed_col_name, prop_info=DEFAULT_BEDLINE_PROPS, **kwargs):
  """ Adds a column of bed file DCIDs associated with a column of experiments.

  See argument section for valid keyword arguments.

  Args:
    seed_col_name: The name of the experiment column.
    prop_info: A list of tuples specifying which properties in a BedLine to
      query for. Each tuple contains:
      (1) The name of the new column.
      (2) The property to query for.
      (3) The type mapped to by the property.
      See DEFAULT_BEDLINE_PROPS for more details
    chromosome: A list of chromosomes to filter for.
    start_pos: A list of start positions to filter measurements for. This must
      be provided with end_pos with a list of the same length. The i-th start
      position with the i-th end position defines one interval to filter for.
    end_pos: A list of end positions to filter measurements for. This must be
      provided with start_pos with a list of the same length. The i-th start
      position with the i-th end position defines one interval to filter for.
  """
  if seed_col_name not in self._dataframe:
    raise ValueError('{} is not a column in the frame.'.format(seed_col_name))
  if ('start_pos' in kwargs) != ('end_pos' in kwargs):
    raise ValueError('Must provide both start_pos and end_pos.')
  if 'start_pos' in kwargs and len(kwargs['start_pos']) != len(kwargs['end_pos']):
    raise ValueError('Length of start_pos must equal length of end_pos.')

  # Get the row limit
  rows = _MAX_ROWS
  if 'rows' in kwargs:
    rows = kwargs['rows']

  # Get seed column information
  seed_col = list(self._dataframe[seed_col_name])
  seed_col_var = '?' + seed_col_name.replace(' ', '')
  seed_col_type = self._col_types[seed_col_name]

  if not seed_col:
    raise ValueError('Seed column {} is empty.'.format(seed_col_name))

  # Create the query
  query = utils.DatalogQuery()
  query.add_variable(seed_col_var)
  query.add_constraint('?bedFileNode', 'dcid', seed_col)
  query.add_constraint('?bedFileNode', 'dcid', seed_col_var)
  query.add_constraint('?bedLineNode', 'typeOf', 'BedLine')
  query.add_constraint('?bedLineNode', 'fromBedFile', '?bedFileNode')

  # If filtering by a specific parameter, need to store which variable is
  # querying for the parameter being filtered by.
  chromosome_var = None
  start_pos_var = None
  end_pos_var = None

  # Add specific properties to query from the bed line
  labels, type_hint = {}, {}
  for prop_line in prop_info:
    query_var = '?' + prop_line[0].replace(' ', '')
    labels[query_var] = prop_line[0]
    type_hint[query_var] = prop_line[2]

    # Add variable and constraint
    query.add_variable(query_var)
    query.add_constraint('?bedLineNode', prop_line[1], query_var)

    # Store variable if filtering by appropriate parameter
    if 'chromosome' in kwargs and prop_line[1] == 'chromosome':
      chromosome_var = query_var
    if 'start_pos' in kwargs and prop_line[1] == 'chromosomeStart':
      start_pos_var = query_var
    if 'end_pos' in kwargs and prop_line[1] == 'chromosomeEnd':
      end_pos_var = query_var

  # Create filters based on the parameters. Edit the query if certain variables
  # are not being queried for and post process the result as necessary.
  select, process, select_funcs, drop_cols = None, None, [], []
  if 'chromosome' in kwargs:
    if not chromosome_var:
      chromosome_var = '?chromosome'
      query.add_variable(chromosome_var)
      query.add_constraint('?bedLineNode', 'chromosome', chromosome_var)
      drop_cols.append(chromosome_var)
    select_funcs.append(select_chromosome(chromosome_var, kwargs['chromosome']))
  if 'start_pos' in kwargs:
    if not start_pos_var:
      start_pos_var = '?chromStart'
      query.add_variable(start_pos_var)
      query.add_constraint('?bedLineNode', 'chromosomeStart', start_pos_var)
      drop_cols.append(start_pos_var)
    if not end_pos_var:
      end_pos_var = '?chromEnd'
      query.add_variable(end_pos_var)
      query.add_constraint('?bedLineNode', 'chromosomeEnd', end_pos_var)
      drop_cols.append(end_pos_var)
    select_funcs.append(
        select_chrom_pos(start_pos_var,
                         end_pos_var,
                         kwargs['start_pos'],
                         kwargs['end_pos']))

  # If filters were specified, compose the filters and add a post processor if
  # necessary.
  if select_funcs:
    select = compose_select(*select_funcs)
    if drop_cols:
      process = delete_column(*drop_cols)

  # Perform the query and merge
  new_frame = DCFrame(datalog_query=query,
                      labels=labels,
                      type_hint=type_hint,
                      select=select,
                      process=process,
                      rows=rows)
  self.merge(new_frame)

# ----------------------------- HELPER FUNCTIONS ------------------------------

def select_biosample_summary(bio_class_col, bio_term_col, bio_classes, bio_terms):
  """ Returns a filter function which filters for appropriate summaries.

  When bio_class and bio_term are specified for querying experiments, the
  returned experiments must match the i-th bio_class and i-th bio term for some
  given i. It cannot match a bio_term and bio_class at different indices in the
  given parameters.

  Args:
    bio_class_col: The name of the bio_class column
    bio_term_col: The name of the bio_term column
    bio_classes: Allowed biosample classes
    bio_terms: Allowed biosample types

  Returns:
    A function filtering the dataframe for appropriately ordered bio summaries.
  """
  def select(row):
    for bio_class, bio_term in zip(bio_classes, bio_terms):
      bio_class = bio_class.replace('"', '')
      bio_term = bio_term.replace('"', '')
      if row[bio_class_col] == bio_class and row[bio_term_col] == bio_term:
        return True
    return False
  return select

def select_chromosome(chrom_col, chromosomes):
  """ Returns a filter function which filters for given chromosomes.

  Args:
    chrom_col: Name of the column containing chromosome information.
    chromosomes: Allowed chromosomes to filter for.

  Returns:
    A function filtering for the given chromosomes.
  """
  def select(row):
    return row[chrom_col] in chromosomes
  return select

def select_chrom_pos(start_pos_col, end_pos_col, start_pos, end_pos):
  """ Returns a function which filters for chromosomes in the given intervals.

  Args:
    start_pos_col: Name of column containing the start position
    end_pos_col: Name of column containing the end position
    start_pos: Allowed start positions
    end_pos: Allowed end positions

  Returns:
    A function filtering for measurements in the given intervals.
  """
  def select(row):
    for start, end in zip(start_pos, end_pos):
      if row[start_pos_col] and row[end_pos_col]\
        and int(row[start_pos_col]) >= start\
        and int(row[end_pos_col]) <= end:
        return True
    return False
  return select

def compose_select(*select_funcs):
  """ Returns a filter function composed of the given selectors.

  Args:
    select_funcs: Functions to compose.

  Returns:
    A filter function which returns True iff all select_funcs return True.
  """
  def select(row):
    return all(select_func(row) for select_func in select_funcs)
  return select

def delete_column(*cols):
  """ Returns a function that deletes the given column from a frame.

  Args:
    cols: Columns to delete from the data frame.

  Returns:
    A function that deletes columns in the given Pandas DataFrame.
  """
  def process(pd_frame):
    for col in cols:
      if col in pd_frame:
        pd_frame = pd_frame.drop(col, axis=1)
    return pd_frame
  return process
