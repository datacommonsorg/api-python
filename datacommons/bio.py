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

TODO(antaresc): Implement ability to take ORs
TODO(antaresc): Implement expand
TODO(antaresc): When searching for BedFiles implement the ability to either
                specify experiment or Lab
TODO(antaresc): Maybe add a row-post process feature into _add_data... I forget
                what the use case for this is though.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from . import _auth

from collections import defaultdict, OrderedDict

import pandas as pd

_PARENT_TYPES = {
  'containedInPlace': 'Place'
}

_CLIENT_ID = ('381568890662-ff9evnle0lj0oqttr67p2h6882d9ensr.apps.googleusercontent.com')
_CLIENT_SECRET = '77HJA4S5m48Z98UKkW_o-jAY'
_API_ROOT = 'https://datcom-api-sandbox.appspot.com'

class BioExtension(object):
    """ The DataCommons Bio API extension.

    Fields:
      _col_type: A map from column name to its DataCommons type.
      _dataframe: A Pandas DataFrame that stores the data queried.
    """

    def __init__(self,
                 client_id=_CLIENT_ID,
                 client_secret=_CLIENT_SECRET,
                 api_root=_API_ROOT):
        # Connect to the DataCommons query service
        self._service = _auth.do_auth(client_id, client_secret, api_root)
        response = self._service.get_prop_type(body={}).execute()
        self._prop_type = defaultdict(dict)
        self._inv_prop_type = defaultdict(dict)
        for t in response.get('type_info', []):
            self._prop_type[t['node_type']][t['prop_name']] = t['prop_type']
            if t['prop_type'] != 'Text':
                self._inv_prop_type[t['prop_type']][t['prop_name']] = t['node_type']

        # Create view fields
        self._col_type = {}
        self._dataframe = pd.DataFrame()

        # Set to initialized
        self._inited = True

    def clear(self):
        """ Clears all the data stored in this extension. """
        self._col_type = {}
        self._dataframe = pd.DataFrame()

    def expand(self, arc_name, seed_col_name, new_col_name, outgoing=True, max_rows=100):
        """ TODO Comment """
        assert self._inited, 'Initialization was unsuccessful, cannot execute query'

        if new_col_name in self._dataframe:
            col_names = ', '.join(['"{}"'.format(col) for col in self._dataframe])
            msg = '"{}" is currently a column name. Please choose one that does not include {}'.format(col_names)
            raise ValueError(msg)

        # Get the seed column information
        seed_col = self._resolve_parameter(seed_col_name)
        seed_col_type = self._col_type[seed_col_name]
        assert seed_col_type != 'Text', 'Parent entity should not be Text'

        # Determine the new column type
        if outgoing:
            new_col_type = 'Text'
            if arc_name in self._prop_type[seed_col_type]:
                new_col_type = self._prop_type[seed_col_type][arc_name]
        else:
            if arc_name in self._inv_prop_type[seed_col_type]:
                new_col_type = self._inv_prop_type[seed_col_type][arc_name]
            elif arc_name in _PARENT_TYPES:
                new_col_type = _PARENT_TYPES[arc_name]
            else:
                raise ValueError('{} does not have incoming property {}'.format(seed_col_type, arc_name))

        # Get list of DCIDs
        dcids = ' '.join(seed_col).strip()
        if not dcids:
            self._dataframe[new_col_name] = ''
            self._col_type[new_col_name] = new_col_type
            return

        # Construct the query
        seed_col_var = '?{}'.format(seed_col_name.replace(' ', '_'))
        new_col_var = '?{}'.format(new_col_name.replace(' ', '_'))
        constraints = [
            'typeOf ?node {}'.format(seed_col_type),
            'dcid ?node {}'.format(dcids),
            'dcid ?node {}'.format(seed_col_var),
        ]
        if outgoing:
            constraints.append('{} ?node {}'.format(arc_name, new_col_var))
        else:
            constraints.append('{} {} ?node'.format(arc_name, new_col_var))

        # Add the data to the frame
        self._add_data([new_col_var], [new_col_name], [new_col_type], constraints, max_rows, seed_col_var=seed_col_var, seed_col_name=seed_col_name)

    def get_experiments(self,
                        assay_category=None,
                        assay_target=None,
                        assembly=None,
                        bio_class=None,
                        bio_term=None,
                        lab_name=None,
                        max_rows=100):
        """ Adds a column of Experiment dcids into the current data frame.

        Args:
          assay_category:
          assay_target:
          bio_class:
          bio_term:
          lab_name:
          max_rows: the maximum number of rows.

        Returns:
        """
        assert self._inited, 'Initialization was unsuccessful, cannot execute query'

        if (bio_class is None) != (bio_term is None):
            raise ValueError('Only one of bio_class or bio_term is specified.')

        # Construct the query data
        new_col_var = ['?experiment']
        new_col_name = ['Experiment']
        new_col_type = ['EncodeExperiment']
        constraints = [
            'typeOf ?experimentNode EncodeExperiment',
            'dcid ?experimentNode ?experiment'
        ]
        if assay_category:
            assays = self._format_arg_range(assay_category)
            constraints.append('assaySlims ?experimentNode {}'.format(assays))
        if assay_target:
            pass # TODO(antaresc): implement
        if assembly:
            assemblies = self._format_arg_range(assembly)
            constraints.append('assembly ?experimentNode {}'.format(assemblies))
        if bio_class and bio_term:
            classes = self._format_arg_range(bio_class)
            terms = self._format_arg_range(bio_term)
            constraints += [
                'biosampleOntology ?experimentNode ?biosample',
                'classification ?biosample {}'.format(classes),
                'termName ?biosample {}'.format(terms)
            ]
        if lab_name:
            names = self._format_arg_range(lab_name)
            constraints += [
                'lab ?experimentNode ?labNode',
                'name ?labNode {}'.format(names)
            ]

        # Add the query contents to the data frame
        self._add_data(new_col_var, new_col_name, new_col_type, constraints, max_rows)

    def get_bed_files(self, experiment=None, lab_name=None, max_rows=100):
        """ Adds a column of BedFile dcids into the current data frame.

        Args:
          experiment: Either a column name containing experiment DCIDs or a list
            of experiment DCIDs. The added BedFiles have a property "experiment"
            pointing towards the provided DCIDs.
          lab_name: Filter the BedFiles by the name of the lab it is sourced
            from.
          max_rows: the maximum number of rows.

        Raise:
          ValueError: if an input provided is not valid.
        """
        assert self._inited, 'Initialization was unsuccessful, cannot execute query'

        if not experiment and not lab_name:
            raise ValueError('One of experiment or lab_name must be specified.')

        # Convert the parameters into appropriate lists
        seed_col_var, seed_col_name = None, None
        if experiment:
            if isinstance(experiment, str):
                seed_col_var = '?experiment'
                seed_col_name = 'Experiment'
            experiment = self._resolve_parameter(experiment)
        if lab_name:
            lab_name = self._resolve_parameter(lab_name)

        # Construct the query
        new_col_var = ['?bedFile']
        new_col_name = ['BedFile']
        new_col_type = ['EncodeBedFile']
        constraints = []
        if experiment is not None:
            constraints += [
                'experiment ?bedFile ?experimentNode',
                'dcid ?experimentNode {}'.format(' '.join(experiment)),
                'dcid ?experimentNode ?experiment',
            ]
        # TODO(antaresc): Fix this when query has been repaired
        # - The issue is that you have to specify the type when you query for
        #   by lab since lab is defined for EncodeExperiment and EncodeBedFile
        # if lab_name is not None:
        #     names = self._format_arg_range(lab_name)
        #     constraints += [
        #         'lab ?bedFile ?labNode',
        #         'name ?labNode {}'.format(names)
        #     ]

        # Add the query contents to the data frame
        self._add_data(new_col_var, new_col_name, new_col_type, constraints, max_rows, seed_col_var=seed_col_var, seed_col_name=seed_col_name)

    def get_bed_lines(self,
                      bed_source,
                      chromosome=None,
                      start_pos=None,
                      end_pos=None,
                      max_rows=100):
        """ Adds BedLines into the current the current data frame.

        Creates new columns holding Bed file lines in the current data frame.
        This includes the chromosome name, start position, end position, line
        name, bed score, and strand measured by the bed line in the specified
        source bed file.

        Optionally, BedLines can be filtered by the name of the chromosome the
        line is measuring or the range of positions the measurement was taken
        at. Specify start_pos and end_pos to match measurements that were
        taken within the range inclusive.

        Args:
          bed_source: Either a column name containing BedFile DCIDs or a list
            of BedFile DCIDs. The added BedLines have a property "bedfile"
            pointing towards the provided DCIDs.
          chromosome: Either a string or a list of strings representing
            chromosome names to filter returned BedLines for.
          start_pos: The starting position of where the measurements were taken
            in the given chromosome. This must be specified in conjunction with
            end_pos.
          end_pos: The ending position of where the measurements were taken
            in the given chromosome. This must be specified in conjunction with
            start_pos.
          max_rows: the maximum number of rows.

        Raise:
          ValueError: if an input provided is not valid.
        """
        assert self._inited, 'Initialization was unsuccessful, cannot execute query'

        if start_pos is not None and end_pos is not None and len(start_pos) != len(end_pos):
            raise ValueError('Lengths of start_pos and end_pos mismatch.')

        # Verify the contents of bed_source
        bed_source = self._resolve_parameter(bed_source)

        # Construct the query
        dcids = ' '.join(bed_source)
        new_col_var = ['?chrom', '?chromStart', '?chromEnd', '?name', '?score', '?strand']
        new_col_name = ['Chromosome', 'Start', 'End', 'BedName', 'BedScore', 'Strand']
        new_col_type = ['Text', 'Integer', 'Integer', 'Text', 'Integer', 'Text']
        constraints = [
            'dcid ?bedFileNode {}'.format(dcids),
            'dcid ?bedFileNode ?bedFile',
            'bedFile ?bedLineNode ?bedFileNode',
            'chromosome ?bedLineNode ?chrom',
            'chromosomeStart ?bedLineNode ?chromStart',
            'chromosomeEnd ?bedLineNode ?chromEnd',
            'bedName ?bedLineNode ?name',
            'bedScore ?bedLineNode ?score',
            'chromosomeStrand ?bedLineNode ?strand'
        ]

        # Create a filter function based on passed in parameters
        chrom_filter, range_filters = None, []
        if chromosome is not None:
            chrom_filter = lambda row: row['Chromosome'] in chromosome
        if start_pos is not None and end_pos is not None:
            for s, e in zip(start_pos, end_pos):
                filter = lambda row: row['Start'] >= s and row['End'] <= e
                range_filters.append(filter)

        # Compose the filters to create the final row filter
        row_filter = None
        if chrom_filter or range_filters:
            def row_filter(row):
                if chrom_filter and not chrom_filter(row):
                    return False
                if range_filters:
                    return any(range_check(row) for range_check in range_filters)
                return True

        # Add the query contents to the data frame
        self._add_data(new_col_var, new_col_name, new_col_type, constraints, max_rows, seed_col_var='?bedFile', seed_col_name='BedFile', row_filter=row_filter)

    def pandas(self, col_names=None):
        """ Returns a copy of the data in this view as a Pandas DataFrame.

        Args:
          col_names: An optional list specifying which columns to extract.
        """
        if col_names:
            return self._dataframe[col_names].copy()
        return self._dataframe.copy()

    def csv(self, col_names=None):
        """ Returns the data in this view as a CSV string.

        Args:
          col_names: An optional list specifying which columns to extract.
        """
        if col_names:
            return self._dataframe[col_names].to_csv(index=False)
        return self._dataframe.to_csv(index=False)

    def tsv(self, col_names=None):
        """ Returns the data in this view as a TSV string.

        Args:
          col_names: An optional list specifying which columns to extract.
        """
        if col_names:
            return self._dataframe[col_names].to_csv(index=false, sep='\t')
        return self._dataframe.to_csv(index=false, sep='\t')

    # --------------------------- HELPER FUNCTIONS ----------------------------

    def _add_data(self,
                  new_col_var,
                  new_col_name,
                  new_col_type,
                  query_constraints,
                  max_rows,
                  seed_col_var=None,
                  seed_col_name=None,
                  row_filter=None):
        """ TODO Add comment """
        # Generate parameter maps
        name_map = dict(zip(new_col_var, new_col_name))
        type_map = dict(zip(new_col_name, new_col_type))
        if seed_col_var and seed_col_name:
            name_map[seed_col_var] = seed_col_name
            type_map[seed_col_name] = self._col_type[seed_col_name]

        # Generate the query, execute and rename the columns
        variables = new_col_var
        if seed_col_var:
            variables = [seed_col_var] + new_col_var
        query_header = ['SELECT ' + ' '.join(variables)]
        query = ', '.join(query_header + query_constraints)
        raw_result = self._query(query, max_rows)
        new_data = raw_result.rename(index=str, columns=name_map)

        # Update the type map and convert the column types
        for col in type_map:
            self._col_type[col] = type_map[col]
            if type_map[col] == 'Integer':
                new_data[col] = pd.to_numeric(new_data[col])
            else:
                new_data[col] = new_data[col].fillna('')

        # Filter row if a filter is provided
        if row_filter:
            new_data = new_data[new_data.apply(row_filter, axis=1)]

        # Merge the current dataframe with the result dataframe if specified
        if seed_col_var:
            new_data = self._dataframe.merge(
                new_data, how='left', on=seed_col_name)

        # Reset the index and update the dataframe
        new_data.reset_index(drop=True, inplace=True)
        self._dataframe = new_data

    def _query(self, query, max_rows, row_filter=None):
        """ Performs the given datalog query and returns the result as a table.

        Args:
          query: string representing the datalog query.
          max_rows: the maximum number of rows.

        Returns:
          A pandas.DataFrame with the selected variables in the query as the
          the column names. If the query returns multiple values for a property
          then the result is flattened into multiple rows.

        Raises:
          RuntimeError: some problem with executing query (hint in the string)
        """
        assert self._inited, 'Initialization was unsuccessful, cannot execute Query'

        # Send the query to the DataCommons query service
        try:
            response = self._service.query_table(body={
                'query': query,
                'options': {
                    'row_count_limit': max_rows
                }
            }).execute()
        except Exception as e:
            msg = 'Failed to execute query:\n  Query: {}\n  Error: {}'.format(query, e)
            raise RuntimeError(msg)

        # Format the and store it in the dataframe
        header = response.get('header', [])
        rows = response.get('rows', [])
        result_dict = OrderedDict([(header, []) for header in header])
        for row in rows:
            for col in row['cols']:
                result_dict[col['key']].append(col['value'])
        return pd.DataFrame(result_dict).drop_duplicates()

    def _resolve_parameter(self, parameter):
        """ Converts the parameter to an appropriate list of DCIDs.

        Args:
          parameter: Either a string representing a column name, or a list of
            DCIDs

        Returns:
          If the parameter is a column name, then this will return the contents
          of that column as a list, otherwise it will return an error. If the
          parameter is not a string, then it returns the parameter.

        Raises:
          ValueError: if the parameter is a string and it is not a valid
            column in the current data frame.
        """
        if isinstance(parameter, str):
            if parameter not in self._dataframe:
                col_names = ', '.join(['"{}"'.format(col) for col in self._dataframe])
                msg = '"{}" is not a valid column name. Current columns include {}'.format(col_names)
                raise ValueError(msg)
            return self._dataframe[parameter]
        return parameter

    def _format_arg_range(self, parameters):
        """ Formats a list of parameters as a string of repeated arguments. """
        return ' '.join(['"{}"'.format(p) for p in parameters])
