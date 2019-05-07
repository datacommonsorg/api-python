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
"""Basic demo showcasing how to use the Bio extension

When constructing a DCFrame from a query, the frame can be manipulated using
the 'select' and 'process' argument.

Note to use this API code in an Colab or another iPython notebook environment
add the following code:
pip install --upgrade numpy
pip install --upgrade pandas
pip install --upgrade git+https://github.com/ACscooter/datacommons.git@api-version-2
"""

from datacommons.utils import DatalogQuery
from datacommons.bio import BioExtension

import pandas as pd

import datacommons

# Print options
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 20)

# Helper function for formatting table printing
def print_pandas(example_num, df):
    print('-'*80)
    print('EXAMPLE {}'.format(example_num))
    print('-'*80 + '\n')
    print(df)
    print('\n')

def main():
  # Example 1: query for experiments that have a specific assay specified.
  frame_1 = datacommons.DCFrame()
  frame_1 = BioExtension(frame_1)
  frame_1.get_experiments('Experiment',
                          assay_category=['Transcription'],
                          bio_class=['cell line'],
                          bio_term=['K562'],
                          rows=1000)
  frame_1.expand('description', 'Experiment', 'Description')
  print_pandas(1, frame_1.pandas())

  # Example 2: query for experiments published by a specific lab
  frame_2 = datacommons.DCFrame()
  frame_2 = BioExtension(frame_2)
  frame_2.get_experiments('Experiment', lab_name=['yijun-ruan'])
  frame_2.expand('description', 'Experiment', 'Description')
  print_pandas(2, frame_2.pandas())

  # Example 3: query for experiments by assembly
  frame_3 = datacommons.DCFrame()
  frame_3 = BioExtension(frame_3)
  frame_3.get_experiments('Experiment', assembly=['hg19'], rows=1000)
  frame_3.expand('description', 'Experiment', 'Description')
  print_pandas(3, frame_3.pandas())

  # Example 4: query for experiments by multiple biosample summaries
  frame_4 = datacommons.DCFrame()
  frame_4 = BioExtension(frame_4)
  frame_4.get_experiments('Experiment',
                          bio_class=['primary cell', 'cell line'],
                          bio_term=['endothelial cell of umbilical vein', 'K562'],
                          rows=1000)
  frame_4.expand('description', 'Experiment', 'Description')
  print_pandas(4, frame_4.pandas())

  # Example 5: query for bed files associated with a given column of experiments
  frame_5 = datacommons.DCFrame()
  frame_5 = BioExtension(frame_5)
  frame_5.get_experiments('Experiment',
                          assay_category=['Transcription'],
                          bio_class=['cell line'],
                          bio_term=['K562'])
  frame_5.get_bed_files('Experiment', 'BedFile IDs')
  frame_5.expand('description', 'Experiment', 'Description')
  frame_5.expand('name', 'BedFile IDs', 'BedFile Names')
  print_pandas(5, frame_5.pandas())

  # Example 6: get all BedLines associated with a BedFile
  frame_6 = datacommons.DCFrame()
  frame_6 = BioExtension(frame_6)
  frame_6.get_experiments('Experiment',
                          assay_category=['Transcription'],
                          bio_class=['cell line'],
                          bio_term=['K562'],
                          rows=1)
  frame_6.get_bed_files('Experiment', 'BedFile IDs')
  frame_6.get_bed_lines('BedFile IDs')
  print_pandas(6, frame_6.pandas())

  # Example 7: getting other fields in extended Bed files. One thing to notice
  # is that we can still filter by the chromosome even if we're not populating
  # a column with its values.
  prop_info = [
      ('StartPos', 'chromosomeStart', 'Integer'),
      ('EndPos', 'chromosomeEnd', 'Integer'),
      ('RGBValue', 'itemRGB', 'Text'),
      ('ThickStart', 'thickStart', 'Integer'),
      ('ThickEnd', 'thickEnd', 'Integer'),
  ]
  frame_7 = datacommons.DCFrame()
  frame_7 = BioExtension(frame_7)
  frame_7.get_experiments('Experiment', lab_name=['yijun-ruan'], rows=40)
  frame_7.get_bed_files('Experiment', 'BedFile IDs', rows=40)
  frame_7.expand('name', 'BedFile IDs', 'BedFileName', rows=40)
  frame_7.get_bed_lines('BedFile IDs',
                        prop_info=prop_info,
                        chromosome=['chr7'],
                        rows=1000)
  print_pandas(7, frame_7.pandas())

  # Example 8: querying bedlines from multiple files and filtering by chromosome
  # and start-end range.
  # NOTE: This query takes a bit longer than others due to its size.
  frame_8 = datacommons.DCFrame()
  frame_8 = BioExtension(frame_8)
  frame_8.get_experiments('Experiment', lab_name=['yijun-ruan'], rows=40)
  frame_8.get_bed_files('Experiment', 'BedFile IDs', rows=40)
  frame_8.expand('name', 'BedFile IDs', 'BedFileName', rows=40)
  frame_8.get_bed_lines('BedFile IDs',
                        chromosome=['chr7'],
                        start_pos=[5000000],
                        end_pos=[10000000],
                        rows=500000)
  print_pandas(8, frame_8.pandas())

if __name__ == '__main__':
  main()
