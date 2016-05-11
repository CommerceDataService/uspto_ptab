'''
  A parser for PDF documents that implements the doc_processing_toolkit from 18F:
  https://github.com/18f/doc_processing_toolkit

  This needs to be used because Tika is only able to parse a portion of our files.
'''
import sys
from textextraction.extractors import text_extractor
import os.path


filepath = sys.argv[-1]
base_path = os.path.abspath(os.path.dirname(__file__))
full_filepath = os.path.join(base_path, filepath)

try:
    # this will create a file by the same name, in the same location, with the .txt extension
    file_contents = text_extractor(doc_path=full_filepath, force_convert=False)
except FileNotFoundError as e:
    print('Path {} does not exist'.format(full_filepath))
    print(e)
except Exception as e:
    print('Something went wrong. :(')
    print(e)
