
import sys

class FoamParserException(Exception):
    pass

# some feautures of ply used need at least 2.3
if sys.version_info < (2,3):
    raise FoamParserException(
        "Version {0} is not sufficient for foamparser (2.3 needed)".format(
            sys.version_info))

from parsed_parameter_file import FoamFileParser, FoamStringParser
