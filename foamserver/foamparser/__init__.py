
import sys
from dictdiffer import DictDiffer

class FoamParserException(Exception):
    pass

# some feautures of ply used need at least 2.3
if sys.version_info < (2,3):
    raise FoamParserException(
        "Version {0} is not sufficient for foamparser (2.3 needed)".format(
            sys.version_info))

from parsed_parameter_file import FoamFileParser, FoamStringParser


class FoamDictDiffer(DictDiffer):
    def _diff_DictProxy(self,first,second,node):
        return self._diff_dict(first,second,node)
    def _diff_ListProxy(self,first,second,node):
        return self._diff_list(first,second,node)
    def _diff_BoolProxy(self,first,second,node):
        return self._diff_generic(bool(first),bool(second),node)

