
import re
from foamserver.foamparser import FoamDictDiffer, FoamFileParser, FoamStringParser

differ = FoamDictDiffer()

def parse_doc(doc):
    try:
        return FoamFileParser(doc['payload']).data
    except:
        return FoamStringParser(doc['payload']).data

def diffs_for_docs(docs):
    diffs = {}
    d0 = parse_doc(docs[0])
    for doc in docs[1:]:
        d = parse_doc(doc)
        try:
            diff = list(differ.diff(d0,d))
            if len(diff) >0:
                diffs[doc['meta']['timestamp']] = diff
        except KeyError:
            print('no timestamp for {0}'.format(doc['_id']['path']))
        d0 = d
    return diffs


def get_system_changes(col,project):
    paths = col.find({'project':project,'_n':{'$eq':1}}).distinct('path')
    events = {}
    for path in paths:
        docs = sorted(
            col.find({'project':project,'path':path}),key=lambda x:x['_n'])
        diffs = diffs_for_docs(docs)
        events[path] = diffs
    return events

def filter_events(events,exclude=[]):
    result = {}
    ex = [re.compile(x) for x in exclude]
    for timestamp,diffs in events.items():
        r = []
        for diff in diffs:
            if not any(x.match(diff[1]) for x in ex):
                r.append(diff)
        if len(r):
            result[timestamp] = r
    return result

