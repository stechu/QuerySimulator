#!/usr/bin/env python

'''
    parse myria query fragments to Finite State Machines
'''

from collections import defaultdict
import json
from itertools import groupby
from fysom import *


# By default, all operators have no children
children = defaultdict(list)
# Populate the list for all operators that do have children
children['RightHashJoin'] = ['argChild1', 'argChild2']
children['RightHashCountingJoin'] = ['argChild1', 'argChild2']
children['SymmetricHashJoin'] = ['argChild1', 'argChild2']
children['LocalMultiwayProducer'] = ['argChild']
children['MultiGroupByAggregate'] = ['argChild']
children['SingleGroupByAggregate'] = ['argChild']
children['ShuffleProducer'] = ['argChild']
children['DbInsert'] = ['argChild']
children['Aggregate'] = ['argChild']
children['Apply'] = ['argChild']
children['Filter'] = ['argChild']
children['UnionAll'] = ['argChildren']
children['Merge'] = ['argChildren']
children['LeapFrogJoin'] = ['argChildren']
children['ColumnSelect'] = ['argChild']
children['SymmetricHashCountingJoin'] = ['argChild1', 'argChild2']
children['BroadcastProducer'] = ['argChild']
children['HyperShuffleProducer'] = ['argChild']
children['InMemoryOrderBy'] = ['argChild']
children['SinkRoot'] = ['argChild']
children['DupElim'] = ['argChild']
children['Rename'] = ['argChild']

root_operators = set(['SinkRoot', 'DbInsert', 'HyperShuffleProducer',
                      'ShuffleProducer', 'BroadcastProducer'])
sender_operators = set(['HyperShuffleProducer',
                        'ShuffleProducer', 'BroadcastProducer'])
receiver_operators = set(['ShuffleConsumer', 'CollectConsumer'])


def read_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)


def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ':'))


def unify_fragments(fragments):
    """Returns a list of operators, adding to each operator a field
    fragment_id, a field id, and converting all the children links to
    list type. """
    ret = []
    for (i, fragment) in enumerate(fragments):
        for operator in fragment['operators']:
            operator['fragmentId'] = i
            for field in children[operator['opType']]:
                if not isinstance(operator[field], list):
                    operator[field] = [operator[field]]
            ret.append(operator)
    #print pretty_json(ret)
    return ret


def operator_get_children(op):
    # Return the names of all child operators of this operator
    ret = []
    for x in children[op['opType']]:
        for c in op[x]:
            ret.append(c)
    return ret


def operator_get_in_pipes(op):
    # By default, all operators have no in pipes
    pipe_fields = defaultdict(list)
    # Populate the list for all operators that do have children
    pipe_fields['CollectConsumer'] = ['argOperatorId']
    pipe_fields['Consumer'] = ['argOperatorId']
    pipe_fields['LocalMultiwayConsumer'] = ['argOperatorId']
    pipe_fields['ShuffleConsumer'] = ['argOperatorId']
    pipe_fields['BroadcastConsumer'] = ['argOperatorId']
    pipe_fields['HyperShuffleConsumer'] = ['argOperatorId']
    return [str(op[x]) for x in pipe_fields[op['opType']]]


# construct fsms for each query fragment
def get_fsms(unified_plan):
    # construct in pipes and out pipes
    in_pipes = defaultdict(list)
    for op in unified_plan:
        # Operator id
        op_id = op['opName']
        # Add pipes
        for producing_op_id in operator_get_in_pipes(op):
            in_pipes[producing_op_id].append(op_id)

    pipe_edges = []
    for producing_op_id in in_pipes:
        pipe_edges.extend([(producing_op_id, y, "")
                           for y in in_pipes[producing_op_id]])

    #  construct fsms
    fsms = []

    def call_event(parent, child):
        event = {
            'name': '{}_call_{}'.format(parent, child),
            'src': parent,
            'dst': child
        }
        return event

    def return_event(parent, child):
        event = {
            'name': '{}_return_{}'.format(child, parent),
            'src': child,
            'dst': parent
        }
        return event
    # group operators by fragment id, create fsm per fragment
    keyfunc = lambda op: op["fragmentId"]
    sorted(unified_plan, key=keyfunc)
    for k, f in groupby(unified_plan, keyfunc):
        events = []
        init = ''
        for op in f:
            op_id = op['opName']
            events.extend([call_event(op['opName'], x)
                           for x in operator_get_children(op)])
            events.extend([return_event(op['opName'], x)
                           for x in operator_get_children(op)])
            if op['opType'] in root_operators:
                init = op_id
        fsm_arg = {
            'initial': init,
            'events': events
        }
        fsms.append({
            'fsm': Fysom(fsm_arg),
            'fragmentId': k}
        )

    return (fsms, pipe_edges)
