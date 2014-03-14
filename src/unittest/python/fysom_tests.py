#!/usr/bin/env python
import unittest
import parse
from fysom import *
import os

''' unit test of optimizer
'''


class TestOpimizerFunctions(unittest.TestCase):

    def testFSM(self):

        def oncall_join(e):
            print 'call join.next() ' + e.msg

        def oncall_gatherR(e):
            print 'call GatherR.next() ' + e.msg

        def onreturnR(e):
            print 'GatherR.next() returns tuples ' + e.msg

        def oncall_gatherS(e):
            print 'call GatherS.next() ' + e.msg

        def onreturnS(e):
            print 'GatherS.next() returns tuples ' + e.msg

        def oninsert_tuple(e):
            print 'Join.next() returns tuples ' + e.msg

        fsm = Fysom({
            'initial': 'insert',
            'events': [
                {'name': 'call_join',  'src': 'insert',  'dst': 'join'},
                {'name': 'call_gatherR', 'src': 'join', 'dst': 'gatherR'},
                {'name': 'returnR',  'src': 'gatherR', 'dst': 'join'},
                {'name': 'call_gatherS', 'src': 'join', 'dst': 'gatherS'},
                {'name': 'returnS', 'src': 'gatherS', 'dst': 'join'},
                {'name': 'insert_tuple', 'src': 'join', 'dst': 'insert'}
            ],
            'callbacks': {
                'oncall_join': oncall_join,
                'oncall_gatherR':   oncall_gatherR,
                'onreturnR':  onreturnR,
                'oncall_gatherS': oncall_gatherS,
                'onreturnS':    onreturnS,
                'oninsert_tuple':  oninsert_tuple
            }
        })

        fsm.call_join('bob', msg='pull join')
        fsm.call_gatherR(msg='pull gather R')
        fsm.returnR(msg='return')

    def testBuildFSMs(self):
        myria_json_plan = parse.read_json(
            os.getcwd()+'/src/unittest/python/hash_join.json')
        fragments = myria_json_plan['fragments']
        unified_plan = parse.unify_fragments(fragments)
        (fsms, pipes) = parse.get_fsms(unified_plan)

        #test query fragment 0
        fsm_sr = fsms[0]['fsm']
        self.assertEqual(fsm_sr.current, 'shuffle_r')
        fsm_sr.shuffle_r_call_scan_r()
        self.assertEqual(fsm_sr.current, 'scan_r')

        #test query fragment 2
        fsm_join = fsms[2]['fsm']
        self.assertEqual(fsm_join.current, 'insert')
