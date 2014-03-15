#!/usr/bin/env python
import unittest
import parse
import os

''' unit test of optimizer
'''


class TestOpimizerFunctions(unittest.TestCase):

    def testBuildFSMs(self):
        myria_json_plan = parse.read_json(
            os.getcwd()+'/testdata/hash_join.json')
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
        fsm_join.insert_call_join()
        self.assertEqual(fsm_join.current, 'join')
        fsm_join.join_call_gather_r()
        self.assertEqual(fsm_join.current, 'gather_r')
        fsm_join.gather_r_return_join()
        self.assertEqual(fsm_join.current, 'join')
