#!/usr/bin/env python
import unittest
from parse import *
import os

''' unit test of optimizer
'''


class TestOpimizerFunctions(unittest.TestCase):

    def testBuildFSMs(self):
        query_plan = QueryPlan(os.getcwd()+'/testdata/hash_join.json')
        (fsms, pipes) = query_plan.get_fsms()

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
