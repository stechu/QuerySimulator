#!/usr/bin/env python
import unittest
from fysom import *

''' unit test of optimizer
'''


class TestOpimizerFunctions(unittest.TestCase):

    def testFSM(self):

        def onpanic(e):
            print 'panic! ' + e.msg

        def oncalm(e):
            print 'thanks to ' + e.msg

        def ongreen(e):
            print 'green'

        def onyellow(e):
            print 'yellow'

        def onred(e):
            print 'red'

        fsm = Fysom({
            'initial': 'green',
            'events': [
                {'name': 'warn',  'src': 'green',  'dst': 'yellow'},
                {'name': 'panic', 'src': 'yellow', 'dst': 'red'},
                {'name': 'calm',  'src': 'red',    'dst': 'yellow'},
                {'name': 'clear', 'src': 'yellow', 'dst': 'green'}
            ],
            'callbacks': {
                'onpanic': onpanic,
                'oncalm':   oncalm,
                'ongreen':  ongreen,
                'onyellow': onyellow,
                'onred':    onred
            }
        })

        fsm.startup()
        fsm.warn(msg='killer bees')
        fsm.calm(msg='sedatives in the honey pots')

