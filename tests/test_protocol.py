# coding: utf-8
# Copyright 2010 Mark L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from twisted.trial import unittest
from twisted.internet import defer
from twisted.test.proto_helpers import StringTransport
from txmongo.protocol import MongoProtocol, MongoCursor


class TestQuerySuccess(unittest.TestCase):
    """These are very much implementation-specific, so they might have be removed,
    should the internal API change, but for the current MongoProtocol implementation
    they make sense.
    """
    
    def setUp(self):
        self.protocol = MongoProtocol()
        self.cursor = MongoCursor(0, 'foobar', 5, self.protocol)
        # A query has been sent, so id is different from the one in the query
        self.protocol._MongoProtocol__id = 1
        self.protocol._MongoProtocol__queries[self.cursor.request_id] = self.cursor
        self.protocol.transport = StringTransport()
    
    def test_UnknownRequestId(self):
        self.protocol.querySuccess(1, 0, [])
        self.assertEqual(len(self.protocol._MongoProtocol__queries), 1)
        self.assertEqual(self.protocol._MongoProtocol__id, 1)
        self.failUnless(self.cursor.deferred, "The query was not found so the Deferred should still exist")
        self.failIf(self.cursor.deferred.called, "The query was not found so the Deferred should not fire")
    
    def test_CursorExhausted(self):
        self.protocol.querySuccess(0, 0, [])
        self.assertEqual(len(self.protocol._MongoProtocol__queries), 0)
        self.assertEqual(self.protocol._MongoProtocol__id, 1)
        self.failIf(self.cursor.deferred, "The required number of documents was retrieved, the defered should be fired and gone")
    
    def test_CursorNotExhausted(self):
        self.protocol.querySuccess(0, 1, [{}, {}])
        for d in self.cursor:  # consume documents until __load_more is triggered
            if isinstance(d, defer.Deferred):
                break
        
        self.assertEqual(len(self.protocol._MongoProtocol__queries), 1,
                         "There is more data, so the query should remain in the __queries")
        self.failIfEqual(self.protocol._MongoProtocol__id, 1,
                         "OP_GETMORE has been performed so the __id should not stay the same")
        self.failIf(0 in self.protocol._MongoProtocol__queries,
                    "Id has changed, so the key the query was under should be vacated")
        self.failUnless(self.cursor.deferred,
                    "Didn't get all the desired data, so the deferred should not fire")
        self.assertEqual(len(self.cursor._buffer), 2)
    
    def test_CursorNotExhaustedAndLimitWasHit(self):
        self.cursor.limit = 2
        self.protocol.querySuccess(0, 1, [{}, {}])
        for d in self.cursor:
            self.failIf(isinstance(d, defer.Deferred), "Should not get a deferred while iterating as all documents are loaded")
        self.failIf(len(self.protocol._MongoProtocol__queries), "We are done with the query, there should be nothing in __queries")
    
    def test_CursorNotExhaustedAndLimitWasNotHit(self):
        self.cursor.limit = 2
        self.protocol.querySuccess(0, 1, [{}])
        self.failUnless(self.cursor.cursor_id, "Cursor should have a live cursor id")
        got_deferred = False
        for d in self.cursor:
            if isinstance(d, defer.Deferred):
                got_deferred = True
                break
        self.failUnless(got_deferred, "Should have got a deferred to load more while iterating through cursor")
        self.assertEqual(len(self.protocol._MongoProtocol__queries), 1, 
                            "There is more data, so the query should remain in the __queries")
        self.failIfEqual(self.protocol._MongoProtocol__id, 1,
                            "OP_GETMORE has been performed so the __id should not stay the same")
        self.failIf(0 in self.protocol._MongoProtocol__queries,
                    "Id has changed, so the key the query was under should be vacated")
        self.failUnless(self.cursor.deferred, "Cursor should have a deferred as it is loading more results")
        self.failIf(self.cursor.deferred.called, "Didn't get all the desired data, so the deferred should not fire")
    
    def test_UnexpectedDocumentNumber(self):
        self.cursor.limit = 2
        # Testing for an AssertionError feels kind of wrong, but it really is an
        # exceptional condition, that should not ever happen according to the spec
        self.assertRaises(AssertionError, self.protocol.querySuccess, 0, 1, [{}, {}, {}])
