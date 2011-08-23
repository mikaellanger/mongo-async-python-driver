# coding: utf-8
# Copyright 2009 Alexandre Fiori, 2011 Marcel Ball
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

import struct
from txmongo._pymongo import bson
from twisted.internet import defer, protocol

_ONE = "\x01\x00\x00\x00"
_ZERO = "\x00\x00\x00\x00"


class MongoCursor(object):
    def __init__(self, request_id, collection, limit, protocol):
        self.initial = True
        self.cursor_id = None
        self.request_id = request_id
        self.protocol = protocol
        self.document_count = 0
        self.limit = limit
        self.collection = collection
        self._buffer = []
        self._position = 0
        self._error = None
        self.deferred = defer.Deferred()
    
    def __del__(self):
        if hasattr(self, 'cursor_id') and self.cursor_id:
            self.protocol.OP_KILL_CURSORS([self.cursor_id])
            self.protocol = None
    
    def error(self, error):
        self._error = error
        if self.deferred:
            self.deferred.errback(self._error)
    
    def add_documents(self, documents):
        self._position = 0
        self._buffer = documents
        self.document_count += len(documents)
        if self.initial and self.deferred:
            self.initial = False
            d, self.deferred = self.deferred, None
            d.callback(self)
            
        elif self.deferred:
            self._position += 1
            d, self.deferred = self.deferred, None
            if self._buffer:
                transforms = self.protocol.get_outgoing_transformations()
                r = self._buffer[0]
                for trans in transforms:
                    r = trans(r)
                d.callback(r)
            else:
                d.callback(None)
    
    def __load_more(self):
        self.request_id = self.protocol.current_id
        next_batch = 0
        if self.limit:
            next_batch = self.limit - self.document_count
        self.protocol.set_cursor(self.request_id, self)
        self.protocol.OP_GET_MORE(self.collection, next_batch, self.cursor_id)
    
    def __iter__(self):
        return self
    
    @defer.inlineCallbacks
    def as_list(self):
        result_list = []
        for r in self:
            if isinstance(r, defer.Deferred):
                r = yield r
            result_list.append(r)
        defer.returnValue(result_list)
    
    def next(self):
        if self._error:
            raise self.error
        if self._position >= len(self._buffer):
            if self.deferred:
                return self.deferred
            else:
                if self.cursor_id:
                    self.deferred = defer.Deferred()
                    self.__load_more()
                    return self.deferred
                else:
                    raise StopIteration()
        else:
            transforms = self.protocol.get_outgoing_transformations()
            r = self._buffer[self._position]
            for trans in transforms:
                r = trans(r)
            self._position += 1
            return r
    

class MongoProtocol(protocol.Protocol):
    def __init__(self):
        self.__id = 0
        self.__queries = {}
        self.__buffer = ""
        self.__datalen = None
        self.__response = 0
        self.__waiting_header = True
    
    @property
    def current_id(self):
        return self.__id
    
    def set_cursor(self, id, cursor):
        self.__queries[id] = cursor
    
    def connectionMade(self):
        self.factory.append(self)
    
    def get_outgoing_transformations(self):
        if hasattr(self, 'factory'):
            return self.factory.manager.API._outgoing_transformations
        return []
    
    def connectionLost(self, reason):
        self.connected = 0
        self.factory.remove(self)
        for queryObj in self.__queries.values():
            queryObj.deferred.errback(ValueError("Connection Lost"))
        protocol.Protocol.connectionLost(self, reason)
    
    def dataReceived(self, data):
        while self.__waiting_header:
            self.__buffer += data
            if len(self.__buffer) < 16:
                break
            
            # got full header, 16 bytes (or more)
            header, extra = self.__buffer[:16], self.__buffer[16:]
            self.__buffer = ""
            self.__waiting_header = False
            datalen, request, response, operation = struct.unpack("<iiii", header)
            self.__datalen = datalen - 16
            self.__response = response
            if extra:
                self.dataReceived(extra)
            break
        else:
            if self.__datalen is not None:
                data, extra = data[:self.__datalen], data[self.__datalen:]
                self.__datalen -= len(data)
            else:
                extra = ""
            
            self.__buffer += data
            if self.__datalen == 0:
                self.messageReceived(self.__response, self.__buffer)
                self.__datalen = None
                self.__waiting_header = True
                self.__buffer = ""
                if extra:
                    self.dataReceived(extra)
    
    def messageReceived(self, request_id, packet):
        response_flag, cursor_id, start, length = struct.unpack("<iqii", packet[:20])
        if response_flag == 1:
            self.queryFailure(request_id, cursor_id, response_flag, packet[20:])
            return
        self.querySuccess(request_id, cursor_id, bson._to_dicts(packet[20:]))
    
    def sendMessage(self, operation, collection, message, query_opts=_ZERO):
        fullname = collection and bson._make_c_string(collection) or ""
        message = query_opts + fullname + message
        header = struct.pack("<iiii", 16 + len(message), self.__id, 0, operation)
        self.transport.write(header + message)
        self.__id += 1
    
    def OP_INSERT(self, collection, docs):
        docs = [bson.BSON.from_dict(doc) for doc in docs]
        self.sendMessage(2002, collection, "".join(docs))
    
    def OP_UPDATE(self, collection, spec, document, upsert=False, multi=False):
        options = 0
        if upsert:
            options += 1
        if multi:
            options += 2
        
        message = struct.pack("<i", options) + \
            bson.BSON.from_dict(spec) + bson.BSON.from_dict(document)
        self.sendMessage(2001, collection, message)
    
    def OP_DELETE(self, collection, spec):
        self.sendMessage(2006, collection, _ZERO + bson.BSON.from_dict(spec))
    
    def OP_KILL_CURSORS(self, cursors):
        message = struct.pack("<i", len(cursors))
        for cursor_id in cursors:
            message += struct.pack("<q", cursor_id)
        self.sendMessage(2007, None, message)
    
    def OP_GET_MORE(self, collection, limit, cursor_id):
        message = struct.pack("<iq", limit, cursor_id)
        self.sendMessage(2005, collection, message)
    
    def OP_QUERY(self, collection, spec, skip, limit, fields=None, slave_okay=False):
        message = struct.pack("<ii", skip, limit) + bson.BSON.from_dict(spec)
        if fields:
            message += bson.BSON.from_dict(fields)
        cursor = MongoCursor(self.__id, collection, limit, self)
        self.__queries[self.__id] = cursor
        opts = _ZERO
        if slave_okay:
            opts = struct.pack('<I', 4)
        self.sendMessage(2004, collection, message, opts)
        return cursor.deferred
    
    def queryFailure(self, request_id, cursor_id, response, raw_error):
        queryObj = self.__queries.pop(request_id, None)
        if queryObj:
            queryObj.deferred.errback(ValueError("mongo error=%s" % repr(raw_error)))
            del(queryObj)
    
    def querySuccess(self, request_id, cursor_id, documents):
        try:
            cursor = self.__queries.pop(request_id)
        except KeyError:
            return
        if len(documents) == 1 and u'$err' in documents[0]:
            if documents[0]['code'] == 13435:
                self.factory.manager.checkMaster()
                #This error likely means the master has resigned and a new master has taken it places
                #start looking for a new master
            cursor.error(ValueError("mongo error=%s" % str(documents[0])))
            return
        
        if cursor.limit and cursor_id:
            next_batch = cursor.limit - cursor.document_count - len(documents)
            # Assert, because according to the protocol spec and my observations
            # there should be no problems with this, but who knows? At least it will
            # be noticed, if something unexpected happens. And it is definitely
            # better, than silently returning a wrong number of documents
            assert next_batch >= 0, "Unexpected number of documents received!"
            if not next_batch:
                self.OP_KILL_CURSORS([cursor_id])
                cursor_id = None
        
        cursor.cursor_id = cursor_id
        cursor.add_documents(documents)
    

