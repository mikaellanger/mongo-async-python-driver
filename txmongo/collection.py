# coding: utf-8
# Copyright 2009 Alexandre Fiori
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

import types
import warnings
from txmongo import filter as qf
from txmongo._pymongo import errors
from txmongo._pymongo.son import SON
from txmongo._pymongo.code import Code
from txmongo._pymongo.objectid import ObjectId
from twisted.internet import defer


class Collection(object):
    def __init__(self, database, collection_name):
        self._database = database
        self._collection_name = collection_name
    
    def __str__(self):
        return "%s.%s" % (str(self._database), self._collection_name)
    
    def __repr__(self):
        return "<mongodb Collection: %s>" % str(self)
    
    def __getitem__(self, collection_name):
        return Collection(self._database,
            "%s.%s" % (self._collection_name, collection_name))
    
    def __getattr__(self, collection_name):
        return self[collection_name]
    
    def __call__(self, collection_name):
        return self[collection_name]
    
    def _fields_list_to_dict(self, fields):
        """
        transform a list of fields from ["a", "b"] to {"a":1, "b":1}
        """
        as_dict = {}
        for field in fields:
            if not isinstance(field, types.StringTypes):
                raise TypeError("fields must be a list of key names")
            as_dict[field] = 1
        return as_dict
    
    def _gen_index_name(self, keys):
        return u"_".join([u"%s_%s" % item for item in keys])
    
    @property
    def name(self):
        """The name of this collection"""
        return self._collection_name
    
    @property
    def full_name(self):
        '''The full name of this Collection (database_name.collection_name).'''
        return "%s.%s" % (str(self._database), self._collection_name)
    
    def options(self):
        def wrapper(result):
            if result:
                options = result.get("options", {})
                if "create" in options:
                    del options["create"]
                return options
            return {}
        
        d = self._database.system.namespaces.find_one({"name": str(self)})
        d.addCallback(wrapper)
        return d
    
    def find(self, spec=None, skip=0, limit=0, fields=None, filter=None, slave_okay=False, _proto=None):
        if spec is None:
            spec = SON()
        
        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(fields, (types.ListType, types.NoneType)):
            raise TypeError("fields must be an istance of list")
        if not isinstance(skip, types.IntType):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, types.IntType):
            raise TypeError("limit must be an instance of int")
        
        if fields is not None:
            if not fields:
                fields = ["_id"]
            fields = self._fields_list_to_dict(fields)
        
        if isinstance(filter, (qf.sort, qf.hint, qf.explain, qf.snapshot)):
            spec = SON(dict(query=spec))
            for k, v in filter.items():
                spec[k] = isinstance(v, types.TupleType) and SON(v) or v
        
        # send the command through a specific connection
        # this is required for the connection pool to work
        # when safe=True
        if _proto is None:
            _proto = self._database._connection(slave_okay=slave_okay)
        return _proto.OP_QUERY(str(self), spec, skip, limit, fields, slave_okay=slave_okay)
    
    @defer.inlineCallbacks
    def find_one(self, spec=None, fields=None, filter=None, slave_okay=False, _proto=None):
        if isinstance(spec, ObjectId):
            spec = SON(dict(_id=spec))
        
        cursor = yield self.find(spec, limit=-1, fields=fields, filter=filter, slave_okay=slave_okay, _proto=_proto)
        docs = yield cursor.as_list()
        if not docs:
            defer.returnValue(None)
        
        if isinstance(docs[0], dict) and docs[0].get("err") is not None:
            raise errors.OperationFailure(docs[0])
        defer.returnValue(docs[0])
    
    def runCommand(self, command, value=1, **kwargs):
        '''
        @see: http://www.mongodb.org/display/DOCS/Commands
        @deprecated: use database.command() instead, as in pymongo
        http://api.mongodb.org/python/1.10.1%2B/api/pymongo/database.html#pymongo.database.Database.command
        '''
        warnings.warn("collection.runCommand: use database.command() instead", DeprecationWarning)
        cmd = SON([ (command, value) ])
        cmd.update(**kwargs)
        d = self._database["$cmd"].find_one(cmd)
        return d
    
    def findAndModify(self, query={}, update=None, upsert=False, **kwargs):
        '''@see: http://api.mongodb.org/python/1.10.1%2B/api/pymongo/collection.html#pymongo.collection.Collection.find_and_modify'''
        cmd = SON([("findAndModify", self._collection_name),
                   ("query", query),
                   ("update", update),
                   ("upsert", upsert), ])
        cmd.update(**kwargs)
        d = self._database["$cmd"].find_one(cmd)
        return d
    
    def count(self, spec=None, fields=None, slave_okay=False):
        def wrapper(result):
            return result["n"]
        
        if fields is not None:
            if not fields:
                fields = ["_id"]
            fields = self._fields_list_to_dict(fields)
        
        spec = SON([("count", self._collection_name),
                    ("query", spec or SON()),
                    ("fields", fields)])
        d = self._database["$cmd"].find_one(spec, slave_okay=slave_okay)
        d.addCallback(wrapper)
        return d
    
    def group(self, keys, initial, reduce, condition=None, finalize=None):
        body = {
            "ns": self._collection_name,
            "key": self._fields_list_to_dict(keys),
            "initial": initial,
            "$reduce": Code(reduce),
        }
        
        if condition:
            body["cond"] = condition
        if finalize:
            body["finalize"] = Code(finalize)
        
        return self._database["$cmd"].find_one({"group": body})
    
    def filemd5(self, spec):
        if not isinstance(spec, ObjectId):
            raise ValueError("filemd5 expected an objectid for its on-keyword argument")
        spec = SON([("filemd5", spec), ("root", self._collection_name)])
        doc = yield self._database['$cmd'].find_one(spec)
        defer.returnValue((doc or {}).get('md5'))
    
    def __safe_operation(self, proto, safe=False, ids=None):
        callit = False
        if safe is True:
            d = self._database["$cmd"].find_one({"getlasterror": 1}, _proto=proto)
        else:
            callit = True
            d = defer.Deferred()
        
        if ids is not None:
            d.addCallback(lambda _: ids)
        
        if callit is True:
            d.callback(None)
        return d
    
    def insert(self, docs, safe=False):
        if isinstance(docs, types.DictType):
            ids = docs.get('_id', ObjectId())
            docs["_id"] = ids
            docs = [docs]
        elif isinstance(docs, types.ListType):
            ids = []
            for doc in docs:
                if isinstance(doc, types.DictType):
                    id = doc.get('_id', ObjectId())
                    ids.append(id)
                    doc["_id"] = id
                else:
                    raise TypeError("insert takes a document or a list of documents")
        else:
            raise TypeError("insert takes a document or a list of documents")
        proto = self._database._connection()
        proto.OP_INSERT(str(self), docs)
        return self.__safe_operation(proto, safe, ids)
    
    def update(self, spec, document, upsert=False, multi=False, safe=False):
        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(document, types.DictType):
            raise TypeError("document must be an instance of dict")
        if not isinstance(upsert, types.BooleanType):
            raise TypeError("upsert must be an instance of bool")
        proto = self._database._connection()
        proto.OP_UPDATE(str(self), spec, document, upsert, multi)
        return self.__safe_operation(proto, safe)
    
    def save(self, doc, safe=False):
        if not isinstance(doc, types.DictType):
            raise TypeError("cannot save objects of type %s" % type(doc))
        
        objid = doc.get("_id")
        if objid:
            return self.update({"_id": objid}, doc, safe=safe, upsert=True)
        else:
            return self.insert(doc, safe=safe)
    
    def remove(self, spec, safe=False):
        if isinstance(spec, ObjectId):
            spec = SON(dict(_id=spec))
        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict, not %s" % type(spec))
        
        proto = self._database._connection()
        proto.OP_DELETE(str(self), spec)
        return self.__safe_operation(proto, safe)
    
    def drop(self, safe=False):
        return self.remove({}, safe)
    
    def create_index(self, sort_fields, unique=False, dropDups=False):
        def wrapper(result, name):
            return name
        
        if not isinstance(sort_fields, qf.sort):
            raise TypeError("sort_fields must be an instance of filter.sort")
        
        name = self._gen_index_name(sort_fields["orderby"])
        index = SON(dict(
            ns=str(self),
            name=name,
            key=SON(dict(sort_fields["orderby"])),
            unique=unique,
            dropDups=dropDups
        ))
        
        d = self._database.system.indexes.insert(index, safe=True)
        d.addCallback(wrapper, name)
        return d
    
    def drop_index(self, index_identifier):
        if isinstance(index_identifier, types.StringTypes):
            name = index_identifier
        elif isinstance(index_identifier, qf.sort):
            name = self._gen_index_name(index_identifier["orderby"])
        else:
            raise TypeError("index_identifier must be a name or instance of filter.sort")
        
        cmd = SON([("deleteIndexes", self._collection_name), ("index", name)])
        return self._database["$cmd"].find_one(cmd)
    
    def drop_indexes(self):
        return self.drop_index("*")
    
    def index_information(self):
        def wrapper(raw):
            info = {}
            for idx in raw:
                info[idx["name"]] = idx["key"].items()
            return info
        
        d = self._database.system.indexes.find({"ns": str(self)})
        d.addCallback(wrapper)
        return d
    
    def rename(self, new_name):
        cmd = SON([("renameCollection", str(self)), ("to", "%s.%s" % \
            (str(self._database), new_name))])
        return self._database("admin")["$cmd"].find_one(cmd)
    
    def distinct(self, key, spec=None):
        def wrapper(result):
            if result:
                return result.get("values")
            return {}
        
        cmd = SON([("distinct", self._collection_name), ("key", key)])
        if spec:
            cmd["query"] = spec
        
        d = self._database["$cmd"].find_one(cmd)
        d.addCallback(wrapper)
        return d
    
    def map_reduce(self, map, reduce, full_response=False, slave_okay=False, **kwargs):
        def wrapper(result, full_response):
            if full_response:
                return result
            return result.get("result")
        
        cmd = SON([("mapreduce", self._collection_name),
                       ("map", map), ("reduce", reduce)])
        cmd.update(**kwargs)
        d = self._database["$cmd"].find_one(cmd, slave_okay=slave_okay)
        d.addCallback(wrapper, full_response)
        return d
    
