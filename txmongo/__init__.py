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

from txmongo._pymongo.son import SON
from txmongo.database import Database
from txmongo.protocol import MongoProtocol
from twisted.internet import defer, reactor, protocol


class _offline(object):
    def OP_INSERT(self, *args, **kwargs):
        deferred = defer.Deferred()
        deferred.errback(RuntimeWarning("not connected"))
        return deferred

    def OP_UPDATE(self, *args, **kwargs):
        deferred = defer.Deferred()
        deferred.errback(RuntimeWarning("not connected"))
        return deferred

    def OP_DELETE(self, *args, **kwargs):
        deferred = defer.Deferred()
        deferred.errback(RuntimeWarning("not connected"))
        return deferred

    def OP_QUERY(self, *args, **kwargs):
        deferred = defer.Deferred()
        deferred.errback(RuntimeWarning("not connected"))
        return deferred


class MongoAPI(object):
    def __init__(self, factory):
        self.__factory = factory
        self._connected = factory.deferred

    def disconnect(self):
        return self.__factory.disconnect()

    def __repr__(self):
        try:
            cli = self.__factory.pool[0].transport.getPeer()
        except:
            info = "not connected"
        else:
            info = "%s:%s - %d connection(s)" % (cli.host, cli.port, self.__factory.size)
        return "<Mongodb: %s>" % info

    def __getitem__(self, database_name):
        return Database(self.__factory, database_name)

    def __getattr__(self, database_name):
        return self[database_name]


<<<<<<< HEAD
class _MongoConnectionManager(object):
    def __init__(self, hosts=['localhost:27017'], pool_size=5):
        if pool_size < 1:
            pool_size = 1
        self.hosts = []
        self.pools = {}
        self.master = None
        self.pool_size = pool_size
        self.deferred = defer.Deferred()
        self.API = MongoAPI(self)
        self._checkingMaster = False
        self._checkCount = 0
        self._checkMaster = task.LoopingCall(self.checkMaster)
        self._checkMaster.start(15, False)
        
        for host in hosts:
            self.addHost(host)
    
    def __str__(self):
        hostinfo = ", ".join([str(p) for p in self.pools.values()])
        return '<_MongoConnectionManager %s hosts - [%s]>' % (len(self.hosts), hostinfo)
    
    def __repr__(self):
        return self.__str__()
    
    def reconnect(self):
        for host in self.hosts:
            self.addHost(host)
    
    def disconnect(self):
        defs = []
        for pool in self.pools.values():
            defs.append(pool.disconnectPool())
        self.pools = {}
        return defer.DeferredList(defs)
    
    def connection(self, slave_okay=False):
        if slave_okay:
            for pool in self.pools.values():
                if pool.isMaster:
                    continue
                con = pool.connection()
                if not isinstance(con, _offline):
                    return con
        
        if self.master is not None:
            pool = self.pools[self.master]
            return pool.connection()
        else:
            return _offline()
    
    def checkMaster(self):
        if self._checkingMaster:
            return
        self._checkingMaster = True
        self._checkCount = 0
        
        for host, pool in self.pools.items():
            d = defer.Deferred()
            pool.checkMaster(d)
            d.addCallback(self._checkMasterCallback, host)
            d.addErrback(self._checkMasterErrback, host, pool)
    
    def _checkMasterCallback(self, isMaster, host):
        print 'Check Master Result: ', host, isMaster
        if isMaster:
            self.master = host
            print 'Found Master: ', host
        self._checkCount += 1
        if self._checkCount == len(self.hosts):
            self._checkingMaster = False
            if not self.master:
                reactor.callLater(1, self.checkMaster)
    
    def _checkMasterErrback(self, err, host, pool):
        print 'Error: ', err
        self._checkCount += 1
        if self._checkCount == len(self.hosts):
            self._checkingMaster = False
            if not self.master:
                reactor.callLater(1, self.checkMaster)
    
    def addHost(self, host):
        if ':' not in host:
            host += ':27017'
        if host not in self.hosts:
            self.hosts.append(host)
        hn, port = host.split(':')
        port = int(port)
        pool = _MongoConnectionPool(self, hn, port, self.pool_size)
        self.pools[host] = pool
        pool.deferred.addCallback(self._addHostComplete, host)
    
    def _addHostComplete(self, pool, host):
        if pool.isMaster:
            self.master = host
            self.deferred.callback(self.API)
    
    def updateHosts(self, hosts):
        for h in hosts:
            if h not in self.hosts:
                self.addHost(h)
        for h in self.hosts:
            if h not in hosts:
                pool = self.pools[h]
                pool.disconnectPool()
                del self.pools[h]
                self.hosts.remove(h)
    

class _MongoConnectionPool(protocol.ReconnectingClientFactory):
    maxDelay = 10
    protocol = MongoProtocol

    def __init__(self, manager, host='localhost', port=27017, pool_size=5):
        self.manager = manager
        self.idx = 0
        self.size = 0
        self.pool = []
        self.pool_size = pool_size
        self.deferred = defer.Deferred()
        self.host = host
        self.port = port
        self.isMaster = False
        self._checkedMaster = False
        self.disconnecting = False
        
        for i in range(self.pool_size):
            reactor.connectTCP(self.host, self.port, self)
        
    def __str__(self):
        return '<_MongoConnectionPool %s:%s (%s connections) - %s>' % (self.host, self.port, self.size, 'master' if self.isMaster else 'secondary')
    
    @defer.inlineCallbacks
    def append(self, conn):
        print 'Appending Connection:', self, conn
        if not self._checkedMaster:
            print 'Checking Master'
            self._checkedMaster = True
            info = yield conn.OP_QUERY("admin.$cmd", SON([ ('isMaster', 1) ]), 0, -1)
            info = info and info[0] or {}
            self.isMaster = info.get('ismaster', False)
        
            if 'hosts' in info:
                self.manager.updateHosts(info['hosts'])
            
        self.size += 1
        self.pool.append(conn)
        if self.deferred and self.size == self.pool_size:
            self.deferred.callback(self)
            self.deferred = None
    
    def checkMaster(self, deferred):
        print 'Checking Master For: ', self
        c = self.connection()
        d = c.OP_QUERY("admin.$cmd", SON([ ('isMaster', 1) ]), 0, -1)
        d.addCallback(self._checkMasterCallback, deferred)
        d.addErrback(self._checkMasterErrback, deferred)        
    
    def _checkMasterCallback(self, info, deferred):
        info = info and info[0] or {}
        self.isMaster = info.get('ismaster', False)
        
        if 'hosts' in info:
            self.manager.updateHosts(info['hosts'])
        
        deferred.callback(self.isMaster)
    
    def _checkMasterErrback(self, err, deferred):
        reactor.callLater(1, self.checkMaster, deferred)
    
    def remove(self, conn):
        print 'Removing Conn: ', self, conn
        try:
            self.pool.remove(conn)
        except:
            pass
        self.size = len(self.pool)
        
        if self.isMaster and not self.disconnecting:
            self.manager.master = None
            self.isMaster = False
            self.manager.checkMaster()
        
        if self.size == 0 and self.disconnecting:
            self.disconnecting.callback(True)
        
    def disconnectPool(self):
        if self.disconnecting is not False:
            return self.disconnecting

        self.stopTrying()
        self.disconnecting = defer.Deferred()
        for conn in self.pool:
            conn.transport.loseConnection()
        return self.disconnecting
    
    def connection(self):
        try:
            assert self.size
            conn = self.pool[self.idx % self.size]
            self.idx += 1
            self.idx = self.idx % self.size
        except Exception as ex:
            print ex
            return _offline()
        else:
            return conn
    

def MongoConnection(hosts=['localhost:27017'], pool_size=1, lazy=False):
    factory = _MongoConnectionManager(hosts, pool_size)
    return (lazy is True) and factory.API or factory.deferred
