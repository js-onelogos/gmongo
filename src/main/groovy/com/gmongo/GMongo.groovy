/*
Copyright 2010-2014 Paulo Poiati

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.gmongo

import java.util.List;

import com.mongodb.DB
import com.mongodb.Mongo
import com.mongodb.MongoClient
import com.mongodb.MongoClientOptions
import com.mongodb.MongoClientURI
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern

import com.gmongo.internal.DBPatcher

class GMongo {

  @Delegate
  Mongo mongo

  static DB connect(String host, int port, String dbName) {
    def mongoClient = new MongoClient(host, port)
    patchAndReturn mongoClient.getDB(dbName)
  }

  static DB connect(String host, int port, String dbName, MongoClientOptions options) {
    def mongoClient = new MongoClient(new ServerAddress(host, port), options)
    patchAndReturn mongoClient.getDB(dbName)
  }

  static DB connect(String connectionString) {
    connect(new MongoClientURI(connectionString))
  }

  static DB connect(MongoClientURI uri) {
    def dbName = uri.database
    if (!dbName) {
      throw new IllegalArgumentException("MongoClientURI must include a database name to obtain a DB reference")
    }
    def mongoClient = new MongoClient(uri)
    patchAndReturn mongoClient.getDB(dbName)
  }

  GMongo(Mongo mongo) {
    this.mongo = mongo
  }

  GMongo() {
    this.mongo = new MongoClient(new ServerAddress())
  }

  GMongo(ServerAddress addr) {
    this.mongo = new MongoClient(addr)
  }

  GMongo(ServerAddress addr, MongoClientOptions opts) {
    this.mongo = new MongoClient(addr, opts)
  }

  GMongo(ServerAddress left, ServerAddress right) {
    this.mongo = new MongoClient([ left, right ])
  }

  GMongo(ServerAddress left, ServerAddress right, MongoClientOptions opts) {
    this.mongo = new MongoClient([ left, right ], opts)
  }

  GMongo(String host) {
    this.mongo = buildClientForHost(host)
  }

  GMongo(String host, Integer port) {
    this.mongo = new MongoClient(host, port)
  }

  GMongo(String host, MongoClientOptions opts) {
    this.mongo = new MongoClient(new ServerAddress(host), opts)
  }

  GMongo( List<ServerAddress> replicaSetSeeds, MongoClientOptions opts ) {
    this.mongo = new MongoClient(replicaSetSeeds, opts)
  }

  GMongo( List<ServerAddress> replicaSetSeeds) {
    this.mongo = new MongoClient(replicaSetSeeds)
  }
  
  GMongo( MongoClientURI mongoURI ) {
    this.mongo = new MongoClient( mongoURI )
  }

  DB getDB(String name) {
    patchAndReturn mongo.getDB(name)
  }

  void setWriteConcern(WriteConcern writeConcern) {
    mongo.setWriteConcern(writeConcern)
  }

  static private patchAndReturn(db) {
    DBPatcher.patch(db); return db
  }

  private static MongoClient buildClientForHost(String host) {
    if (host?.startsWith("mongodb://")) {
      return new MongoClient(new MongoClientURI(host))
    }
    return new MongoClient(host)
  }
}
