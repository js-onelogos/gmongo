package com.gmongo

import com.mongodb.ServerAddress
import com.mongodb.WriteConcern

class IntegrationTestCase extends GroovyTestCase {

  static DB_NAME = 'gmongo_test'

  def mongo, db

  void setUp() {
    mongo = new GMongo(new ServerAddress('localhost', 27017))
    mongo.setWriteConcern(WriteConcern.ACKNOWLEDGED)
    db = mongo.getDB(DB_NAME)
  }

  void testNothing() {}
}
