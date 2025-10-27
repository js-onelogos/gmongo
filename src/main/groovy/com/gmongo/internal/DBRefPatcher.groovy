package com.gmongo.internal

import com.mongodb.DB
import com.mongodb.DBRef

import java.util.Collections
import java.util.Map
import java.util.WeakHashMap
import java.util.concurrent.ConcurrentHashMap

class DBRefPatcher {

  private static final Map<DBRef, DB> REF_TO_DB = Collections.synchronizedMap(new WeakHashMap<DBRef, DB>())
  private static final Map<String, DB> DB_BY_NAME = new ConcurrentHashMap<String, DB>()
  private static volatile boolean PATCHED = false

  static void register(DB db) {
    if (db == null) return
    DB_BY_NAME[db.getName()] = db
    ensurePatched()
  }

  static void attach(Object value, DB db) {
    if (value == null || db == null) return
    ensurePatched()
    register(db)
    if (value instanceof DBRef) {
      REF_TO_DB[value as DBRef] = db
      return
    }
    if (value instanceof Map) {
      (value as Map).values().each { attach(it, db) }
      return
    }
    if (value instanceof Iterable) {
      (value as Iterable).each { attach(it, db) }
    }
  }

  private static DB resolve(DBRef ref) {
    def db = REF_TO_DB[ref]
    if (db != null) {
      return db
    }
    def dbName = ref.databaseName
    if (dbName != null) {
      return DB_BY_NAME[dbName]
    }
    return null
  }

  private static void ensurePatched() {
    if (PATCHED) return
    synchronized (DBRefPatcher) {
      if (PATCHED) return
      DBRef.metaClass.fetch = { ->
        def db = resolve(delegate as DBRef)
        if (db == null) {
          throw new IllegalStateException('Cannot resolve database for DBRef.fetch(); pass a DB explicitly')
        }
        return db.getCollection(delegate.collectionName).findOne(delegate.id)
      }
      DBRef.metaClass.fetch = { DB override ->
        if (override != null) {
          register(override)
          REF_TO_DB[delegate as DBRef] = override
          return override.getCollection(delegate.collectionName).findOne(delegate.id)
        }
        delegate.fetch()
      }
      PATCHED = true
    }
  }
}
