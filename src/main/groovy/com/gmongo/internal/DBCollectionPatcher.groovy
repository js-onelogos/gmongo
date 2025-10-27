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
package com.gmongo.internal

import com.mongodb.DBObject
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.MongoCommandException
import groovy.lang.MissingMethodException
import org.bson.types.ObjectId

class DBCollectionPatcher {
  
  static final PATCHED_METHODS = [ 
    'insert', 'find', 'findOne', 'findAndModify', 'findAndRemove', 'remove', 'save', 'count', 'update', 
    'updateMulti', 'distinct', 'apply', 'createIndex', 'ensureIndex',
    'mapReduce', 'dropIndex', 'getCount', 'group', 'setHintFields', 'aggregate'
  ]

  static final ALIAS = [
    leftShift : 'insert', 
    rightShift: 'remove'
  ]

  static final ADDITIONAL_METHODS = [
    update: { DBObject q, DBObject o, Boolean upsert ->
      delegate.update(q, o, upsert, false)
    },

    apply: { DBObject o ->
      if (!o.containsField('_id')) {
        o.put('_id', new ObjectId())
      }
      delegate.insert(o)
      return o.get("_id")
    },

    ensureIndex: { DBObject keys, Object arg2 = null, Object arg3 = null ->
      _ensureIndex(delegate as DBCollection, keys, arg2, arg3)
    }
  ]
  
  private static final COPY_GENERATED_ID = { defaultArgs, invokeArgs, result, collection = null ->
    MirrorObjectMutation.copyGeneratedId(invokeArgs.first(), defaultArgs.first())
  }

  static final AFTER_RETURN = [
    apply: { defaultArgs, invokeArgs, result, DBCollection collection ->
      defaultArgs[0]._id = result
    },
    
    find: { defaultArgs, invokeArgs, result, DBCollection collection ->
      DBCursorPatcher.patch(result)
    },

    findOne: { defaultArgs, invokeArgs, result, DBCollection collection ->
      DBRefPatcher.attach(result, collection?.getDB())
    },

    findAndModify: { defaultArgs, invokeArgs, result, DBCollection collection ->
      DBRefPatcher.attach(result, collection?.getDB())
    },

    findAndRemove: { defaultArgs, invokeArgs, result, DBCollection collection ->
      DBRefPatcher.attach(result, collection?.getDB())
    },

    save: COPY_GENERATED_ID, insert: COPY_GENERATED_ID
  ]

  static patch(c) {
    if (c.hasProperty(Patcher.PATCH_MARK))
      return
    _addCollectionTruth(c)
    Patcher._patchInternal c, PATCHED_METHODS, ALIAS, ADDITIONAL_METHODS, AFTER_RETURN
  }
  
  private static _addCollectionTruth(c) {
    c.metaClass.asBoolean { -> delegate.count() > 0 }
  }

  private static void _ensureIndex(DBCollection collection, DBObject keys, Object arg2, Object arg3) {
    try {
      if (arg2 == null) {
        collection.createIndex(keys)
        return
      }

      if (arg2 instanceof DBObject) {
        collection.createIndex(keys, arg2 as DBObject)
        return
      }

      if (arg2 instanceof String) {
        if (arg3 instanceof Boolean) {
          collection.createIndex(keys, arg2 as String, arg3 as Boolean)
          return
        }
        collection.createIndex(keys, arg2 as String)
        return
      }

      def args = [keys, arg2, arg3].findAll { it != null } as Object[]
      throw new MissingMethodException('ensureIndex', DBCollection, args)
    } catch (MongoCommandException ex) {
      if (!_isIndexAlreadyExistsError(ex)) {
        throw ex
      }
    }
  }

  private static boolean _isIndexAlreadyExistsError(MongoCommandException ex) {
    return ex.code == 85 || ex.codeName == 'IndexOptionsConflict'
  }
}

class MirrorObjectMutation {
  
  static void copyGeneratedId(Object[] from, Object[] to) {
    copyGeneratedId(from as List, to as List)
  }
  
  static void copyGeneratedId(List from, List to) {
    from.size().times {
      copyGeneratedId(from[it], to[it])
    }
  }
  
  static void copyGeneratedId(Map from, Map to) {
    to._id = from._id
  }
  
}
