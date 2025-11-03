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

import com.mongodb.AggregationOptions
import com.mongodb.BasicDBObject
import com.mongodb.Cursor
import com.mongodb.DBCollection
import com.mongodb.DBObject
import com.mongodb.MongoCommandException
import com.mongodb.ReadPreference
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

    aggregate: { Object... rawArgs ->
      _aggregate(delegate as DBCollection, rawArgs as Object[])
    },

    apply: { DBObject o ->
      if (!o.containsField('_id')) {
        o.put('_id', new ObjectId())
      }
      delegate.insert(o)
      return o.get("_id")
    },

    group: { Object... rawArgs ->
      _group(delegate as DBCollection, rawArgs as Object[])
    },

    setHintFields: { Object hints ->
      _setHintFields(hints)
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

  private static AggregationResultAdapter _aggregate(DBCollection collection, Object[] args) {
    def arguments = (args ? args.toList() : [])
    def readPreference = _extractReadPreference(arguments)
    def options = _extractAggregationOptions(arguments)
    def pipeline = _buildPipeline(arguments)
    def cursor = readPreference ?
      collection.aggregate(pipeline, options, readPreference) :
      collection.aggregate(pipeline, options)
    return new AggregationResultAdapter(cursor)
  }

  private static AggregationOptions _extractAggregationOptions(List arguments) {
    if (!arguments) {
      return AggregationOptions.builder().build()
    }
    def last = arguments.last()
    if (last instanceof AggregationOptions) {
      arguments.remove(arguments.size() - 1)
      return last as AggregationOptions
    }
    return AggregationOptions.builder().build()
  }

  private static ReadPreference _extractReadPreference(List arguments) {
    if (!arguments) {
      return null
    }
    def last = arguments.last()
    if (last instanceof ReadPreference) {
      arguments.remove(arguments.size() - 1)
      return last as ReadPreference
    }
    return null
  }

  private static List<DBObject> _buildPipeline(List stages) {
    def pipeline = []
    stages.each { candidate ->
      _collectPipelineStage(candidate, pipeline)
    }
    if (pipeline.isEmpty()) {
      throw new MissingMethodException('aggregate', DBCollection, stages as Object[])
    }
    return pipeline
  }

  private static void _collectPipelineStage(Object candidate, List<DBObject> pipeline) {
    if (candidate == null) {
      return
    }
    if (candidate instanceof DBObject) {
      pipeline << (candidate as DBObject)
      return
    }
    if (candidate instanceof Collection) {
      (candidate as Collection).each { nested ->
        _collectPipelineStage(nested, pipeline)
      }
      return
    }
    if (candidate.getClass().isArray()) {
      ((Object[]) candidate).each { nested ->
        _collectPipelineStage(nested, pipeline)
      }
      return
    }
    throw new MissingMethodException('aggregate', DBCollection, [candidate] as Object[])
  }

  private static Object _group(DBCollection collection, Object[] args) {
    def arguments = (args ? args.toList() : [])
    def readPreference = _extractReadPreference(arguments)
    DBObject groupOptions = null
    if (arguments.size() > 4 && (arguments.last() instanceof DBObject)) {
      groupOptions = arguments.remove(arguments.size() - 1) as DBObject
    }
    Object finalizeFunction = null
    if (arguments.size() > 4) {
      finalizeFunction = arguments.remove(arguments.size() - 1)
    }
    if (arguments.size() != 4) {
      throw new MissingMethodException('group', DBCollection, args)
    }
    def key = arguments[0] as DBObject
    def cond = arguments[1] as DBObject
    def initial = arguments[2] as DBObject
    def reduce = arguments[3]
    def command = new BasicDBObject('ns', collection.getName())
    if (key != null) {
      if (key instanceof DBObject) {
        command.put('key', key)
      } else {
        command.put('keyf', key)
      }
    }
    if (cond != null) {
      command.put('cond', cond)
    }
    if (initial != null) {
      command.put('initial', initial)
    }
    if (reduce != null) {
      command.put('$reduce', reduce)
    }
    if (finalizeFunction != null) {
      command.put('finalize', finalizeFunction)
    }
    if (groupOptions != null) {
      command.putAll(groupOptions.toMap())
    }
    def groupCommand = new BasicDBObject('group', command)
    def result = readPreference ?
      collection.getDB().command(groupCommand, readPreference) :
      collection.getDB().command(groupCommand)
    result.throwOnError()
    return result.get('retval')
  }

  private static void _setHintFields(Object hints) {
    // Driver 4.x removed setHintFields; keep method for compatibility by validating input only.
    if (hints == null) {
      return
    }
    if (hints instanceof DBObject) {
      return
    }
    if (hints instanceof Collection) {
      (hints as Collection).each { entry ->
        if (entry != null && !(entry instanceof DBObject)) {
          throw new MissingMethodException('setHintFields', DBCollection, [hints] as Object[])
        }
      }
    }
  }

  private static class AggregationResultAdapter implements Iterable<DBObject> {
    private final List<DBObject> results

    AggregationResultAdapter(Cursor cursor) {
      def collected = []
      if (cursor != null) {
        try {
          while (cursor.hasNext()) {
            collected << cursor.next()
          }
        } finally {
          cursor.close()
        }
      }
      this.results = collected.asImmutable()
    }

    List<DBObject> results() {
      return results
    }

    @Override
    Iterator<DBObject> iterator() {
      return results.iterator()
    }

    DBObject getAt(int index) {
      return results[index]
    }
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
