'use strict';

var _ = require('lodash');
var crypto = require('crypto');
var Collection = require('mongomock/Collection');
var dynamodb = require('serverless-dynamodb-client');
var AWS = require('aws-sdk');
var docClient = dynamodb.doc;

var db = {}
db.scan = async (name) => {
  console.log(">>>> called scan function", name);
  
  var params = {
    TableName : name,
  };

  console.log(params);

  var result = [];

  try {
    do {
      var items = await docClient.scan(params).promise();
      items.Items.forEach((item) => result.push(item));
    } while(typeof items.LastEvaluatedKey != "undefined");
  } catch (err) {
    if (err.code == 'ResourceNotFoundException') {
      return [];
    }

    throw new Error(err);
  }

  return result;
}
db.query = async (name, query = {}) => {
  console.log(">>>> called query function", name, query);
}

function init (env, callback) {

  if (!env.storageURI || !_.isString(env.storageURI)) {
    throw new Error('dynamodb config uri is missing or invalid');
  }

  var configPath = env.storageURI.split('dynamodb://').pop();
  console.log(configPath);

  function reportAsCollection (name) {
    console.log('>>>reportAsCollection called:', name);
    var data = { };
    data[name] = [];

    var collection = new Collection([]);

    console.log(data);

    var wrapper = {
      findQuery: null
      , sortQuery: null
      , limitCount: null
      , find: function find (query) {
        query = _.cloneDeepWith(query, function booleanize (value) {
          //TODO: for some reason we're getting {$exists: NaN} instead of true/false
          if (value && _.isObject(value) && '$exists' in value) {
            return {$exists: true};
          }
        });
        wrapper.findQuery = query;
        return wrapper;
      }
      , limit: function limit (count) {
        wrapper.limitCount = count;
        return wrapper;
      }
      , sort: function sort (query) {
        wrapper.sortQuery = query;
        return wrapper;
      }
      , toArray: async function toArray(callback) {
        var results = [];

        if (_.isEmpty(wrapper.findQuery)) {
          // scan
          results = await db.scan(name);
        } else {
          results = await db.query(name, wrapper.findQuery);
          // query
        }

        console.log(results);

        callback(null, results);

        return wrapper;
      }
    };

    return wrapper;
  }

  try {
    callback(null, {
      collection: reportAsCollection
      , ensureIndexes: _.noop
    });
  } catch (err) {
    callback(err);
  }
}

module.exports = init;
