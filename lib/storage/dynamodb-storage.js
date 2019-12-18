'use strict';

var _ = require('lodash');
// var crypto = require('crypto');
var Collection = require('mongomock/Collection');
var dynamodb = require('serverless-dynamodb-client');
// var AWS = require('aws-sdk');
var docClient = dynamodb.doc;

var bson = require('bson');
var ObjectID = bson.ObjectID;

var db = {}
db.scan = async (name, query = {}) => {
  console.log(">>>> called scan function", name);
  
  var params = {
    TableName : name,
    ...db.prepareParams(query)
  };

  console.log('>>>>>> scan params:', params);

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
// db.query = async (name, query = {}) => {
//   console.log(">>>> called query function", name, query);

//   var params = {
//     TableName : name,
//     ...db.prepareParams(query)
//     // FilterExpression: "#created_at >= :created_at",
//     // ExpressionAttributeNames:{
//     //     "#created_at": "created_at"
//     // },
//     // ExpressionAttributeValues: {
//     //     ":created_at": "2019-12-17T13:18:25.616Z"
//     // }
//   };

//   console.log('>>>>>> query params:', params);

//   var result = [];

//   try {
//     do {
//       var items = await docClient.scan(params).promise();
//       items.Items.forEach((item) => result.push(item));
//     } while(typeof items.LastEvaluatedKey != "undefined");
//   } catch (err) {
//     if (err.code == 'ResourceNotFoundException') {
//       return [];
//     }

//     throw new Error(err);
//   }

//   return result;
// }
db.prepareParams = (query = {}) => {
  if (!Object.keys(query).length) {
    return {};
  }

  var params = {
    "ExpressionAttributeNames": Object.keys(query).map(function(key) {
      return {["#"+key]: key}
    }).reduce(function(acc, cur) {
      var [key] = Object.keys(cur);
      acc[key] = cur[key];
      return acc;
    }),
    "ExpressionAttributeValues": Object.entries(query).map(function(obj) {
      let [value] = Object.values(obj[1]);
      return {[":"+obj[0]]: value}
    }).reduce(function(acc, cur) {
      var [key] = Object.keys(cur);
      acc[key] = cur[key];
      return acc;
    })
  }

  return params;
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

    // var collection = new Collection([]);

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
        console.log(">>>> called wrapper limit", limit);
        wrapper.limitCount = count;
        return wrapper;
      }
      , sort: function sort (query) {
        console.log(">>>> called wrapper sort", sort);
        wrapper.sortQuery = query;
        return wrapper;
      }
      , toArray: async function toArray(callback) {
        var results = [];

        // if (_.isEmpty(wrapper.findQuery)) {
          // scan
          // results = await db.scan(name);
        // } else {
          results = await db.scan(name, wrapper.findQuery);
          // query
        // }

        // console.log(results);

        callback(null, results);

        return wrapper;
      }
      , update: function update(query, modifier, options) {
        console.log(">>>> called wrapper update", query, modifier, options);
        return wrapper;
      }
      , insert: function insert(doc) {
        if (!doc._id || !(doc._id instanceof ObjectID)) {
          doc._id = new bson.ObjectId();
        }
        console.log(">>>> called wrapper insert", doc);
        return doc;
      }
      , save: function save() {
        console.log(">>>> called wrapper save");
        return wrapper;
      }
      , remove: function remove(query) {
        console.log(">>>> called wrapper remove", query);
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
