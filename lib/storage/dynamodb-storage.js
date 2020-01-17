'use strict';

var _ = require('lodash');
// var crypto = require('crypto');
// var Collection = require('mongomock/Collection');
var dynamodb = require('serverless-dynamodb-client');
// var AWS = require('aws-sdk');
var docClient = dynamodb.doc;

var bson = require('bson');
var ObjectID = bson.ObjectID;

var db = {}
db.scan = async (name, wrapper) => {
  console.log(">>>> called scan function", name);
  
  var params = {
    TableName: name,
    ...db.prepareParams(wrapper.findQuery)
  };

  console.log('>>>>>> scan params:', params);

  var result = [];

  try {
    do {
      var data = await docClient.scan(params).promise();
      if (wrapper.sortQuery) {
        let query = wrapper.sortQuery;
        data.Items.sort(function(a, b) {
          let [sortKey] = Object.keys(query);
          if (a[sortKey] < b[sortKey]) {
            return -1 * query[sortKey];
          }
          if (a[sortKey] > b[sortKey]) {
            return 1 * query[sortKey];
          }
          return 0;
        })
      }
      data.Items.forEach((item) => result.push(item));
    } while(typeof data.LastEvaluatedKey != "undefined");
  } catch (err) {
    if (err.code == 'ResourceNotFoundException') {
      return [];
    }

    throw new Error(err);
  }

  if (wrapper.limitCount) {
    result = result.slice(0, wrapper.limitCount);
  }

  console.log(">>>results", result);

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

  // console.log(">>>>query", query);

  var params = {
    "ExpressionAttributeNames": Object.keys(query).map(function(key) {
      return {["#"+key]: key}
    }).reduce(function(acc, cur) {
      var [key] = Object.keys(cur);
      acc[key] = cur[key];
      return acc;
    }),
    "ExpressionAttributeValues": Object.entries(query).map(function(obj) {
      let value = obj[1];
      if (typeof value === 'object' ) {
        [value] = Object.values(value);
      }
      return {[":"+obj[0]]: value}
    }).reduce(function(acc, cur) {
      var [key] = Object.keys(cur);
      acc[key] = cur[key];
      return acc;
    }),
    "FilterExpression": Object.entries(query).map(function(obj) {
      let [field, expression] = obj;
      var op = "$eq";
      if (typeof expression === 'object' ) {
        [op] = Object.entries(expression)[0];
      }

      switch(op) {
        case '$eq':
          op = '='
          break;
        case '$lt':
          op = '<'
          break;
        case '$gt':
          op = '>'
          break;
        case '$gte':
          op = ">="
          break;
        case '$lte':
          op = "<="
          break;
        default:
          throw new Error("unknown operation: "+op);
      }

      return "#"+field+" "+op+" "+":"+field;
    }).reduce(function(acc, cur) {
      acc = acc + ' AND ' + cur;
      return acc;
    })
  }

  return params;
}

function init (env, callback) {

  if (!env.storageURI || !_.isString(env.storageURI)) {
    throw new Error('dynamodb config uri is missing or invalid');
  }

  // var configPath = env.storageURI.split('dynamodb://').pop();
  // console.log(configPath);

  function reportAsCollection (name) {
    // console.log('>>>reportAsCollection called:', name);
    var data = { };
    data[name] = [];

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
        // console.log(">>>> called wrapper limit", count);
        wrapper.limitCount = count;
        return wrapper;
      }
      , sort: function sort (query) {
        // console.log(">>>> called wrapper sort", query);
        wrapper.sortQuery = query;
        return wrapper;
      }
      , toArray: async function toArray(callback) {
        var results = [];

        // if (_.isEmpty(wrapper.findQuery)) {
          // scan
          // results = await db.scan(name);
        // } else {
          results = await db.scan(name, wrapper);
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

  var FakeDb = (function() {
    function Db() {}
    Db.prototype.admin = function() {
      function FakeAdmin() {}
      FakeAdmin.prototype.buildInfo = function(obj, cb) {
        let info = {
          "version" : "2012-08-10",
          "gitVersion" : "",
          "sysInfo" : "",
          "loaderFlags" : "",
          "compilerFlags" : "",
          "allocator" : "",
          "versionArray" : [],
          "javascriptEngine" : "V8",
          "bits" : 1,
          "debug" : false,
          "maxBsonObjectSize" :1,
          "ok" : 1
        }
        cb(null, info);
      }
      return new FakeAdmin();
    }
    return Db;
  })();

  try {
    callback(null, {
      collection: reportAsCollection,
      ensureIndexes: _.noop,
      db: new FakeDb()
    });
  } catch (err) {
    callback(err);
  }
}

module.exports = init;
