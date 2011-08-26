/**
 * 2011 Marco Rogers - Yammer Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var async = require('async')
  , pg = require("pg")
  , Client;

//if(pg.native) pg = pg.native;
Client = pg.Client;

// TODO: Seriously?!! No escape function?
var escape = require('mysql').Client.prototype.escape;

// TODO: wtf, need a proper upsert
// http://www.postgresql.org/docs/current/static/plpgsql-control-structures.html#PLPGSQL-UPSERT-EXAMPLE
var upsertFunc =  '' +
'CREATE FUNCTION upsert_key(_key VARCHAR, data TEXT) RETURNS VOID AS \n' +
'$$ \n' +
'BEGIN \n' +
'    LOOP \n' +
'        -- first try to update the key \n' +
'        UPDATE "store" SET "value" = data WHERE "key" = _key; \n' +
'        IF found THEN \n' +
'            RETURN; \n' +
'        END IF; \n' +
'        -- not there, so try to insert the key \n' +
'        -- if someone else inserts the same key concurrently, \n' +
'        -- we could get a unique-key failure \n' +
'        BEGIN \n' +
'            INSERT INTO "store" ("key", "value") VALUES (_key, data); \n' +
'            RETURN; \n' +
'        EXCEPTION WHEN unique_violation THEN \n' +
'            -- do nothing, and loop to try the UPDATE again \n' +
'        END; \n' +
'    END LOOP; \n' +
'END; \n' +
'$$ \n' +
'LANGUAGE plpgsql;';

var createQuery = {
    name: 'create'
    , text: 'CREATE TABLE "store" ( ' +
        '"key" VARCHAR( 100 ) NOT NULL PRIMARY KEY,' + 
        '"value" TEXT NOT NULL ' + 
        ' );'
  }
  , getQuery = {
    name: 'get'
    , text:'SELECT "value" FROM "store" WHERE "key" = $1;'
  }
  , setQuery = {
    name: 'set'
    , text: 'SELECT upsert_key($1, $2);'
  }
  , removeQuery = {
    name: 'remove'
    , text: 'DELETE FROM "store" WHERE "key" = $1;'
  }

exports.database = function(settings)
{
  this.db = new Client(settings);
  this.settings = settings;

  this.settings.cache = 1000;
  this.settings.writeInterval = 100;
  this.settings.json = true;
}

exports.database.prototype.init = function(callback)
{
  var self = this
    , connectionErrHandler = function(err) {
        self.db.end();
        if(self.db.stream) { self.db.stream.destroy(); }
        self.db = new Client(self.settings);
        callback && callback(err);
      }

  self.db.connect();
  self.db.on('error', connectionErrHandler);
  self.db.once('connect', function(err) {
    self.db.removeListener('error', connectionErrHandler);

    if(err) {
      return callback && callback(err);
    }

    var checkErrors = function (cb) {
      // These structures may already be there, so ignore that error,
      // return anything else
      return function(err) {
        if(err && (/already exists/i).test(err.message)) {
          err = null;
        }
        cb(err);
      }
    }

    async.waterfall([
      function(cb) {
        self.db.query(upsertFunc, checkErrors(cb));
      }
      , function(cb) {
        self.db.query(createQuery, checkErrors(cb));
      }
    ]
    , callback);
  });
}

exports.database.prototype.get = function (key, callback)
{
  this.db.query(getQuery, [key], function(err,results)
  {
    var value = null;
   
    if(!err && results.rows.length == 1)
    {
      value = results.rows[0].value;
    }
  
    callback(err,value);
  });
}

exports.database.prototype.set = function (key, value, callback)
{
  if(key.length > 100) {
    return callback(new Error("Your Key can only be 100 chars"));
  }

  // Careful! Ordering of key and value is reversed in sql
  this.db.query(setQuery, [key, value], callback);
}

exports.database.prototype.remove = function (key, callback)
{
  this.db.query(removeQuery, [key], callback);
}

exports.database.prototype.doBulk = function (bulk, callback)
{ 
  var self = this
    , sql = ['BEGIN;']
    , sqlPart
    , op
    , ctr = 1
    , values = [];

  for(var i in bulk)
  {
    op = bulk[i];
    if(op.type == "set")
    {
      sqlPart = 'SELECT upsert_key(' + escape(op.key) + ', ' + escape(op.value) + ');';
    }
    else if(op.type == "remove")
    {
      sqlPart = 'DELETE FROM "store" WHERE "key" =' + escape(op.key) + ';';
    }
    sql.push(sqlPart);
  }
  sql.push('COMMIT;');
  sql = sql.join('\n');

  self.db.query(sql, function(err) {  
    if(err)
    {
      self.db.query('ROLLBACK;');
    }

    callback(err);
  });
}

exports.database.prototype.close = function(callback)
{
  this.db.end();
  callback('Closed');
}
