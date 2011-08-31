/**
 * 2011 Peter 'Pita' Martischka
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

try
{
  var sqlite3 = require("sqlite3");  
}
catch(e)
{
  console.error("FATAL: The sqlite dependency could not be loaded. We removed it from the dependencies since it caused problems on several Platforms to compile it. If you still want to use sqlite, do a 'npm install sqlite3' in your etherpad-lite root folder");
  process.exit(1);
}

var async = require("async")
  , util = require('util')
  , events = require('events') 
  ;
exports.database = function(settings)
{
  this.db=null; 
  
  if(!settings || !settings.filename)
  {
    settings = {filename:":memory:"};
  }
  
  this.settings = settings;
  
  //set settings for the dbWrapper
  if(settings.filename == ":memory:")
  {
    this.settings.cache = 0;
    this.settings.writeInterval = 0;
    this.settings.json = true;
  }
  else
  {
    this.settings.cache = 1000;
    this.settings.writeInterval = 100;
    this.settings.json = true;
  }
}
util.inherits(exports.database, events.EventEmitter)

exports.database.prototype.init = function(callback)
{
  var _this = this
    , starttime = new Date().getTime()
    ;
  
  async.waterfall([
    function(callback)
    {
      _this.db = new sqlite3.cached.Database(_this.settings.filename, callback);
    },
    function(callback)
    {
      var sql = "CREATE TABLE IF NOT EXISTS store (key TEXT PRIMARY KEY,value TEXT)";
      _this.db.run(sql, function () {
        _this.emit('metric.init', (new Date()).getTime() - starttime)
        callback.apply(this, arguments)
      });
    } 
  ],callback);
}

exports.database.prototype.get = function (key, callback)
{
  var self = this
    , starttime = (new Date()).getTime()
    ;
  self.db.get("SELECT value FROM store WHERE key = ?", key, function(err,row)
  {
    self.emit('metric.get', (new Date()).getTime() - starttime)
    callback(err,row ? row.value : null);
  });
}

exports.database.prototype.set = function (key, value, callback)
{
  var self = this
    , starttime = (new Date()).getTime()
    ;
  self.db.run("REPLACE INTO store VALUES (?,?)", key, value, function () {
    self.emit('metric.set', (new Date()).getTime() - starttime)
    callback.apply(this, arguments)
  });
}

exports.database.prototype.remove = function (key, callback)
{
  var self = this
    , starttime = (new Date()).getTime()
    ;
  self.db.run("DELETE FROM store WHERE key = ?", key, function () {
    self.emit('metric.remove', (new Date()).getTime() - starttime)
    callback.apply(this, arguments)
  });
}

exports.database.prototype.doBulk = function (bulk, callback)
{ 
  var sql = "BEGIN TRANSACTION;\n"
    , self = this
    , starttime = (new Date()).getTime()
    ;
  for(var i in bulk)
  {
    if(bulk[i].type == "set")
    {
      sql+="REPLACE INTO store VALUES (" + escape(bulk[i].key) + ", " + escape(bulk[i].value) + ");\n";
    }
    else if(bulk[i].type == "remove")
    {
      sql+="DELETE FROM store WHERE key = " + escape(bulk[i].key) + ";\n";
    }
  }
  sql += "END TRANSACTION;";
  
  self.db.exec(sql, function(err){
    self.emit('metric.bulk', (new Date()).getTime() - starttime)
    if(err)
    {
      console.error("ERROR WITH SQL: ");
      console.error(sql);
    }
    
    callback(err);
  });
}

exports.database.prototype.close = function(callback)
{
  this.db.close();
  callback(null)
}

function escape (val) 
{
  return "'"+val.replace(/'/g, "''")+"'";
};
