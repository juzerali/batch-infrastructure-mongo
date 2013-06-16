/**
* Module dependencies
*/

var	EventEmitter = require("events").EventEmitter
,	util = require("util")
,	mongoskin = require("mongoskin");


var infrastructure = {};
module.exports = infrastructure;

infrastructure.getInputDatasource = function(options) {
	return new InputDatasource(options);
}

infrastructure.getOutputDatasource = function(options) {
	return new OutputDatasource(options);
}

/**
* Required
*/
function InputDatasource (options) {
	var self = this;
	var options = options || {};
	self.db = mongoskin.db(options.db, {safe: false})
	self.collection = options.collection;
	self.line = 0;
	EventEmitter.call(self);
}

InputDatasource.prototype.__defineGetter__("type", function(){
	return "mongo";
});

InputDatasource.prototype.__defineGetter__("callback", function(){
	return true;
});

util.inherits(InputDatasource, EventEmitter);

InputDatasource.prototype.read = function(err, data) {
	var self = this;
	self.emit("data", err, data);
};

/**
* Required
*/
function OutputDatasource (options) {
	var self = this;
	options = self.options = options || {};
	self.collection = options.collection;
	self.db = mongoskin.db(options.db, {safe: false});
	self.writer();
}

OutputDatasource.prototype.write = function(data) {
	this._writer(this.db, data);
};

OutputDatasource.prototype.writer = function(_writer){
	var self = this;
	self._writer = (typeof _writer === 'function' && _writer) || function(db, data){
		self.db.collection(self.collection).save(data, {"upsert": true})
	};
}


function ensureFunction(fn){
	if(!typeof fn === "function")
	throw new Error(fn + ": Expected function but got " + typeof fn);
}