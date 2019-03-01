var debug = require("debug")("dme:store:solr");
var MongoClient = require('mongodb').MongoClient 
var Deferred = require("promised-io/promise").defer;
var All= require("promised-io/promise").all;
var Sequence= require("promised-io/promise").seq;
var Query = require("rql/query");
var LazyArray = require("promised-io/lazy-array").LazyArray;
var StoreBase=require("dactic/store");
var util = require("util");
var defer = require("promised-io/promise").defer;
var when = require("promised-io/promise").when;
var ObjectID = require('bson').ObjectID;
var jsArray = require("rql/js-array");
var RQ = require("rql/parser");
var Result = require("dactic/result");

var Store = module.exports = function(id,options){
	StoreBase.apply(this,arguments);
}

util.inherits(Store, StoreBase);

Store.prototype.authConfigProperty="solr";
Store.prototype.primaryKey="id";
	
Store.prototype.connect=function(){
	var def = new defer()
	if (this.options && this.options.url) {
		debug("Creating MongoClient client @ " + this.options.url + "/" + this.id);
		var _self=this;
		MongoClient.connect(this.options.url , function(err,client){
			if (err){
				def.reject(Error("Unable to connect to MongoDB: " + err));
			}
			_self.client=client;
			def.resolve(client);
			
		});
	}else{
		def.reject(new Error("Missing MongoDB configuration in Store Init"));
	}

	return def.promise;
}

Store.prototype.getSchema=function(){
	debug("getSchema()");
	return this.schema || {}	
}

Store.prototype.setSchema=function(schema){
	debug("mongodb: setSchema()", schema);
	this.schema=schema;
	var _self=this;
	var db = this.client;
	var def = new Deferred()
	var colDef = new Deferred();
	if (this.client){

		_self.client.collection(_self.id, function(err,col){
			if (err){
				console.warn("Error Retrieving Collection: " + err);
				return colDef.reject(err)
			}
			col.stats(function(err,stats){
				if (!stats){
					_self.client.createCollection(_self.id, function(err,col){
						colDef.resolve(col);
					})
				}else{
					colDef.resolve(col)
				}
			})

		}, function(err){
			colDef.reject("Unable to find or create collection: " + _self.id)
		})
	}else{
		colDef=false;
		def.resolve(true);
	}

	//ensure index;
	if (colDef){
		when(colDef, function(col){
			debug("Check for Indexes");
			var indexes=[]
			if (_self.schema && _self.schema.properties){
				Object.keys(_self.schema.properties).forEach(function(prop){
					if (_self.schema.properties[prop] && _self.schema.properties[prop].index){
						var spec = {name: prop, unique: _self.schema.properties[prop].unique?true:false};
						indexes.push(col.createIndex(prop,{w:1, unique:_self.schema.properties[prop].unique?true:false }));
					}
				})
				when(All(indexes), function(){
					def.resolve(true);
				})

			}else{
				def.resolve(true);
			}
		}, function(err){
			def.reject(err);
		})
	}

	return def.promise;

}

Store.prototype.query=function(query, opts){
	var _self=this;
	var deferred = new defer();
	//var query = new Query(query);
	if (!this.client){ return deferred.reject(Error("Not Connected")); }
	var collection = this.client.collection(this.collectionId || this.id);

	// compose search conditions
	var x = parse(query, opts);
	var meta = x[0], search = x[1];

	// range of non-positive length is trivially empty
	//if (options.limit > options.totalCount)
	//	options.limit = options.totalCount;
	if (meta.limit <= 0) {
		var results = [];
		results.totalCount = 0;
		return results;
	}

	// request full recordset length
	// N.B. due to collection.count doesn't respect meta.skip and meta.limit
	// we have to correct returned totalCount manually.
	// totalCount will be the minimum of unlimited query length and the limit itself
	var totalCountPromise = (meta.totalCount) ?
		when(callAsync(collection.count, [search]), function(totalCount){
			totalCount -= meta.lastSkip;
			if (totalCount < 0)
				totalCount = 0;
			if (meta.lastLimit < totalCount)
				totalCount = meta.lastLimit;
			// N.B. just like in rql/js-array
			return Math.min(totalCount, typeof meta.totalCount === "number" ? meta.totalCount : Infinity);
		}) : undefined;

	//console.log("SEARCH: ", search);
	//console.log("META: ", meta);

	collection.find(search, meta, function(err, cursor){
		if (err) return deferred.reject(err);
		cursor.toArray(function(err, results){
			if (err) return deferred.reject(err);
			// N.B. results here can be [{$err: 'err-message'}]
			// the only way I see to distinguish from quite valid result [{_id:..., $err: ...}] is to check for absense of _id
			if (results && results[0] && results[0].$err !== undefined && results[0]._id === undefined) {
				return deferred.reject(results[0].$err);
			}
			var fields = meta.fields;
			var len = results.length;
			// damn ObjectIDs!
			for (var i = 0; i < len; i++) {
				delete results[i]._id;
			}
			// kick out unneeded fields
			if (fields) {
				// unhash objects to arrays
				if (meta.unhash) {
					results = jsArray.executeQuery('values('+fields+')', directives, results);
				}
			}
			// total count
			when(totalCountPromise, function(result){
				var metadata = {}
				metadata.count = results.length;
				metadata.start = meta.skip;
				metadata.end = meta.skip + results.count;
				metadata.totalCount = result;

				debug("MongoDB Store Results: ", results)
				debug("   Result Meta: ", metadata);
				deferred.resolve(new Result(results,metadata))
			});
		});
	});

	return deferred.promise;
}

Store.prototype.get=function(id, opts){
	var _self = this;
	var def = new defer();
	var query = {};
	query[this.primaryKey] = id;
	var collection = this.client.collection(this.collectionId || this.id);
	collection.find(query).toArray(function(err,docs){
		if (docs[0]) { 
			delete docs[0]._id; 
			return def.resolve(new Result(docs[0])); 
		}
		def.reject();
	});
	return def.promise;
}

Store.prototype.post=function(obj, opts){
	return when(this.put(obj,opts),function(results){
		var obj = results.results;
		//return obj;
		return new Result(obj);
	});
}

Store.prototype.put=function(obj, opts){
	var deferred = defer();
	opts = opts || {};
	var search = {id: obj[this.primaryKey]};
	var collection = this.client.collection(this.collectionId || this.id);
	if (!opts.overwrite) {
		// do an insert, and check to make sure no id matches first
		collection.findOne(search, function(err, found){
			if (err) return deferred.reject(err);
			if (found === null) {
				if (!obj.id) obj.id = ObjectID.createPk().toJSON();
				collection.insertOne(obj, function(err, robj){
					if (err) return deferred.reject(err);
					// .insert() returns array, we need the first element
					robj = robj && robj[0];
					if (robj) delete robj._id;
					deferred.resolve(new Result(robj));
				});
			} else {
				deferred.reject(id + " exists, and can't be overwritten");
			}
		});
	} else {
		collection.update(search, obj, {upsert: opts.overwrite}, function(err, res){
			if (err) return deferred.reject(err);
			if (obj) delete obj._id;
			deferred.resolve(new Result(obj));
		});
	}

	return deferred.promise;
}

Store.prototype.delete=function(obj,opts){
	var deferred = defer();
	collection.remove(search, function(err, result){
		if (err) return deferred.reject(err);
		deferred.resolve(undefined);
	});
	return deferred;
}
	


function parse(query, directives){
	// parse string to parsed terms
	if(typeof query === "string"){
		// handle $-parameters
		// TODO: consider security issues
		//// N.B. considered, treated as evil, bump
		//throw new URIError("Sorry, we don't allow raw querystrings. Please, provide the parsed terms instead");
		if (directives && directives.parameters) {
			query = query.replace(/\$[1-9]/g, function(param){
				return directives.parameters[param.substring(1) - 1];
			});
		}
		// poorman regexp? *foo, bar*
		/***v = (v.charAt(0) != '*') ? '^' + v : v.substring(1);
		v = (v.slice(-1) != '*') ? v + '$' : v.substring(0, v.length-1);***/
		query = RQ.parseQuery(query);
	}
	var options = {
		skip: 0,
		limit: +Infinity,
		lastSkip: 0,
		lastLimit: +Infinity
	};
	var search = {};
//	var needBulkFetch = directives && directives.postprocess; // whether to fetch whole dataset to process it here
//if (!needBulkFetch) {

	function walk(name, terms) {
		// valid funcs
		var valid_funcs = ['lt','lte','gt','gte','ne','in','nin','not','mod','all','size','exists','type','elemMatch'];
		// funcs which definitely require array arguments
		var requires_array = ['in','nin','all','mod'];
		// funcs acting as operators
		var valid_operators = ['or', 'and'];//, 'xor'];
		// compiled search conditions
		var search = {};
		// iterate over terms
		terms.forEach(function(term){
			var func = term.name;
			var args = term.args;
			// ignore bad terms
			// N.B. this filters quirky terms such as for ?or(1,2) -- term here is a plain value
			if (!func || !args) return;
			//dir(['W:', func, args]);
			// process well-known functions
			// http://www.mongodb.org/display/DOCS/Querying
			if (func == 'sort' && args.length > 0) {
				options.sort = args.map(function(sortAttribute){
					var firstChar = sortAttribute.charAt(0);
					var orderDir = 'ascending';
					if (firstChar == '-' || firstChar == '+') {
						if (firstChar == '-') {
							orderDir = 'descending';
						}
						sortAttribute = sortAttribute.substring(1);
					}
					return [sortAttribute, orderDir];
				});
			} else if (func == 'select') {
				options.fields = args;
			} else if (func == 'values') {
				options.unhash = true;
				options.fields = args;
				// N.B. mongo has $slice but so far we don't allow it
			/*} else if (func == 'slice') {
				options[args.shift()] = {'$slice': args.length > 1 ? args : args[0]};*/
			} else if (func == 'limit') {
				// we calculate limit(s) combination
				options.lastSkip = options.skip;
				options.lastLimit = options.limit;
				// TODO: validate args, negative args
				var l = args[0] || Infinity, s = args[1] || 0;
				// N.B: so far the last seen limit() contains Infinity
				options.totalCount = args[2];
				if (l <= 0) l = 0;
				if (s > 0) options.skip += s, options.limit -= s;
				if (l < options.limit) options.limit = l;
				// grouping
			} else if (func == 'group') {
				// TODO:
				// nested terms? -> recurse
			} else if (args[0] && typeof args[0] === 'object') {
				if (valid_operators.indexOf(func) > -1){
					if (func=="and") {
						search['$'+func] = [walk(func, args)];
					}else{
						search["$"+func] = walk(func,args);
					}
				}
				// N.B. here we encountered a custom function
				// ...
				// structured query syntax
				// http://www.mongodb.org/display/DOCS/Advanced+Queries
			} else {
				//dir(['F:', func, args]);
				// mongo specialty
				if (func == 'le') func = 'lte';
				else if (func == 'ge') func = 'gte';
				// the args[0] is the name of the property
				var key = args.shift();
				// the rest args are parameters to func()
				if (requires_array.indexOf(func) >= 0) {
					args = args[0];
				} else {
					// FIXME: do we really need to .join()?!
					args = args.length == 1 ? args[0] : args.join();
				}
				// regexps:
				if (typeof args === 'string' && args.indexOf('re:') === 0)
				args = new RegExp(args.substr(3), 'i');
				// regexp inequality means negation of equality
				if (func == 'ne' && args instanceof RegExp) {
					func = 'not';
				}
				// TODO: contains() can be used as poorman regexp
				// E.g. contains(prop,a,bb,ccc) means prop.indexOf('a') >= 0 || prop.indexOf('bb') >= 0 || prop.indexOf('ccc') >= 0
				//if (func == 'contains') {
				//	// ...
				//}
				// valid functions are prepended with $
				if (valid_funcs.indexOf(func) > -1) {
					func = '$'+func;
				}
				// $or requires an array of conditions
				// N.B. $or is said available for mongodb >= 1.5.1
				if (name == 'or') {
					if (!(search instanceof Array))
						search = [];
					var x = {};
					x[func == 'eq' ? key : func] = args;
					search.push(x);
					// other functions pack conditions into object
				} else {
					// several conditions on the same property is merged into one object condition
					if (search[key] === undefined)
						search[key] = {};
					if (search[key] instanceof Object && !(search[key] instanceof Array))
						search[key][func] = args;
					// equality cancels all other conditions
					if (func == 'eq')
						search[key] = args;
				}
			}
			// TODO: add support for query expressions as Javascript
		});
		return search;
	}
	//dir(['Q:',query]);
	search = walk(query.name, query.args);
	//dir(['S:',search]);
	return [options, search];
}


