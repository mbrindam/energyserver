/**
 * Module dependencies.
 */

var express = require('express'), mongoose = require('mongoose'), async = require('async'), url = require('url');

var app = express();

/**
 * Configuration
 */

app.configure(function() {
});

if (process.env.MONGOLAB_URI) {
	var db = mongoose.createConnection(process.env.MONGOLAB_URI);
} else {
	var db = mongoose.createConnection('localhost', 'cmlp');
}

/* Return structure for meter */
function Meter(meterId, latlong, name, lastread) {
	this.meterId = meterId;
	this.latlong = latlong;
	this.name = name;
	this.lastread = lastread;
}

var nesmeter = mongoose.Schema({
	_id : {
		type : String,
		unique : true,
		index : true
	},
	Loc : {
		Latitude : 'string',
		Longitude : 'string'
	},
	Name : {
		type : String
	},
	lastread : {
		type : Date
	}

});

var meterreads2 = mongoose.Schema({
	_id : {
		type : String,
		unique : true,
		index : true
	},
	meterId: { type: String },
	lastread: { type: Date },
	date: { type: Date }
}, { collection: 'meterreads2' });

var meterlocation = mongoose.Schema({
	meterId: { type: String },
	type: { type: String },
	meter_num: { type: String },
	account: { type: String },
	unit_num: { type: String },
	loc: [],
	address: { type: String },
	_id: {
		type : String,
		unique : true,
		index : true
	}
});

mongoose.set('debug', true);

var NesMeter = db.model('nesmeters', nesmeter);
var Meters = db.model('meterreads2', meterreads2);
var MeterLocations = db.model('meterlocations', meterlocation);


//Routes
app.get('/cmlp/meterreadsapi/locations', function(req, res)
		{

	var meters = {};
	var list = [];
	var pending = 0;

	var queryData = url.parse(req.url, true).query;

	var q = {};
	if (typeof queryData.type !== "undefined") {
		if (queryData.type === "usage") {
			var start = new Date();
			start.setDate(start.getDate() - 14);
			q = {
					lastread : {
						"$gte" : start
					}
			};
		} else if (queryData.type === "outage") {
			var start = new Date();
			start.setDate(start.getDate() - 14);

			var end = new Date();
			end.setDate(end.getDate() - 1);
			q = {
					lastread : {
						"$gte" : start,
						"$lt" : end
					}
			};
		} else if (queryData.type === "anomoly") {
			var start = new Date();
			start.setDate(start.getDate() - 14);

			q = {
					lastread : {
						"$lt" : start
					}
			};
		} else if (queryData.type === "quality") {
			var start = new Date();
			start.setDate(start.getDate() - 14);
			q = {
					lastread : {
						"$gte" : start
					}
			};
		}

	}

	console.log('nesmeter - ' + q);

	NesMeter.find(q, function(err, mreads) {
		if (err) {
			res.json({
				'status' : 'failure',
				'error' : err
			});
		} else {
			if (mreads) {

				meters['list'] = list;

				for (var i in mreads) {
					var meter = new Meter(mreads[i]._id, mreads[i].Loc, mreads[i].Name, mreads[i].lastread);
					list.push(meter); 
				}

				res.setHeader('Content-Type', 'application/json');
				res.setHeader('Access-Control-Allow-Origin', '*');
				res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With');
				res.json(
						meters
				);
			}
		}

		res.end();

	});
		});


app.get('/cmlp/meterreadsapi', function(req, res) {

    var queryData = url.parse(req.url, true).query;
	var meters = {};
	var list = [];
	var mlocs = {};
	var mtyps = {};
	var q = {};
	if (typeof queryData.type !== "undefined") {
		if (queryData.type === "usage") {
			var start = new Date();
			start.setDate(start.getDate() - 14);
			q = {
					lastread : {
						"$gte" : start
					}
			};
		} else if (queryData.type === "outage") {
			var start = new Date();
			start.setDate(start.getDate() - 14);

			var end = new Date();
			end.setDate(end.getDate() - 1);
			q = {
					lastread : {
						"$gte" : start,
						"$lt" : end
					}
			};
		} else if (queryData.type === "anomoly") {
			var start = new Date();
			start.setDate(start.getDate() - 14);

			q = {
					lastread : {
						"$lt" : start
					}
			};
		} else if (queryData.type === "quality") {
			meters['list'] = list;
			res.setHeader('Content-Type', 'application/json');
			res.setHeader('Access-Control-Allow-Origin', '*');
			res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With');
			res.json(
					meters
			);
			res.end();
			return;
		}

	}

	console.log('meterreads2 - %j', q);

	//async.series([
	//              function (callback) {
	Meters.find(q).sort({'date': -1}).limit(1000).execFind(function(err,mreads){
		if (err) {
			console.log(err);
			res.json({
				'status' : 'failure',
				'error' : err
			});
			res.end();
			return;
		} else {
			console.log("ecnmeters: %j", mreads);
			if (mreads) {

				meters['list'] = list;
				var pending = 0;

				for (var i in mreads) {
					pending++;
					console.log('EcnMeter: %j', mreads[i]);
					mlocs[mreads[i].meterId] = 'unknown';
					mtyps[mreads[i].meterId] = 'unknown';
					var latlong = mreads[i].loc;
					
					(function(curmeter, curindex) {
						MeterLocations.findOne({ meterId: mreads[curindex].meterId}, function(err, mloc) {
							/*if (err) {
								//errors.e404(req, rsp, db);
								meters['list'] = list;
								var json = JSON.stringify(meters);

								rsp.writeHead(200, { 'Content-Type': 'application/json', 'content-length':json.length,
									'Access-Control-Allow-Origin' : '*', 'Access-Control-Allow-Headers': 'X-Requested-With'});
								rsp.write(json);
								rsp.end();
								return;
							}*/
							pending--;
							if (!err && mloc) {
								//console.log("mloc: %j, curmeter: %j, pending: %d", mloc, curmeter, pending)
								mlocs[mloc.meterId] = mloc.address;
								mtyps[mloc.meterId] = mloc.type;
								var meter = new Meter(mloc.meterId, mloc.loc, mloc.meterId, curmeter.lastread);
								list.push(meter); //[mreads[i].meterId] = meter;
							}

							if (pending == 0) {
								console.log("responding");
								res.setHeader('Content-Type', 'application/json');
								res.setHeader('Access-Control-Allow-Origin', '*');
								res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With');
								res.json(
										meters
								);
								res.end();
								
								/*var json = JSON.stringify(meters);

								rsp.writeHead(200, { 'Content-Type': 'application/json', 'content-length':json.length,
									'Access-Control-Allow-Origin' : '*', 'Access-Control-Allow-Headers': 'X-Requested-With'});
								rsp.write(json);
								rsp.end();*/


							}
						});
					})(mreads[i], i);
					
				}
				
				/*meters['mtyps'] = mtyps;
				meters['mlocs'] = mlocs;*/
			}
		}
	});
//    callback();
//}],
// [
//function (callback) {
//  var pending = 0;
//  for (i=0;i<mreads.length)
//}
// ]);
});

var port = process.env.PORT || 5550;
app.listen(port, function() {
	console.log("Express server listening on port %d in %s mode", port,
			app.settings.env);
});
