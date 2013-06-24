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
	consumption: { type: Number },
	date: { type: Date }
});

var NesMeter = db.model('nesmeters', nesmeter);
var Meters = db.model('meterreads2', meterreads2);

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
		}

	}

	console.log('meterreads2 - %j', q);

	Meters.find({}, function(err, mreads) {
		if (err) {
			res.json({
				'status' : 'failure',
				'error' : err
			});
		} else {
			console.log("ecnmeters: %j", mreads);
			if (mreads) {

				meters['list'] = list;

				for (var i in mreads) {
					console.log('EcnMeter: %j', mreads[i]);
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

var port = process.env.PORT || 5550;
app.listen(port, function() {
	console.log("Express server listening on port %d in %s mode", port,
			app.settings.env);
});
