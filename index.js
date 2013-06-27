/**
 * Module dependencies.
 */

var express = require('express'), mongoose = require('mongoose'), async = require('async'), url = require('url');

var app = express();

var errors = require('./lib/errors');

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

var meterread = mongoose.Schema({
	meterId: { type: String },
	consumption: { type: Number },
	date: { type: Date },
	type: { type: String },
	_id: {
		type : String,
		unique : true,
		index : true
	}
}, { collection: 'meterreads' });

//mongoose.set('debug', true);

var NesMeter = db.model('nesmeters', nesmeter);
var Meters = db.model('meterreads2', meterreads2);
var MeterLocations = db.model('meterlocations', meterlocation);
var MeterDetail = db.model('meterreads', meterread);


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

app.get('/cmlp/meterreadsapi/view', function(req, res) {
	var queryData = url.parse(req.url, true).query;

	if (typeof queryData.meterId === "undefined") {
		return errors.e404(req,res);
	}
	
	var meterId = queryData.meterId;
	
	var start = new Date();
	var end = new Date();
	if (typeof queryData.start !== "undefined") {
		console.log('set startdate: ' + queryData.start);

		var startdatearr = queryData.start.split('/');
		//	start.setDate(Date.parse(queryData.start));
		console.log('startdatearr length: ' + startdatearr.length );


		if (startdatearr.length != 3) {
			start.setDate(start.getDate() - 14);
		}
		else {
			console.log("year: " + startdatearr[2] + " month: " + startdatearr[0] + " day: " + startdatearr[1]);
			start.setFullYear(startdatearr[2]);
			start.setMonth(startdatearr[0]-1);
			start.setDate(startdatearr[1]);
			console.log("startdate: " + start.getDate());
		}
	} else {
		start.setDate(start.getDate()-14);
	}

	if (typeof queryData.end !== "undefined") {
		console.log('set enddate: ' + queryData.end);

		var enddatearr = queryData.end.split('/');
		//	start.setDate(Date.parse(queryData.start));
		console.log('enddatearr length: ' + enddatearr.length );


		if (enddatearr.length != 3) {
			end.setDate(end.getDate());
		}
		else {
			console.log("year: " + enddatearr[2] + " month: " + enddatearr[0] + " day: " + enddatearr[1]);
			end.setFullYear(enddatearr[2]);
			end.setMonth(enddatearr[0]-1);
			end.setDate(enddatearr[1]);
			console.log("enddate: " + end.getDate());
		}
	} else {
		end.setDate(end.getDate());
	}

	console.log("meterId: " + meterId);
	q = { meterId: meterId, date: {"$gte": start, "$lte": end} };
	console.log(q);
	
	MeterDetail.find(q).sort({'date':1}).execFind( function (err, mreads) {
		if (err) {
			res.json({
				'status' : 'failure',
				'error' : err
			});
		} else {
			if (mreads) {
				processErtMeterData(MeterDetail,mreads, MeterLocations);
			}
		}
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

	//console.log('meterreads2 - %j', q);

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
			//console.log("ecnmeters: %j", mreads);
			if (mreads) {

				meters['list'] = list;
				var pending = 0;

				for (var i in mreads) {
					pending++;
					//console.log('EcnMeter: %j', mreads[i]);
					mlocs[mreads[i].meterId] = 'unknown';
					mtyps[mreads[i].meterId] = 'unknown';
					var latlong = mreads[i].loc;
					
					(function(curmeter, curindex) {
						MeterLocations.findOne({ meterId: mreads[curindex].meterId}, function(err, mloc) {
							if (err) {
								console.log("warn: %j", err);
								//errors.e404(req, rsp, db);
							}
							pending--;
							if (!err && mloc) {
								//console.log("mloc: %j, curmeter: %j, pending: %d", mloc, curmeter, pending)
								mlocs[mloc.meterId] = mloc.address;
								mtyps[mloc.meterId] = mloc.type;
								var meter = new Meter(mloc.meterId, mloc.loc, mloc.meterId, curmeter.lastread);
								list.push(meter); //[mreads[i].meterId] = meter;
							}

							if (pending == 0) {
								//console.log("responding");
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


function processErtMeterData(meterreads, mreads, meterlocations) {
	var list =[];
	var prevConsumption=0;
	var mdelts = [];
	var mdates = [];
	var prevDay = '';
	var prevDayObj = '';
	var theDay = '';
	var theDayObj = '';
	var j=0;
	for (var i =0; i<mreads.length; i++) {
		//debugger;
		theDayObj = mreads[i].date;
		theDay = mreads[i].date.toString().substr(0,3);
		if (theDay != prevDay) {
			mdates[j] = theDayObj; //+' '+(mreads[i].date.getMonth()+1)+'/'+(mreads[i].date.getDate());
			if (prevConsumption) {
				mdelts[j-1] = mreads[i].consumption - prevConsumption;
			}
			j++;
			prevConsumption = mreads[i].consumption;
			prevDay = theDay;
			prevDayObj = theDayObj;
		}
	}
	mdelts[j-1] = mreads[i-1].consumption - prevConsumption;

	var mindate = null;
	var minval = null;
	var maxdate = null;
	var maxval = null;
	
	var aggregate = 0;
	
	for (var i=0; i<mdates.length; i++) {

		var cr =  new ConsumptionRecord(mdates[i], mdelts[i]);
		
		if ((mdelts[i] < minval) || (minval == null)) {
			minval = mdelts[i];
			mindate = mdates[i];
		}
		
		if ((mdelts[i] > maxval) || (maxval == null)) {
			maxval = mdelts[i];
			maxdate = mdates[i];
		}
		
		aggregate += mdelts[i];
		var avg = aggregate / mdelts.length;
		
		list.push(cr);
	}

	
	meterlocations.findOne({ meterId: mreads[0].meterId}, function(err, mloc) {
		var consdata = new ConsumptionData(mreads[0].meterId, mreads[0].meterId, mloc, list,
				new Stats(new StatRecord(mindate, minval), new StatRecord(maxdate, maxval), avg));
		var json = JSON.stringify(consdata);

		res.setHeader('Content-Type', 'application/json');
		res.setHeader('Access-Control-Allow-Origin', '*');
		res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With');
		res.json(consdata);
		res.end();
	});
}

function processNesMeterData(nesmeter, start, end) {
	var nesbill = db.collection('nesbill');

	/*start = new Date();
	start.setDate(start.getDate()-14);*/
	q = { DeviceId: meterid, DateTime: {"$gte": start, "$lte": end}};
	qholder = { query: q, meter: nesmeter };
	nesbill.find(qholder.query).sort({'date':1}).toArray( function(err, mreads) {

		if (!err && mreads.length) 
		{

			var list =[];
			var prevConsumption=0;
			var mdelts = [];
			var mdates = [];
			var prevDay = '';
			var prevDayObj;
			var theDay = '';
			var theDayObj;
			var j=0;
			for (var i =0; i<mreads.length; i++) {
				theDay = mreads[i].DateTime.toString().substr(0,3);
				theDayObj = mreads[i].DateTime;
				var curconsumption = mreads[i].SumOfTiers.SumForwardReverseActive;

				if (theDay != prevDay) {
					mdates[j] = theDayObj; //theDay+' '+(mreads[i].DateTime.getMonth()+1)+'/'+(mreads[i].DateTime.getDate());
					if (prevConsumption) {
						mdelts[j-1] = (curconsumption - prevConsumption)/100;
					}
					j++;
					prevConsumption = curconsumption;
					prevDay = theDay;
					prevDayObj = theDayObj;
				}
			}
			mdelts[j-1] = (curconsumption - prevConsumption)/100;

			var mindate = null;
			var minval = null;
			var maxdate = null;
			var maxval = null;
			
			var aggregate = 0;
			
			for (var i=0; i<mdates.length; i++) {
				var cr =  new ConsumptionRecord(mdates[i], mdelts[i]);
				
				if ((mdelts[i] < minval) || (minval == null)) {
					minval = mdelts[i];
					mindate = mdates[i];
				}
				
				if ((mdelts[i] > maxval) || (maxval == null)) {
					maxval = mdelts[i];
					maxdate = mdates[i];
				}
				
				aggregate += mdelts[i];
				var avg = aggregate / mdelts.length;
				
				list.push(cr);
			}

			var loc = [qholder.meter.Loc.Latitude, qholder.meter.Loc.Longitude];
			var locobj = new Location(qholder.meter.Id, loc, "");
			var consdata = new ConsumptionData(qholder.meter.Id, qholder.meter.Name, locobj, list, 
					new Stats(new StatRecord(mindate, minval), new StatRecord(maxdate, maxval), avg));
			var json = JSON.stringify(consdata);

			rsp.writeHead(200, { 'Content-Type': 'application/json', 'content-length':json.length,
				'Access-Control-Allow-Origin' : '*', 'Access-Control-Allow-Headers': 'X-Requested-With'});
			rsp.write(json);
			rsp.end();

		}
		else {
			return errors.e404(req, rsp, db);
		}});

}
