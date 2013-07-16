/**
 * Module dependencies.
 */

var express = require('express'), mongoose = require('mongoose'), async = require('async'), url = require('url');

require('mongoose-long')(mongoose);

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


function Meter(meterId, latlong, name, lastread) {
	this.meterId = meterId;
	this.latlong = latlong;
	this.name = name;
	this.lastread = lastread;
}


function PQData(meterId, name, loc, freq, current, voltage, freqstats, currentstats, voltagestats) {
	this.meterId = meterId;
	this.name = name;
	this.loc = loc;
	this.freq = freq;
	this.current = current;
	this.voltage = voltage
	this.freqstats = freqstats;
	this.currentstats = currentstats;
	this.voltagestats = voltagestats;
}

function ConsumptionRecord(name, data) {
	this.name = name;
	this.data = data;
}

function ConsumptionData(meterId, name, loc, consrec, stats) {
	this.meterId = meterId;
	this.name = name;
	this.loc = loc;
	this.consrec = consrec;
	this.stats = stats;
}

function SingleConsumptionRecord(meterId, type, value, date) {
	this.meterId = meterId;
	this.meterType = type;
	this.value = value;
	this.date = date;
}

function StatRecord(date, value) {
	this.date = date;
	this.value = value;
}

function Stats(min, max, avg) {
	this.min = min;
	this.max = max;
	this.avg = avg;
}

function Location(meterId, loc, address) {
	this.meterId = meterId;
	this.loc = loc;
	this.address = address;
}

var SchemaTypes = mongoose.Schema.Types;

//mongo schemas
var nesinstantpower = mongoose.Schema({
	_id : {
		type : String,
		unique : true,
		index : true
	},
	CurrentTransformerVoltageTransformer: { type: Number },
	DateTime: { type: Date },
	DeviceId: { type: String },
	ExportReactive: { type: Number },
	ForwardActivePower: { type: Number },
	Frequency: { type: Number },
	ImportReactive: { type: Number },
	Phases: [ {
	         Id: { type: String },
	         Name: { type: String },
	         PhaseAngle: { type: Number },
	         PowerFactor: { type: Number },
	         RmsCurrent: { type: Number },
	         RmsVoltage: { type: Number },
	         _id : {
	     		type : String,
	     		unique : true,
	     		index : true
	     	}
	}],
	 ReverseActivePower: { type: Number },
	 VaallPhases: { type: Number }
}, { collection: 'nesinstantpower' });

var nesbill = mongoose.Schema({
	_id : {
		type : String,
		unique : true,
		index : true
	},
	DeviceId : { type: String },
	DateTime : { type: Date },
	SumOfTiers: 
	{ 
		DifferenceForwardReverseActive: {type: SchemaTypes.Long},
		ErrorCounter: {type: SchemaTypes.Long},
		ExportReactive: {type: SchemaTypes.Long},
		ForwardActive: {type: SchemaTypes.Long},
		ImportReactive: {type: SchemaTypes.Long},
		PowerOutageMinutes: {type: SchemaTypes.Long},
		PowerOutageSeconds: {type: SchemaTypes.Long},
		PowerOutages: {type: SchemaTypes.Long},
		PulseInput1: {type: SchemaTypes.Long},
		PulseInput2: {type: SchemaTypes.Long},
		ReverseActive: {type: SchemaTypes.Long},
		SumForwardReverseActive: {type: SchemaTypes.Long},
		_id : {
			type : String,
			unique : true,
			index : true
		}
	}
} , { collection: 'nesbill' });

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
	},
	Id: {
		type : String
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
var NesBill = db.model('nesbill', nesbill);
var NesInstantPower = db.model('nesinstantpower', nesinstantpower);

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


app.get('/cmlp/meterreadsapi/viewpq', function(req, res) {
	var queryData = url.parse(req.url, true).query;

	if (typeof queryData.meterId === "undefined") {
		return errors.e404(req,rsp,db);
	}

	var meterid = queryData.meterId;

	var list = [];
	
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
			start.setHours(0, 0, 0, 0);
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
			end.setHours(23, 59, 59, 999);
			console.log("enddate: " + end.getDate());
		}
	} else {
		end.setDate(end.getDate());
	}

	q = { Id: meterid };

	console.log("viewpq q: " + JSON.stringify(q));
	
	NesMeter.findOne(q, function(err, nesmeter) {
		if (!err && nesmeter) {
		   processNesMeterInstantPowerData(nesmeter, start, end, req, res);	
		}
		else {
			console.log("404 in viewpq: NesMeter.findOne: " + meterid);
			return errors.e404();
			
		}
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
			if (mreads && mreads.length) {
				processErtMeterData(MeterDetail,mreads, MeterLocations, res);
			}
			else {
				console.log("Searching nesmeters for id: " + meterId);
				q = { Id: meterId };

				NesMeter.findOne(q).execFind( function( err, nesmeter) {
					if (!err && nesmeter) {
						processNesMeterData(nesmeter, meterId, start, end, res);
					}
					else {
						console.log("404 in nesmeters.findOne()");
						return errors.e404(req, res, db);
					}
				});
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
			if (mreads) {

				meters['list'] = list;
				var pending = 0;

				for (var i in mreads) {
					pending++;
					mlocs[mreads[i].meterId] = 'unknown';
					mtyps[mreads[i].meterId] = 'unknown';
					var latlong = mreads[i].loc;

					(function(curmeter, curindex) {
						MeterLocations.findOne({ meterId: mreads[curindex].meterId}, function(err, mloc) {
							if (err) {
								console.log("warn: %j", err);
							}
							pending--;
							if (!err && mloc) {
								mlocs[mloc.meterId] = mloc.address;
								mtyps[mloc.meterId] = mloc.type;
								var meter = new Meter(mloc.meterId, mloc.loc, mloc.meterId, curmeter.lastread);
								list.push(meter); 
							}

							if (pending == 0) {
								res.setHeader('Content-Type', 'application/json');
								res.setHeader('Access-Control-Allow-Origin', '*');
								res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With');
								res.json(
										meters
								);
								res.end();
							}
						});
					})(mreads[i], i);

				}
			}
		}
	});
});


// server startup routine
var port = process.env.PORT || 5550;
app.listen(port, function() {
	console.log("Express server listening on port %d in %s mode", port,
			app.settings.env);
});

// end server startup routing


// helper functions
function processErtMeterData(meterreads, mreads, meterlocations, res) {
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

function processNesMeterData(nesmeter, meterId, start, end, res) {
	console.log("processNesMeterData, nesmeter: " + nesmeter);
	q = { DeviceId: meterId, DateTime: {"$gte": start, "$lte": end}};
	qholder = { query: q, meter: nesmeter };

	NesBill.find(qholder.query).sort({'date':1}).execFind( function(err, mreads) {

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
			console.log("mreads.length: " + mreads.length);

			for (var i =0; i<mreads.length; i++) {

				var mreading = mreads[i];
				//console.log("mread: " + mreads[i]);
				if (mreading && mreading.SumOfTiers) {
					//console.log("mread: " + mreads[i]);

					theDay = mreading.DateTime.toString().substr(0,3);
					theDayObj = mreading.DateTime;
					var curconsumption = mreading.SumOfTiers.SumForwardReverseActive;

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

			console.log("qholder: " + JSON.stringify(qholder.meter));
			var loc = [qholder.meter[0].Loc.Latitude, qholder.meter[0].Loc.Longitude];
			var locobj = new Location(qholder.meter[0].Id, loc, "");
			var consdata = new ConsumptionData(qholder.meter.Id, qholder.meter.Name, locobj, list, 
					new Stats(new StatRecord(mindate, minval), new StatRecord(maxdate, maxval), avg));
						res.setHeader('Content-Type', 'application/json');
			res.setHeader('Access-Control-Allow-Origin', '*');
			res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With');
			res.json(
					consdata
			);
			res.end();

		}
		else {
			return errors.e404(req, rsp, db);
		}
	})
};

function processNesMeterInstantPowerData(nesmeter, start, end, req, res) {
	q = { DeviceId: nesmeter.Id, DateTime: {"$gte": start, "$lte": end}};
	qholder = { query: q, meter: nesmeter };
	NesInstantPower.find(q).sort({'date':1}).execFind( function(err, mreads) {

		if (!err && mreads.length) 
		{

			var freqlist =[];
			var currentlist =[];
			var voltagelist =[];
			var prevConsumption=0;
			var mdelts = [];
			var mdates = [];
			var frequencies = [];
			var currents = [];
			var voltages = [];
			var prevDay = '';
			var prevDayObj;
			var theDay = '';
			var theDayObj;
			var j=0;
			for (var i =0; i<mreads.length; i++) {
				theDay = mreads[i].DateTime.toString().substr(0,3);
				theDayObj = mreads[i].DateTime;
				var freq = mreads[i].Frequency;
				var current = mreads[i].Phases[0].RmsCurrent;
				var voltage = mreads[i].Phases[0].RmsVoltage;
				//	var curconsumption = mreads[i].SumOfTiers.SumForwardReverseActive;

				//	if (theDay != prevDay) {
				mdates[j] = theDayObj; //theDay+' '+(mreads[i].DateTime.getMonth()+1)+'/'+(mreads[i].DateTime.getDate());
				//if (prevConsumption) {
				//	mdelts[j-1] = (curconsumption - prevConsumption)/100;
				//}
				frequencies[j] = freq / 1000;
				currents[j] = current / 1000;
				voltages[j] = voltage / 1000;
				j++;
				//prevConsumption = curconsumption;
				prevDay = theDay;
				prevDayObj = theDayObj;
				//	}
			}
			//mdelts[j-1] = (curconsumption - prevConsumption)/100;
			var minfreqdate = null;
			var minfreqval = null;
			var maxfreqdate = null;
			var maxfreqval = null;
			var aggregatefreq = null;
			var minvoltdate = null;
			var minvoltval = null;
			var maxvoltdate = null;
			var maxvoltval = null;
			var aggregatevolt = null;
			var mincurrdate = null;
			var mincurrval = null;
			var maxcurrdate = null;
			var maxcurrval = null;
			var aggregatecurr = null;

			for (var i=0; i<mdates.length; i++) {
				var cr =  new ConsumptionRecord(mdates[i], frequencies[i]);
				if ((frequencies[i] < minfreqval) || (minfreqval == null)) {
					minfreqval = frequencies[i];
					minfreqdate = mdates[i];
				}

				if ((frequencies[i] > maxfreqval) || (maxfreqval == null)) {
					maxfreqval = frequencies[i];
					maxfreqdate = mdates[i];
				}

				aggregatefreq += frequencies[i];

				freqlist.push(cr);

				var currentrec = new ConsumptionRecord(mdates[i], currents[i]);

				if ((currents[i] < mincurrval) || (mincurrval == null)) {
					mincurrval = currents[i];
					mincurrdate = mdates[i];
				}

				if ((currents[i] > maxcurrval) || (maxcurrval == null)) {
					maxcurrval = currents[i];
					maxcurrdate = mdates[i];
				}

				aggregatecurr += currents[i];
				currentlist.push(currentrec);

				var voltagerec = new ConsumptionRecord(mdates[i], voltages[i]);

				if ((voltages[i] < minvoltval) || (minvoltval == null)) {
					minvoltval = voltages[i];
					minvoltdate = mdates[i];
				}

				if ((voltages[i] > maxvoltval) || (maxvoltval == null)) {
					maxvoltval = voltages[i];
					maxvoltdate = mdates[i];
				}

				aggregatevolt += voltages[i];

				voltagelist.push(voltagerec);
			}

			var loc = [qholder.meter.Loc.Latitude, qholder.meter.Loc.Longitude];
			var locobj = new Location(qholder.meter.Id, loc, "");
			var consdata = new PQData(qholder.meter.Id, qholder.meter.Name, locobj, freqlist, currentlist, voltagelist,
					new Stats(new StatRecord(minfreqdate, minfreqval), new StatRecord(maxfreqdate, maxfreqval), aggregatefreq/mdates.length),
					new Stats(new StatRecord(mincurrdate, mincurrval), new StatRecord(maxcurrdate, maxcurrval), aggregatecurr/mdates.length),
					new Stats(new StatRecord(minvoltdate, minvoltval), new StatRecord(maxvoltdate, maxvoltval), aggregatevolt/mdates.length));
/*			var json = JSON.stringify(consdata);

			rsp.writeHead(200, { 'Content-Type': 'application/json', 'content-length':json.length,
				'Access-Control-Allow-Origin' : '*', 'Access-Control-Allow-Headers': 'X-Requested-With'});
			rsp.write(json);
			rsp.end();*/

			res.setHeader('Content-Type', 'application/json');
			res.setHeader('Access-Control-Allow-Origin', '*');
			res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With');
			res.json(
				consdata
			);
		}
		else {
			return errors.e404(req, res);
		}});

};

