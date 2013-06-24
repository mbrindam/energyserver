/**
 * Module dependencies.
 */


var express = require('express')
,   mongoose = require('mongoose')
,   async = require('async');

var app = express();

/**
 * Configuration
 */

app.configure(function() {
});


if (process.env.MONGOLAB_URI)
{
   var db = mongoose.createConnection(process.env.MONGOLAB_URI);
}
else 
{
   var db = mongoose.createConnection('localhost', 'cmlp');
}


var meter = mongoose.Schema( {
   _id: { type: String },
   Loc:  { type: Array }, 
   Name: { type: String },
   lastread: { type: Date }

});

var Meter = db.model('nesmeter', meter);


//Routes

app.get('/', function(req, res)
  {
     Meter.findOne(req.body, function(err, meter) {
       if (err) {
          res.json({'status': 'failure', 'error' : err});
       }
       else {
           if (meter) {
              res.json({'status': 'success', meter: meter._id}); 
           } 
       }

       res.end();
     
     }); 
  });


var port = process.env.PORT || 5550;
app.listen(port, function()
                {
        console.log("Express server listening on port %d in %s mode", port, app.settings.env);
                });
