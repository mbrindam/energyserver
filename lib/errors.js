exports.e404 = function(req, res, msg) {
   var msg = msg || "The content you were looking for <em>"+req.url+"</em> cannot be found.";
   res.setHeader('Content-Type', 'text/html');
   res.statusCode = 404;
   res.send('<h1>404 Not Found</h1><br/><h2>' + msg + '</h2>');
   res.end();
}
