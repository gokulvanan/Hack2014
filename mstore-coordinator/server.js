// Load the http module to create an http server.
var http = require('http');
var url = require('url');

// Configure our HTTP server to respond with Hello World to all requests.
var server = http.createServer(function (request, response) {
  var q = url.parse(request.url,true);
  var path = q.pathname;
  response.writeHead(200, {"Content-Type": "application/json"});
  if(path == 'getNextLogPosition'){
      var obj ={ "status":"success", data:1 };
      response.end();
  }else if(path == 'updateLogPosition'){
      var logObj = { id:2, query:"upsert into test values (1,'Hello')" };
      var obj ={ "status":"success", data:logObj };
      response.end("Hello World\n");
  }else{

      response.end("{status:'error', data:'failed invalid path'}");
  }
});

// Listen on port 8000, IP defaults to 127.0.0.1
server.listen(8000);

// Put a friendly message on the terminal
console.log("Server running at http://127.0.0.1:8000/");
