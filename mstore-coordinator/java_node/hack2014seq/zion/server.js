var express = require("express");
var app = express();
var bodyParser = require("body-parser");

// var coordinator = require('./app/models/log');

app.use(bodyParser.urlencoded({ extended: true}));
app.use(bodyParser.json());

var port = process.env.PORT || 5050;

var router = express.Router();

router.use(function(req, res, next){
	console.log("Checking here ");
	next();
});

router.get('/', function(req, res){
	res.json({message: 'Welcome to hack2014 zion api!'});
});

router.route('logs')
	.post(function(req, res){
		console.log(req.params);
		res.json({message:"Zion api Post operation"});
	});

router.route('/logs/:log_id')
	.get(function(req, res){
		res.json({message: "Zion api Get operation"});
	})

	.put(function(req, res){
		res.json({message: "Zion api Put operation"});
	})

	.delete(function(req, res){
		res.json({message: "Zion api delete operation"});
	});



app.use('/api', router);

app.listen(port);
console.log(' Checkout zion magic on port '+port);