var Job = require("node-batch")
,	infra = require("../../batch-infrastructure-mongo");

var inDataOpt = {
		"db" : "mongo://localhost/node-batch-test",
		"collection": "testdocuments",
		"query": {},		//A mongo-native or a mongoskin query
	};

var outDataOpt = {
		"db": "mongo://localhost/node-batch-test",
		"collection": "processeddocuments"
	}

var indatasource = infra.getInputDatasource(inDataOpt);
var outdatasource = infra.getOutputDatasource(outDataOpt);


var job = new Job(indatasource, outdatasource);

/**
* Add middlewares to process data
*/
job.use(addRandomNumber)
	.use(addProcessed);

job.run(function(db, cb){
	var self = this;
	db.collection(self.collection).findEach({}, cb);
});

function addRandomNumber(){
	var data = this.data;
	data["0"] = (Math.random().toString(36).substring(2));
}

function addProcessed(){
	var data = this.data;
	data["10"] = ("Processed!")
}

function serialize(){
	var str = "";
	var data = this.data;
	for (var key in data){
		str += data[key] + ",";
	}
	this.data = str;
}