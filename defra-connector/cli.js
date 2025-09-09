"use strict";

process.on("uncaughtException",function(err){
	console.log(err.stack);
});

var util = require("util");
var fs = require("fs/promises");

var { MongoClient, } = require("mongodb");
// var { createClient: GQLWSClient, } = require("graphql-ws");
var { createClient: GQLSSEClient, } = require("graphql-sse");
// var ws = require("ws");
var fetch = require("node-fetch");

var {
	logError,
	...defraKV
} = require("./defra-kv.js");

const MONGO_ENDPOINT = "mongodb://127.0.0.1:27017/?replicaSet=rs0";
const LOG_MONGO = false;

// const DEFRA_WS_ENDPOINT = "ws://127.0.0.1:9181/api/v0/graphql";
const DEFRA_SSE_ENDPOINT = "http://127.0.0.1:9181/api/v0/graphql";
const LOG_DEFRA = false;

var mClient;
var mEvents;
var mResumeToken;
var mStartToken;

var dConnSeq = 0;
var dClient;
var dEvents;
var dLogFile;


main().catch(console.error);

// *************************************

async function main() {
	defraKV.init();

	try {
		var res = await defraKV.has("foo");
		console.log("has foo",res);

		var res = await defraKV.set("foo",{ bar: 1 });
		console.log("set",res);

		var res = await defraKV.set("foo",{ bar: 2 });
		console.log("set",res);

		var res = await defraKV.get("foo");
		console.log("get",res);

		var res = await defraKV.remove("foo");
		console.log("remove",res);
	}
	catch (err) {
		logError(err);
	}


	// await Promise.all([
	// 	listenMongo(),
	// 	listenDefra(),
	// ]);
}

async function listenMongo() {
	mClient = new MongoClient(MONGO_ENDPOINT,{
		directConnection: true,
		...(
			LOG_MONGO ? {
				mongodbLogComponentSeverities: {
					default: "debug",
				},
				mongodbLogPath: {
					file: await fs.open(`./mongo-${+new Date()}.log`,"w"),
					async write(log) {
						try {
							await this.file.appendFile(util.inspect(log) + "\n");
						}
						catch (fileError) {
							this.file = null;
							console.error("MONGO LOGGING FAILED:",String(fileError));
						}
					}
				},
			} : null
		),
	});

	await mClient.connect();

	console.log("MongoDB connected.");

	return startMongoWatcher();
}

async function startMongoWatcher() {
	console.log("Listening for MongoDB change events...");

	// filter if you want; otherwise omit pipeline
	// const pipeline = [
	//   { $match: { operationType: { $in: ["insert","update","replace","delete","drop","rename","invalidate"] } } }
	// ];
	var pipeline = [];

	mEvents = mClient.watch(pipeline,{
		fullDocument: "updateLookup",
		...(mResumeToken != null ? { resumeAfter: mResumeToken, } : null),
		...(mStartToken != null ? { startAfter: mStartToken, } : null),
	});

	mEvents.on("change",mOnChange);
	mEvents.on("error",mOnError);
	mEvents.on("close",mOnClose);

	// for await (const ev of mEvents) {
	//   console.log(ev);
	// }
}

function mTeardownEvents() {
	if (mEvents != null) {
		mEvents.off("change",mOnChange);
		mEvents.off("error",mOnError);
		mEvents.off("close",mOnClose);
		mEvents = null;
	}
}

function mOnChange(data) {
	if (data.operationType == "invalidate") {
		mStartToken = data._id;
		mResumeToken = null;
		console.log("MONGO INVALIDATE:",util.inspect(data,{depth:10}));
		mTeardownEvents();
		return startMongoWatcher();
	}
	else {
		mStartToken = null;
		mResumeToken = data._id;
		console.log("MONGO CHANGE:",util.inspect(data,{depth:10}));
	}
}

function mOnError(err) {
	console.error("MONGO WATCH ERROR:",String(err));
}

function mOnClose() {
	console.log("MONGO CLOSE");
	mTeardownEvents();
	setTimeout(() => {
		listenMongo().catch(console.error);
	},250);
}

async function listenDefra() {
	// var dClient = GQLWSClient({
	// 	url: DEFRA_WS_ENDPOINT,
	// 	webSocketImpl: ws,
	// 	lazy: true,
	// 	retryAttempts: Infinity,
	// 	connectionParams: {
	// 		"Content-Type": "application/json",
	// 		"Accept": "text/event-stream",
	// 		"Cache-Control": "no-cache",
	// 	},
	// });

	if (LOG_DEFRA && dLogFile == null) {
		dLogFile = await fs.open(`./defra-${+new Date()}.log`,"w")
	}

	dClient = GQLSSEClient({
		url: DEFRA_SSE_ENDPOINT,
		fetchFn: fetch,
		lazy: true,
		retryAttempts: Infinity,
		// singleConnection: true,
		headers: {
			"Content-Type": "application/json",
			"Accept": "text/event-stream",
			"Cache-Control": "no-cache",
		},
		...(
			LOG_DEFRA && dLogFile != null ?
				{
					onMessage(msg) {
						return dLogFile.appendFile(util.inspect(msg,{depth:10}) + "\n");
					},
					async fetchFn(url,opts) {
						var id = ++dConnSeq;
						await dLogFile.appendFile(`[DEFRA CONNECT ${id}] ${opts?.method || "GET"}: ${url}\n`);
						var fetchRes = fetch(url,opts);
						try {
							return await fetchRes;
						}
						catch (err) {
							await dLogFile.appendFile(`[DEFRA CONNECT ${id}] ERROR: ${String(err)}\n`);
							console.error();
							return fetchRes;
						}
					},
				} :

				{
					fetchFn: fetch,
				}
		),
	});

	console.log("DefraDB Connected.");

	return startDefraListener();
}

async function startDefraListener() {
	console.log("Listening for DefraDB updates...");

	dEvents = dClient.subscribe(
		{
			// not currently supported:
			//
			// query: `
			// 	subscription {
			// 		Commit {
			// 			CID
			// 			DocID
			// 			CollectionID
			// 			Delta
			// 		}
			// 	}
			// `,
			query: `
				subscription {
					User {
						_docID
						name
						email
						counter
					}
				}
			`,
		},
		{
			next: dOnNext,
			error: dOnError,
			complete: dOnComplete,
		}
	);
	// "dEvents.return()" to dispose


	// var dEvents = dClient.iterate({
	// 	query: `
	// 		subscription {
	// 			User {
	// 				_docID
	// 				name
	// 				email
	// 				counter
	// 			}
	// 		}
	// 	`,
	// });
	// for await (let result of dEvents) {
	// 	// next = result = { data: { greetings: 5x } }
	// 	console.log("DEFRA:",result);
	// 	// "break" to dispose
	// }
}

function dOnNext(evt) {
	console.log("DEFRA UPDATE:",util.inspect(evt.data,{depth:10}));
}

function dOnError(err) {
	console.error("DEFRA WATCH ERROR:",String(err));
}

function dOnComplete() {
	console.log("DEFRA SUBSCRIPTION CLOSED");
	setTimeout(() => {
		listenDefra().catch(console.error);
	},250);
}
