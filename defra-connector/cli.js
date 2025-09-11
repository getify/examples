#!/bin/env node

"use strict";

process.on("uncaughtException",function(err){
	console.log(err.stack);
});

var path = require("path");
var util = require("util");
var fs = require("fs/promises");

var {
	MongoClient,
	ObjectId: MongoObjectId,
} = require("mongodb");
// var { createClient: GQLWSClient, } = require("graphql-ws");
var { createClient: GQLSSEClient, } = require("graphql-sse");
// var ws = require("ws");
var fetch = require("node-fetch");
var { ClassicLevel, } = require("classic-level");
var cliArgs = require("minimist")(
	process.argv.slice(2),
	{
		boolean: [ "help", ],
		string: [ "collection", "db" ],
		alias: {
			collection: "c"
		},
		default: {},
	}
);

const MONGO_ENDPOINT = "mongodb://127.0.0.1:27017/?replicaSet=rs0";
const LOG_MONGO = false;

// const DEFRA_WS_ENDPOINT = "ws://127.0.0.1:9181/api/v0/graphql";
const DEFRA_SSE_ENDPOINT = "http://127.0.0.1:9181/api/v0/graphql";
const DEFRA_SCHEMA_ENDPOINT = "http://127.0.0.1:9181/api/v0/schema";
const DEFRA_GRAPHQL_ENDPOINT = "http://127.0.0.1:9181/api/v0/graphql";
const LOG_DEFRA = true;



var localKV = new ClassicLevel(
	path.join(".",".local-kv"),
	{ valueEncoding: "json", }
);

var mClient;
var mSession;
var mSessionID;
var mEvents;
var mResumeToken;
var mStartToken;

var dConnSeq = 0;
var dClient;
var dEvents = [];
var dLogFile;


main().catch(console.error);

// *************************************

async function main() {
	if (cliArgs.help) {
		printHelp();
		process.exit(0);
	}
	else if (!(cliArgs.collection && cliArgs.db)) {
		console.error("Missing required --collection and --db");
		console.log("");
		printHelp();
		process.exit(1);
	}
	else if (Array.isArray(cliArgs.db)) {
		console.error("Only one --db may be specified");
		console.log("");
		printHelp();
		process.exit(1);
	}

	cliArgs.collection = (
		Array.isArray(cliArgs.collection) ?
			cliArgs.collection :

			[ cliArgs.collection, ]
	);

	// try {
	// 	var res = await localKV.has("foo");
	// 	console.log("has foo",res);

	// 	var res = await localKV.put("foo",{ bar: 1 });
	// 	console.log("put",res);

	// 	var res = await localKV.put("foo",{ bar: 2 });
	// 	console.log("put",res);

	// 	var res = await localKV.get("foo");
	// 	console.log("get",res);

	// 	var res = await localKV.del("foo");
	// 	console.log("del",res);
	// }
	// catch (err) {
	// 	console.error(err);
	// }


	await Promise.all([
		listenMongo(),
		listenDefra(),
	]);
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

	try {
		mSession = mClient.startSession();
		mSessionID = mSession.id.id;

		await mClient.connect();

		console.log("MongoDB connected.");

		return await startMongoWatcher();
	}
	catch (err) {
		console.error(err);
		process.exit(1);
	}
}

async function startMongoWatcher() {
	console.log(`Listening for MongoDB change events on (${
		cliArgs.collection
			.map(typeName => `${cliArgs.db}:${typeName}`)
			.join(",")
	})...`);

	// filter if you want; otherwise omit pipeline
	// const pipeline = [
	//   { $match: { operationType: { $in: ["insert","update","replace","delete","drop","rename","invalidate"] } } }
	// ];
	var pipeline = [
		{
			$match: {
				$or: [
					{
						lsid: { $exists: false, },
						"lsid.id": { $ne: mSessionID, },
					},
				],
			},
		},
	];

	mEvents = mClient.db(cliArgs.db).watch(pipeline,{
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

async function mOnChange(changeEntry) {
	if (changeEntry.operationType == "invalidate") {
		mStartToken = changeEntry._id;
		mResumeToken = null;
		console.log("MONGO INVALIDATE:",util.inspect(changeEntry,{depth:10}));
		mTeardownEvents();
		return startMongoWatcher();
	}
	else {
		mStartToken = null;
		mResumeToken = changeEntry._id;

		let { coll: collName, } = changeEntry.ns || {};
		let { _id: mID, ...fullDocument } = changeEntry.fullDocument || {}

		if (mID != null && collName != null && fullDocument != null) {
			console.log("MONGO CHANGE:",util.inspect(changeEntry,{depth:10}));

			let _docID = await localKV.get(`md_${String(mID)}`);
			let resp;

			// already a known document in defra?
			if (_docID != null) {
				resp = await defraGraphQL({
					query: `
						mutation Update${collName}($docID:ID!,$input:${collName}MutationInputArg!) {
							update_${collName}(
								filter: { _docID: { _eq: $docID } },
								input: $input
							)
							{
								_docID
								_version { cid height signature { __typename } }
							}
						}
					`,
					variables: {
						docID: _docID,
						input: fullDocument,
					},
				});
			}
			// otherwise, insert new document
			else {
				resp = await defraGraphQL({
					query: `
						mutation Insert${collName}($input:${collName}MutationInputArg!) {
							${collName}(input: $input)
							{
								_docID
								_version { cid height signature { __typename } }
							}
						}
					`,
					variables: {
						input: fullDocument,
					},
				});
			}

			// cache defra commit info (if successful)
			let respEntry = (
				((resp ?
					resp[Object.keys(resp)[0]] :
					null
				) || [])[0]
			);

			if (respEntry != null) {
				let headsEntry = extractHeads(respEntry._version);
				if (
					headsEntry.height >= 0 &&
					headsEntry.heads.length > 0
				) {

					if (_docID == null) {
						_docID = respEntry._docID;
						console.log(`insert; mongo (${mID}) -> defra (${_docID})`);
						await localKV.put(`dh_${_docID}`,headsEntry);
						await localKV.put(`dm_${_docID}`,mID);
						await localKV.put(`md_${mID}`,_docID);
					}
					else {
						await localKV.put(`dh_${_docID}`,headsEntry);
						console.log(`update; mongo (${mID}) -> defra (${_docID})`);
					}
				}
				else {
					console.error(`MONGO->DEFRA INSERT/UPDATE ERROR: ${JSON.stringify(resp)}`)
				}
			}
			else {
				console.error(`MONGO->DEFRA ERROR: ${JSON.stringify(resp)}`);
			}
		}
		else {
			console.error(`MONGO UNRECOGNIZED CHANGE: ${JSON.stringify(changeEntry)}`);
		}
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
	// dClient = GQLWSClient({
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

	try {
		let typeDefs = (
			Object.fromEntries(
				(await Promise.all(
					cliArgs.collection.map(fetchDefraTypeFields)
				))
			)
		);

		return await startDefraListener(typeDefs);
	}
	catch (err) {
		console.error(err.toString());
		process.exit(1);
	}
}

async function defraGraphQL({ query, schema, variables = {}, } = {}) {
	try {
		let apiResp = await fetch(
			(
				schema != null ?
					DEFRA_SCHEMA_ENDPOINT :

					DEFRA_GRAPHQL_ENDPOINT
			),
			{
				method: "POST",
				headers: {
					"Content-Type": (
						schema != null ?
							"text/plain" :

							"application/json"
					),
				},
				body: (
					schema ||
					(query ? JSON.stringify({ query, variables, }) : null) ||
					""
				),
			}
		);
		if (apiResp.ok) {
			return (
				schema != null ?
					(await apiResp.text()) :

					(await apiResp.json()).data
			);
		}
		else {
			let apiRespBody = await apiResp.text();
			throw new Error(apiRespBody);
		}
	}
	catch (err) {
		console.error(err.toString());
	}
}

async function fetchDefraTypeFields(collection) {
	console.log(`Fetching DefraDB type definition for ${collection}...`);

	var resp = await defraGraphQL({
		query: `
			query {
				__type(name:"${collection}") {
					fields { name }
				}
			}
		`,
	});

	if (resp && resp.__type && resp.__type.fields) {
		let fieldNames = (
			resp.__type.fields
				.map(entry => entry.name)
				.filter(name => name[0] != "_")
		);
		return [ collection, fieldNames, ];
	}

	throw new Error(`Type '${collection}' not found/retrieved properly`);
}

async function startDefraListener(typeDefs) {
	console.log(`Subscribing to DefraDB updates (${Object.keys(typeDefs).join(",")})...`);

	for (let [ typeName, typeFields, ] of Object.entries(typeDefs)) {
		dEvents.push(
			dClient.subscribe(
				{
					query: `
						subscription {
							${typeName}(showDeleted: true) {
								_docID ${typeFields.join(" ")}
								_version { cid height signature { __typename } }
							}
						}
					`,
				},
				{
					next: dOnNext,
					error: dOnError,
					complete: dOnComplete,
				}
			)
		);
	}


	// dEvents = dClient.subscribe(
	// 	{
	// 		// not currently supported:
	// 		//
	// 		// query: `
	// 		// 	subscription {
	// 		// 		Commit {
	// 		// 			CID
	// 		// 			DocID
	// 		// 			CollectionID
	// 		// 			Delta
	// 		// 		}
	// 		// 	}
	// 		// `,
	// 		query: `
	// 			subscription {
	// 				User {
	// 					_docID
	// 					name
	// 					email
	// 					counter
	// 				}
	// 			}
	// 		`,
	// 	},
	// 	{
	// 		next: dOnNext,
	// 		error: dOnError,
	// 		complete: dOnComplete,
	// 	}
	// );
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

function extractHeads(versions) {
	if (versions != null) {
		let maxHeight = Math.max(...versions.map(v => v.height));
		return {
			heads: [
				...(new Set(
					versions
						.filter(v => v.height == maxHeight)
						.map(v => v.cid)
				)),
			],
			height: maxHeight,
		};
	}
	return { heads: [], height: -1, };
}

async function dOnNext(evt) {
	var changeEntry = (
		((evt && evt.data ?
			evt.data[Object.keys(evt.data)[0]] :
			null
		) || [])[0]
	);

	if (changeEntry != null) {
		let collName = Object.keys(evt.data)[0];
		let { _docID, _version, ...$set } = changeEntry;
		let headsEntry = extractHeads(_version);
		let cachedHeads = (
			(await localKV.get(`dh_${_docID}`)) ||
			{ heads: [], height: -1 }
		);

		if (
			// higher commit height?
			headsEntry.height > cachedHeads.height ||

			(
				// same commit height?
				headsEntry.height == cachedHeads.height &&

				// but includes commit(s) we didn't do and cache?
				![ ...headsEntry.heads, ]
					.every(cID => cachedHeads.heads.includes(cID))
			)
		) {
			console.log("DEFRA CHANGE:",util.inspect(evt.data,{depth:10}));

			try {
				await mSession.withTransaction(async () => {
					var mID = (await localKV.get(`dm_${_docID}`)) || null;

					try {
						let res = await (
							mClient
								.db(cliArgs.db)
								.collection(collName)
								.updateOne(
									/*filter=*/{
										_id: new MongoObjectId(...(
											mID != null ? [ mID ] : []
										)),
									},
									/*update=*/{
										$set,
									},
									/*options=*/{
										upsert: true,
										session: mSession,
									}
								)
						);

						// mongo upsert succeeded?
						if (
							res &&
							res.acknowledged &&
							(res.upsertedCount == 1 || res.modifiedCount == 1)
						) {
							// inserting new document into mongo?
							if (mID == null) {
								mID = String(res.upsertedId);
								console.log(`insert; defra (${_docID}) -> mongo (${mID})`);
								await localKV.put(`dm_${_docID}`,mID);
								await localKV.put(`md_${mID}`,_docID);
							}
							else {
								console.log(`update; defra (${_docID}) -> mongo (${mID})`);
							}
						}
						else {
							console.error(`DEFRA->MONGO UPSERT ERROR: ${util.inspect(res,{depth:10})}`);
						}
					}
					catch (err) {
						console.error(`DEFRA->MONGO ERROR: ${err.toString()}`);
					}
				});
			}
			catch (err) {
				console.error(`DEFRA->MONGO TRANSACTION ERROR: ${err.toString()}`);
			}
		}
	}
	else {
		console.error(`DEFRA UNRECOGNIZED CHANGE: ${JSON.stringify(evt)}`);
	}
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

function printHelp() {
	console.log("DefraConnector");
	console.log("  Usage: cli.js --db=DB_NAME -c COLL_NAME [OPTION]...");
	console.log("");
	console.log("--db=DB                              mongo DB name");
	console.log("-c, --collection=COLL_NAME           collection to subscribe to by name");
	console.log("--help                               print this help");
}
