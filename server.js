"use strict";

process.on("uncaughtException",function(err){
	console.log(err.stack);
});


var path = require("path");
var http = require("http");
var httpServer = http.createServer(handleRequest);

var nodeStaticAlias = require("@getify/node-static-alias");
var cookie = require("cookie");
var getStream = require("get-stream");
var fetch = require("node-fetch");
var { MongoClient } = require("mongodb");


var HSTSHeader = {
	"Strict-Transport-Security": `max-age=${ 1E9 }`,
};
var noSniffHeader = {
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Content-Type-Options
	"X-Content-Type-Options": "nosniff",
};
var CSPHeader = {
	"Content-Security-Policy":
		[
			`default-src ${[
				"'self'",
				"'unsafe-inline'",
			].join(" ")};`,

			// `style-src ${[
			// 	"'self'",
			// ].join(" ")};`,

			// `connect-src ${[
			// 	"'self'",
			// ].join(" ")};`,

			// `script-src ${[
			// 	"'self'",
			// ].join(" ")};`,
		].join(" ")
};

const OLD_EXPIRES = "Thu, 01 Jan 1970 00:00:01 UTC";
const COOKIE_PARAMS = "domain=defra-connector.getify.com; path=/; SameSite=Lax; Secure";
const CLEAR_COOKIE = `${COOKIE_PARAMS}; Expires=${OLD_EXPIRES}`;
const STATIC_DIR = path.join(__dirname,"web");
const DEV = true;
const CACHE_FILES = false;
const DEFRA_SCHEMA_ENDPOINT = "http://127.0.0.1:9181/api/v0/schema";
const DEFRA_PURGE_ENDPOINT = "http://127.0.0.1:9181/api/v0/purge";
const DEFRA_GRAPHQL_ENDPOINT = "http://127.0.0.1:9181/api/v0/graphql";
const MONGO_ENDPOINT = "mongodb://127.0.0.1:27017/?replicaSet=rs0";


var mClient;
var staticServer = new nodeStaticAlias.Server(STATIC_DIR,{
	serverInfo: "Defra-Connector",
	cache: CACHE_FILES ? (60 * 60 * 3) : 0,
	cacheStaleRevalidate: CACHE_FILES ? (60 * 60 * 24 * 7) : 0,
	gzip: /^(?:(?:text\/.+)|(?:image\/svg\+xml)|(?:application\/javascript)|(?:application\/json)|(?:application\/manifest\+json))(?:; charset=utf-8)?$/,
	headers: {
		...(!DEV ? HSTSHeader : {}),
	},
	onContentType(contentType,headers) {
		// apparently this is the new preferred mime-type for JS
		if (contentType == "application/javascript") {
			contentType = "text/javascript";
		}

		// only add CSP headers for text/html pages
		if (contentType == "text/html") {
			Object.assign(headers,CSPHeader);
		}

		// no-sniff header for CSS and JS only
		if (/^(?:text\/(?:css|javascript))|(?:application\/json)$/.test(contentType)) {
			Object.assign(headers,noSniffHeader);
		}

		// add utf-8 charset for some text file types
		if (
			/^((text\/(?:html|css|javascript))|(?:application\/json)|(image\/svg\+xml)|(application\/manifest\+json))$/.test(contentType)
		) {
			contentType = `${contentType}; charset=utf-8`;
		}

		return contentType;
	},
	alias: [
		{
			match: "/defradb",
			serve: "defradb.html",
		},
		{
			match: "/mongodb",
			serve: "mongodb.html",
		},
		{
			match: /[^]/,
			serve: "<% absPath %>",
		},
	],
});

connectMongo()
	.then(client => (mClient = client))
	.catch(err => console.log(err));

httpServer.listen(8888,"127.0.0.1");


// *************************************

function handleRequest(req,res) {
	if (!DEV && !/^defra-connector\.getify\.com$/.test(req.headers["host"])) {
		res.writeHead(307,{
			Location: `https://defra-connector.getify.com${req.url}`,
			"Cache-Control": "public, max-age=3600",
			Expires: new Date(Date.now() + (3600 * 1000) ).toUTCString(),
		});
		res.end();
	}
	// unconditional, permanent HTTPS redirect
	else if (!DEV && req.headers["x-forwarded-proto"] !== "https") {
		res.writeHead(301,{
			"Cache-Control": "public, max-age=31536000",
			Expires: new Date(Date.now() + 31536000000).toUTCString(),
			Location: `https://defra-connector.getify.com${req.url}`
		});
		res.end();
	}
	else {
		onRequest(req,res);
	}
}

async function onRequest(req,res) {
	// process inbound request?
	if ([ "GET", "POST", "HEAD", ].includes(req.method)) {
		// parse cookie values?
		if (req.headers.cookie) {
			req.headers.cookie = cookie.parse(req.headers.cookie);
		}

		if (req.method == "GET") {
			let parsedURL = new URL(req.url,"https://defra-connector.getify.com");
			req.params = Object.fromEntries(parsedURL.searchParams.entries());
		}
		else if (req.method == "POST") {
			let body = await getStream(req);
			try {
				req.body = JSON.parse(body);
			}
			catch (err) {
				req.body = { raw: body, };
			}
		}
	}

	// handle graphql calls to defradb
	if (req.method == "POST" && req.url == "/defradb") {
		try {
			let reqDocMode = (
				(req.body && req.body.query) ?
					"graphql" :

				(req.body && req.body.schema) ?
					"schema" :

				(req.body && req.body.purge) ?
					"purge" :

					null
			);
			let apiResp = await fetch(
				(
					reqDocMode == "schema" ?
						DEFRA_SCHEMA_ENDPOINT :

					reqDocMode == "purge" ?
						DEFRA_PURGE_ENDPOINT :

						DEFRA_GRAPHQL_ENDPOINT
				),
				{
					method: "POST",
					headers: {
						...(
							Object.fromEntries(
								Object.entries(req.headers)
									.filter(([k]) => (
										!/^host$|^connection$|^content-length$|^transfer-encoding$|^expect$|^sec-|^cf-/.test(k)
								))
							)
						),

						"Content-Type": (
							reqDocMode == "graphql" ?
								"application/json" :
								"text/plain"
						),
					},
					body: (
						reqDocMode == "graphql" ?
							JSON.stringify(req.body) :

						reqDocMode == "schema" ?
							req.body.schema :

						""
					),
				}
			);
			if (apiResp.ok) {
				let respText = await apiResp.text();
				return sendJSON(res,200,respText || { "result": true, });
			}
			else {
				let apiRespBody = await apiResp.text();
				return sendJSON(res,500,{
					error: `DefraDB could not be reached (${apiResp.status}: ${apiRespBody})`,
				});
			}
		}
		catch (err) {
			return sendJSON(res,500,{
				error: err.toString(),
			});
		}
	}
	else if (req.method == "POST" && req.url == "/mongodb") {
		let {
			op: opName,
			db: dbName,
			coll: collName,
		} = req.body || {};

		try {
			if (opName == "purge") {
				let { databases } = (
					await mClient.db("admin").command({
						listDatabases: 1,
						nameOnly: true,
					})
				);
				let dbCount = 0;
				let dropCount = 0;
				for (let { name, } of databases) {
					if (![ "admin", "local", "config", ].includes(name)) {
						dbCount++;
						try {
							await mClient.db(name).dropDatabase();
							dropCount++;
						}
						catch (err) {}
					}
				}
				return sendJSON(res,200,{
					success: (dbCount == dropCount),
					dbCount,
					dropCount,
				});
			}
			else if (opName == "command") {
				let db = mClient.db(dbName || "myapp");
				let cmd = Object.assign({},req.body.commandBody || {});
				if (req.body.command && !cmd[req.body.command]) cmd[req.body.command] = 1;
				if (Object.keys(cmd).length == 0) {
					return sendJSON(res,400,{
						error: "Missing command. Provide `command` (e.g. 'ping') or `commandBody` (e.g. {\"buildInfo\":1}).",
					});
				}
				let result = await db.command(cmd);
				return sendJSON(res,200,{ ok: 1, result, });
			}

			if (!(dbName && collName)) {
				return sendJSON(res,400,{
					error: "'db' and 'coll' are required",
				});
			}
			let coll = mClient.db(dbName).collection(collName);

			switch (opName) {
				case "find": {
					let {
						filter = {},
						projection,
						sort,
						limit = 20,
						skip = 0,
					} = req.body;
					let docs = await coll.find(filter,{
						projection,
						sort,
						limit: Math.min(Number(limit) || 20, 500),
						skip: Number(skip) || 0,
					}).toArray();
					return sendJSON(res,200,{ ok: 1, docs, });
				}
				case "insertOne": {
					let { doc = {}, } = req.body;
					let r = await coll.insertOne(doc);
					return sendJSON(res,200,{
						ok: 1,
						insertedId: r.insertedId,
					});
				}
				case "updateOne": {
					let {
						filter = {},
						update = {},
						options = {},
					} = req.body;
					let r = await coll.updateOne(filter,update,{
						upsert: !!options.upsert,
					});
					return sendJSON(res,200,{
						ok: 1,
						matched: r.matchedCount,
						modified: r.modifiedCount,
						upsertedId: r.upsertedId,
					});
				}
				case "deleteOne": {
					let { filter = {} } = req.body;
					let r = await coll.deleteOne(filter);
					return sendJSON(res,200,{
						ok: 1,
						deletedCount: r.deletedCount,
					});
				}
				case "aggregate": {
					let { pipeline = [], options = {} } = req.body;
					let docs = await coll.aggregate(pipeline,options).toArray();
					return sendJSON(res,200,{ ok: 1, docs, });
				}
				default: {
					return sendJSON(res,400,{ error: "unknown op", });
				}
			}
		}
		catch (err) {
			console.log(err);
			return sendJSON(res,500,{ error: err.toString(), });
		}
	}
	else if (["GET","HEAD"].includes(req.method)) {
		if (!DEV) {
			// special cache expiration behavior for favicon
			if (/^\/favicon\.ico$/.test(req.url)) {
				try {
					await serveFile(req.url,200,{
						"Cache-Control": `public, max-age=${60*60*24*30}`,
						...HSTSHeader,
					},req,res);
				}
				catch (err) {
					// empty favicon.ico response
					res.writeHead(204,{
						"Content-Type": "image/x-icon",
						"Cache-Control": "public, max-age: 604800",
					});
					res.end();
				}
				return;
			}
		}

		// handle all other static files
		staticServer.serve(req,res,async function onStaticComplete(err){
			if (err) {
				try {
					return await serveFile("/index.html",200,{
						...HSTSHeader,
						...CSPHeader,
					},req,res);
				}
				catch (err2) {}

				res.writeHead(404);
				res.end();
			}
		});
	}
	else {
		res.writeHead(404);
		res.end();
	}
}

function serveFile(url,statusCode,headers,req,res) {
	var listener = staticServer.serveFile(url,statusCode,headers,req,res);
	return new Promise(function c(resolve,reject){
		listener.on("success",resolve);
		listener.on("error",reject);
	});
}

async function connectMongo() {
	var client = new MongoClient(MONGO_ENDPOINT,{
		maxPoolSize: 10,
	});

	await client.connect();

	console.log("MongoDB connected.");

	return client;
}

// async function disconnectMongo(client) {
// 	if (client) await client.close();
// }

function sendJSON(res,code,data) {
	res.writeHead(code,{
		"Content-Type": "application/json; charset=utf-8",
	});
	res.end(
		typeof data == "string" ? data : JSON.stringify(data)
	);
}
