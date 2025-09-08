"use strict";

var util = require("util");
var { spawn } = require("child_process");
var path = require("path");

var transport = null;
var queue = Promise.resolve();

const DEFAULT_TIMEOUT_MS = 10000;


module.exports = {
	execQuery,
	close,

	has: hasKV,
	get: getKV,
	set: setKV,
	remove: removeKV,

	logError,

	// not using WASM transport yet
	// init: initWASM,
	init: initCLI,
};


// **************************************

function execQuery(query,vars,timeoutMs) {
	if (!transport) {
		// not using WASM transport yet
		// initWASM();
		initCLI();
	}
	if (typeof query != "string" || query == "") {
		return Promise.reject(new Error("Query must be a non-empty string"));
	}

	var run = () => transport.execute({ query, vars, timeoutMs, });

	// keep the chain alive even after failures
	var p = queue.then(run,run);
	queue = p.catch(() => {});
	return p;
}

async function hasKV(key) {
	// var res = await execQuery(
	// 	`query hasKV($key:String!) {
	// 		KV(filter: { key: { _eq:$key } })
	// 		{ _docID }
	// 	}`,
	// 	{ key, }
	// );
	// if (res && res.KV) {
	// 	return res.KV.some(entry => entry.key == key);
	// }
	// return false;

	return transport.execute({ has: key, });
}

async function getKV(key) {
	// var res = await execQuery(
	// 	`query getKV($key:String!) {
	// 		KV(filter: { key: { _eq: $key } })
	// 		{ key value }
	// 	}`,
	// 	{ key, }
	// );
	// if (res && res.KV) {
	// 	return res.KV.find(entry => entry.key == key);
	// }

	return transport.execute({ get: key, });
}

async function setKV(key,value) {
	var now = new Date().toISOString();

	// Note: this style (passing in `value` as an external variable)
	// does not currently work, due to a bug in defra
	//
	// await execQuery(
	// 	`mutation setKV($key:String!,$value:JSON!,$now:DateTime!) {
	// 		upsert_KV(
	// 			filter: { key: { _eq: $key } }
	// 			create: { key: $key, value: $value, updatedAt: $now }
	// 			update: { value: $value, updatedAt: $now }
	// 		)
	//		{ _docID }
	// 	}`,
	// 	{ key, value, now, }
	// );

	// var valueLiteral = toGraphQLLiteral(value);
	// var res = await execQuery(
	// 	`mutation setKV($key:String!,$now:DateTime!) {
	// 		upsert_KV(
	// 			filter: { key: { _eq: $key } }
	// 			create: { key: $key, value: ${valueLiteral}, updatedAt: $now }
	// 			update: { value: ${valueLiteral}, updatedAt: $now }
	// 		)
	// 		{ _docID }
	// 	}`,
	// 	{ key, now, }
	// );

	// return (
	// 	res &&
	// 	Array.isArray(res.upsert_KV) &&
	// 	res.upsert_KV.length > 0
	// );

	return transport.execute({ set: { key, value, }, });
}

async function removeKV(key) {
	// var res = await execQuery(
	// 	`mutation removeKV($key:String!) {
	// 		delete_KV(
	// 			filter: { key: { _eq: $key } }
	// 		)
	// 		{ _docID }
	// 	}`,
	// 	{ key, }
	// );

	// return (
	// 	res &&
	// 	Array.isArray(res.delete_KV) &&
	// 	res.delete_KV.length > 0
	// );

	return transport.execute({ remove: key, });
}

function close() {
	if (!transport || typeof transport.close !== "function") {
		return Promise.resolve();
	}
	return transport.close();
}


// **************************************

// current (hack)
function initCLI() {
	var bin = path.resolve(process.cwd(),"defra-kv");
	var dataConfigDir = path.resolve(process.cwd(),".defra-kv");
	var keyringSecret = "dev-dev-dev";
	var dev = false;

	transport = {
		execute({ query, vars, has, get, set, remove, timeoutMs, } = {}) {
			return execCLI({
				bin,
				dataConfigDir,
				keyringSecret,
				dev,
				query,
				vars,
				has,
				get,
				set,
				remove,
				timeoutMs,
			})
		},

		close: async () => {},
	};
}

function execCLI({
	bin,
	dataConfigDir,
	keyringSecret = process.env.DEFRA_KEYRING_SECRET,
	dev,
	query,
	vars,
	has,
	get,
	set,
	remove,
	timeoutMs,
} = {}) {
	var tmo = Number.isFinite(timeoutMs) ? timeoutMs : DEFAULT_TIMEOUT_MS;

	var args = [
		"-dir", dataConfigDir,
		"-timeout", `${Math.max(1,Math.ceil(tmo / 1000))}s`,
		"-pretty=false",
	];
	if (dev) {
		args.push("-dev");
	}
	// query/vars takes precedence over KV actions
	if (typeof query == "string" && query != "") {
		if (vars != null) {
			args.push("-vars",JSON.stringify(vars));
		}
	}
	else if (typeof has == "string" && has != "") {
		args.push("-has",has);
	}
	else if (typeof get == "string" && get != "") {
		args.push("-get",get);
	}
	else if (set && typeof set.key == "string" && set.key != "") {
		args.push("-set",set.key);
	}
	else if (typeof remove == "string" && remove != "") {
		args.push("-remove",remove);
	}

	var env = {
		...process.env,
		...(
			!!keyringSecret ?
				{ DEFRA_KEYRING_SECRET: keyringSecret, } :
				null
		),
	};

	return new Promise((resolve,reject) => {
		var child = spawn(
			bin,
			args,
			{
				stdio: ["pipe","pipe","pipe",],
				cwd: process.cwd(),
				env,
			}
		);

		var stdout = "";
		var stderr = "";
		var timedOut = false;

		var timer = setTimeout(
			() => {
				timedOut = true;
				child.kill("SIGKILL");
			},

			// cushion beyond CLI's own -timeout
			tmo + 1000
		);

		child.stdout.on("data",d => { stdout += d.toString("utf8"); });
		child.stderr.on("data",d => { stderr += d.toString("utf8"); });
		child.on("error", (err) => {
			clearTimeout(timer);
			reject(new Error(`Failed to spawn defra-kv: ${err.message}`));
		});

		child.on("close",code => {
			clearTimeout(timer);
			if (timedOut) return reject(new Error(`defra-kv timed out after ${tmo}ms`));

			// explicit exit-code handling for KV actions?
			if (
				args.includes("-has") ||
				args.includes("-get") ||
				args.includes("-set") ||
				args.includes("-remove")
			) {
				if (code === 0) {
					if (args.includes("-get")) {
						try {
							resolve(JSON.parse(stdout));
						}
						catch (err) {
							reject(
								new Error(
									`Could not parse defra-kv output: ${stdout}`,
									{
										cause: err,
									}
								)
							);
						}
					}
					else {
						return resolve(true);
					}
				}
				else if (code == 3) {
					return resolve(false);
				}
				else {
					return reject(
						new Error(`defra-kv exited with code: ${code}`)
					);
				}
			}
			else if (code !== 0) {
				// stderr may contain JSON array of errors or plain text
				return reject(
					new GraphQLError(
						`defra-kv exited with code: ${code}`,
						{ stdout, stderr, }
					)
				);
			}
			else {
				try {
					var parsed = JSON.parse(stdout); // {"data": ...}
					resolve(parsed.data);
				}
				catch (err) {
					reject(
						new GraphQLError(
							"Could not parse defra-kv output",
							{
								stdout,
								cause: err,
							}
						)
					);
				}
			}
		});

		// feed query/set via stdin to avoid shell quoting issues
		if (query || (set && set.value)) {
			child.stdin.write(query || JSON.stringify(set.value));
		}
		child.stdin.end();
	});
}

// (future)
function initWASM({ instance, DEFAULT_TIMEOUT_MS = 10000 } = {}) {
	if (!instance || typeof instance.exec !== "function") {
		throw new Error("initWASM requires an instance exposing exec(query, vars, {timeoutMs})");
	}
	transport = {
		async execute({ query, vars, timeoutMs, } = {}) {
			var tmo = Number.isFinite(timeoutMs) ? timeoutMs : DEFAULT_TIMEOUT_MS;
			// expect instance.exec to throw on errors, return {"data": ...} or just the data
			var out = await instance.exec(
				query,
				vars,
				{ timeoutMs: tmo, }
			);
			// normalize to "data" like the CLI
			return out && out.data !== undefined ? out.data : out;
		},
		async close() {
			if (typeof instance.close == "function") {
				await instance.close();
			}
		},
	};
	return state;
}

function logError(err) {
	if (err.errors && err.errors.length > 0) {
		console.error(util.inspect(err.errors,{depth:10}));
	}
	else if (err.stderr) {
		console.error(err.stderr);
	}
	else {
		console.error(String(err));
	}
}

class GraphQLError extends Error {
	constructor(
		message,
		{
			errors = [],
			stdout = "",
			stderr = "",
			cause,
		} = {}
	) {
		super(message,cause ? { cause, } : undefined);

		this.name = "GraphQLError";

		if (stderr != "") {
			try {
				// TODO: remove temporary hack to skip the sonic warning
				stderr = stderr.replace(
					"WARNING:(ast) sonic only supports go1.17~1.23, but your environment is not suitable\n",
					""
				);

				stderr = JSON.parse(stderr);
				if (Array.isArray(stderr)) {
					this.errors = stderr;
					stderr = "";
				}
			}
			catch (err) {}
		}
		else {
			this.errors = errors;
		}
		if (stdout) {
			this.stdout = stdout;
		}
		if (stderr) {
			this.stderr = stderr;
		}

		if (Error.captureStackTrace) {
			Error.captureStackTrace(this,GraphQLError);
		}
	}
}

// Convert a JS value into a GraphQL input literal (strings quoted, keys unquoted)
//
// Note: temporary hack, only needed because can't send JSON/object value
// into defra as external variable currently, have to inline them into
// GraphQL query
function toGraphQLLiteral(v) {
	if (v == null) return "null";
	var t = typeof v;

	if (t == "string") return JSON.stringify(v);                 // "..."
	if (t == "number") {
		if (!Number.isFinite(v)) throw new Error("Non-finite number not allowed");
		return String(v);
	}
	if (t == "boolean") return v ? "true" : "false";
	if (v instanceof Date) return JSON.stringify(v.toISOString());
	if (Array.isArray(v)) return `[${v.map(toGraphQLLiteral).join(", ")}]`;

	if (t == "object") {
		let fields = Object.entries(v)
			.filter(([, val ]) => val != null)
			.map(([ k, val ]) => {
				if (!/^[_A-Za-z][_0-9A-Za-z]*$/.test(k)) {
					throw new Error(`Key "${k}" is not a valid GraphQL name; can't inline`);
				}
				return `${k}: ${toGraphQLLiteral(val)}`;
			})
			.join(", ");
		return `{${fields}}`;
	}

	throw new Error(`Unsupported type: ${t}`);
}
