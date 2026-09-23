import { createServer } from "node:http";
import { readFile, realpath, stat } from "node:fs/promises";
import { extname, relative, resolve, sep } from "node:path";
import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import PDFDocument from "pdfkit";
//#region server/db.mjs
function postgresSql(sql) {
	let n = 0;
	return sql.replace(/\?/g, () => `$${++n}`);
}
function mutex() {
	let tail = Promise.resolve();
	return async (fn) => {
		const previous = tail;
		let release;
		tail = new Promise((r) => {
			release = r;
		});
		await previous;
		try {
			return await fn();
		} finally {
			release();
		}
	};
}
/** SQLite is deliberately available only to isolated local tests. Production must have PostgreSQL. */
async function createDatabase(env = process.env) {
	if (env.DATABASE_URL) {
		const { Pool } = await import("pg");
		const pool = new Pool({
			connectionString: env.DATABASE_URL,
			max: 5,
			connectionTimeoutMillis: 1e4,
			idleTimeoutMillis: 3e4
		});
		pool.on("error", () => {
			console.error("Временная ошибка соединения с базой данных.");
		});
		const queryWith = (client) => async (sql, args = []) => {
			const result = await client.query(postgresSql(sql), args);
			return {
				rows: result.rows,
				changes: result.rowCount
			};
		};
		const db = {
			dialect: "postgres",
			query: queryWith(pool),
			async transaction(fn) {
				const client = await pool.connect();
				try {
					await client.query("BEGIN");
					const result = await fn({
						dialect: "postgres",
						query: queryWith(client)
					});
					await client.query("COMMIT");
					return result;
				} catch (error) {
					await client.query("ROLLBACK");
					throw error;
				} finally {
					client.release();
				}
			},
			async close() {
				await pool.end();
			}
		};
		await migrate(db);
		return db;
	}
	if (env.NODE_ENV !== "test" || !env.TEST_SQLITE_PATH || env.RENDER || env.RENDER_EXTERNAL_URL) throw new Error("DATABASE_URL обязателен. Резервное локальное хранилище в Render не используется.");
	const { DatabaseSync } = await import("node:sqlite");
	const sqlite = new DatabaseSync(env.TEST_SQLITE_PATH);
	sqlite.exec("PRAGMA foreign_keys=ON");
	sqlite.exec("PRAGMA journal_mode=WAL");
	const lock = mutex();
	const directQuery = async (sql, args = []) => {
		const stmt = sqlite.prepare(sql);
		if (/^\s*(SELECT|PRAGMA|WITH)/i.test(sql)) return {
			rows: stmt.all(...args),
			changes: 0
		};
		const result = stmt.run(...args);
		return {
			rows: [],
			changes: Number(result.changes)
		};
	};
	const db = {
		dialect: "sqlite",
		query: (sql, args) => lock(() => directQuery(sql, args)),
		transaction: (fn) => lock(async () => {
			sqlite.exec("BEGIN IMMEDIATE");
			try {
				const result = await fn({
					dialect: "sqlite",
					query: directQuery
				});
				sqlite.exec("COMMIT");
				return result;
			} catch (error) {
				sqlite.exec("ROLLBACK");
				throw error;
			}
		}),
		async close() {
			await lock(() => sqlite.close());
		}
	};
	await migrate(db);
	return db;
}
var migration1 = [
	`CREATE TABLE IF NOT EXISTS sessions (token_hash TEXT PRIMARY KEY, client_id TEXT NOT NULL, role TEXT NOT NULL, auth_version TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, expires_at TEXT NOT NULL)`,
	`CREATE INDEX IF NOT EXISTS sessions_expiry_idx ON sessions(expires_at)`,
	`CREATE TABLE IF NOT EXISTS auth_attempts (key_hash TEXT PRIMARY KEY, failures INTEGER NOT NULL, window_at TEXT NOT NULL, blocked_until TEXT NOT NULL)`,
	`CREATE TABLE IF NOT EXISTS travel_requests (id TEXT PRIMARY KEY, client_id TEXT NOT NULL, input_json TEXT NOT NULL, journey_id TEXT, journey_json TEXT, candidate_id TEXT, status TEXT NOT NULL, customer_name TEXT NOT NULL, customer_email TEXT NOT NULL, contact TEXT NOT NULL, notes TEXT NOT NULL, manager_note TEXT NOT NULL DEFAULT '', internal_note TEXT NOT NULL DEFAULT '', candidates_json TEXT NOT NULL, quote_json TEXT, revision INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
	`CREATE INDEX IF NOT EXISTS requests_client_idx ON travel_requests(client_id,created_at)`,
	`CREATE TABLE IF NOT EXISTS request_versions (id TEXT PRIMARY KEY, request_id TEXT NOT NULL REFERENCES travel_requests(id), revision INTEGER NOT NULL, action TEXT NOT NULL, actor_role TEXT NOT NULL, snapshot_json TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(request_id,revision))`,
	`CREATE TABLE IF NOT EXISTS travel_catalog (id TEXT PRIMARY KEY, data_json TEXT NOT NULL, updated_at TEXT NOT NULL)`,
	`CREATE TABLE IF NOT EXISTS travel_materials (id TEXT PRIMARY KEY, title TEXT NOT NULL, category TEXT NOT NULL, source_url TEXT NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL)`,
	`CREATE TABLE IF NOT EXISTS email_outbox (id TEXT PRIMARY KEY, dedupe_key TEXT NOT NULL UNIQUE, request_id TEXT NOT NULL REFERENCES travel_requests(id), request_revision INTEGER NOT NULL, kind TEXT NOT NULL, mode TEXT NOT NULL, intended_to TEXT NOT NULL, recipient TEXT NOT NULL, subject TEXT NOT NULL, html TEXT NOT NULL, text_body TEXT NOT NULL, reply_to TEXT NOT NULL, status TEXT NOT NULL, provider_id TEXT, attempts INTEGER NOT NULL DEFAULT 0, ambiguous INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT NOT NULL, lease_until TEXT, last_error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL, accepted_at TEXT, delivered_at TEXT)`,
	`CREATE INDEX IF NOT EXISTS outbox_queue_idx ON email_outbox(status,next_attempt_at)`
];
async function migrate(db) {
	await db.transaction(async (tx) => {
		if (tx.dialect === "postgres") await tx.query("SELECT pg_advisory_xact_lock(724901823)");
		await tx.query("CREATE TABLE IF NOT EXISTS schema_versions (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)");
		if (!(await tx.query("SELECT version FROM schema_versions WHERE version=?", [1])).rows.length) {
			for (const sql of migration1) await tx.query(sql);
			await tx.query("INSERT INTO schema_versions(version,applied_at) VALUES(?,?)", [1, (/* @__PURE__ */ new Date()).toISOString()]);
		}
		if (!(await tx.query("SELECT version FROM schema_versions WHERE version=?", [2])).rows.length) {
			await tx.query("ALTER TABLE email_outbox ADD COLUMN sender TEXT NOT NULL DEFAULT ''");
			await tx.query("INSERT INTO schema_versions(version,applied_at) VALUES(?,?)", [2, (/* @__PURE__ */ new Date()).toISOString()]);
		}
	});
}
function requestFromRow(row) {
	return {
		id: row.id,
		clientId: row.client_id,
		input: JSON.parse(row.input_json),
		journeyId: row.journey_id,
		journey: row.journey_json ? JSON.parse(row.journey_json) : null,
		candidateId: row.candidate_id,
		status: row.status,
		customerName: row.customer_name,
		customerEmail: row.customer_email,
		contact: row.contact,
		notes: row.notes,
		managerNote: row.manager_note,
		internalNote: row.internal_note,
		candidates: JSON.parse(row.candidates_json),
		quote: row.quote_json ? JSON.parse(row.quote_json) : null,
		revision: row.revision,
		createdAt: row.created_at,
		updatedAt: row.updated_at
	};
}
async function saveVersion(tx, request, action, actorRole) {
	await tx.query("INSERT INTO request_versions(id,request_id,revision,action,actor_role,snapshot_json,created_at) VALUES(?,?,?,?,?,?,?)", [
		randomUUID(),
		request.id,
		request.revision,
		action,
		actorRole,
		JSON.stringify(request),
		request.updatedAt
	]);
}
async function updateStoredRequest(tx, request, previousRevision, action, actorRole) {
	if ((await tx.query("UPDATE travel_requests SET journey_id=?,journey_json=?,candidate_id=?,status=?,manager_note=?,internal_note=?,candidates_json=?,quote_json=?,revision=?,updated_at=? WHERE id=? AND revision=?", [
		request.journeyId,
		request.journey ? JSON.stringify(request.journey) : null,
		request.candidateId,
		request.status,
		request.managerNote,
		request.internalNote,
		JSON.stringify(request.candidates),
		request.quote ? JSON.stringify(request.quote) : null,
		request.revision,
		request.updatedAt,
		request.id,
		previousRevision
	])).changes !== 1) {
		const error = /* @__PURE__ */ new Error("Заявка уже изменена. Обновите страницу и повторите действие.");
		error.status = 409;
		throw error;
	}
	await saveVersion(tx, request, action, actorRole);
}
//#endregion
//#region server/auth.mjs
var cookieName = "tiare_session";
var hash = (value) => createHash("sha256").update(value).digest("hex");
var passwordVersion = (env) => env.MANAGER_PASSWORD ? hash("tiare-manager-v1:" + env.MANAGER_PASSWORD) : "";
var day$1 = 864e5;
function cookieValue(request) {
	const found = (request.headers.cookie || "").split(";").map((part) => part.trim()).find((part) => part.startsWith(cookieName + "="));
	if (!found) return "";
	const token = found.slice(14);
	return /^[A-Za-z0-9_-]{43}$/.test(token) ? token : "";
}
function setSessionCookie(response, token, env) {
	const secure = !!(env.RENDER || env.NODE_ENV === "production" || (env.APP_BASE_URL || env.RENDER_EXTERNAL_URL || "").startsWith("https://"));
	response.setHeader("Set-Cookie", `${cookieName}=${token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${30 * 86400}${secure ? "; Secure" : ""}`);
}
async function sessionForRequest(db, request, response, env) {
	const token = cookieValue(request), now = (/* @__PURE__ */ new Date()).toISOString();
	if (token) {
		const { rows } = await db.query("SELECT * FROM sessions WHERE token_hash=? AND expires_at>?", [hash(token), now]);
		if (rows[0]) {
			const session = rows[0];
			if (session.role !== "manager" || session.auth_version && session.auth_version === passwordVersion(env)) return {
				tokenHash: session.token_hash,
				clientId: session.client_id,
				role: session.role
			};
			await db.query("DELETE FROM sessions WHERE token_hash=?", [hash(token)]);
		}
	}
	return createSession(db, response, env, "client", randomUUID());
}
async function createSession(db, response, env, role, clientId) {
	const token = randomBytes(32).toString("base64url"), now = Date.now(), tokenHash = hash(token);
	await db.query("INSERT INTO sessions(token_hash,client_id,role,auth_version,created_at,expires_at) VALUES(?,?,?,?,?,?)", [
		tokenHash,
		clientId,
		role,
		role === "manager" ? passwordVersion(env) : "",
		new Date(now).toISOString(),
		new Date(now + 30 * day$1).toISOString()
	]);
	setSessionCookie(response, token, env);
	return {
		tokenHash,
		clientId,
		role
	};
}
function requesterKey(request) {
	return hash("manager-login:" + (request.socket.remoteAddress || "unknown"));
}
async function managerLogin(db, request, response, env, session, password) {
	if (!env.MANAGER_PASSWORD || env.MANAGER_PASSWORD.length < 16) {
		const e = /* @__PURE__ */ new Error("Вход менеджера пока не настроен.");
		e.status = 503;
		throw e;
	}
	const key = requesterKey(request), now = Date.now();
	if (!await db.transaction(async (tx) => {
		if (tx.dialect === "postgres") await tx.query("SELECT pg_advisory_xact_lock(hashtext(?))", [key]);
		const { rows } = await tx.query("SELECT * FROM auth_attempts WHERE key_hash=?", [key]);
		const previous = rows[0];
		if (previous && Date.parse(previous.blocked_until) > now) return false;
		const previousCount = previous && now - Date.parse(previous.window_at) < 15 * 6e4 ? previous.failures : 0;
		const failures = previousCount + 1, windowAt = previousCount ? previous.window_at : new Date(now).toISOString(), blocked = failures >= 5 ? new Date(now + 15 * 6e4).toISOString() : "";
		await tx.query("INSERT INTO auth_attempts(key_hash,failures,window_at,blocked_until) VALUES(?,?,?,?) ON CONFLICT(key_hash) DO UPDATE SET failures=excluded.failures,window_at=excluded.window_at,blocked_until=excluded.blocked_until", [
			key,
			failures,
			windowAt,
			blocked
		]);
		return true;
	})) {
		const e = /* @__PURE__ */ new Error("Слишком много попыток входа. Повторите через 15 минут.");
		e.status = 429;
		throw e;
	}
	if (!timingSafeEqual(createHash("sha256").update(typeof password === "string" ? password : "").digest(), createHash("sha256").update(env.MANAGER_PASSWORD).digest())) {
		const e = /* @__PURE__ */ new Error("Неверный пароль.");
		e.status = 401;
		throw e;
	}
	await db.query("DELETE FROM auth_attempts WHERE key_hash=?", [key]);
	await db.query("DELETE FROM sessions WHERE token_hash=?", [session.tokenHash]);
	return createSession(db, response, env, "manager", session.clientId);
}
async function managerLogout(db, response, env, session) {
	await db.query("DELETE FROM sessions WHERE token_hash=?", [session.tokenHash]);
	return createSession(db, response, env, "client", session.clientId);
}
function requireManager(session) {
	if (session.role !== "manager") {
		const e = /* @__PURE__ */ new Error("Это действие доступно только менеджеру.");
		e.status = 403;
		throw e;
	}
}
//#endregion
//#region node_modules/zod/v3/helpers/util.js
var util;
(function(util) {
	util.assertEqual = (_) => {};
	function assertIs(_arg) {}
	util.assertIs = assertIs;
	function assertNever(_x) {
		throw new Error();
	}
	util.assertNever = assertNever;
	util.arrayToEnum = (items) => {
		const obj = {};
		for (const item of items) obj[item] = item;
		return obj;
	};
	util.getValidEnumValues = (obj) => {
		const validKeys = util.objectKeys(obj).filter((k) => typeof obj[obj[k]] !== "number");
		const filtered = {};
		for (const k of validKeys) filtered[k] = obj[k];
		return util.objectValues(filtered);
	};
	util.objectValues = (obj) => {
		return util.objectKeys(obj).map(function(e) {
			return obj[e];
		});
	};
	util.objectKeys = typeof Object.keys === "function" ? (obj) => Object.keys(obj) : (object) => {
		const keys = [];
		for (const key in object) if (Object.prototype.hasOwnProperty.call(object, key)) keys.push(key);
		return keys;
	};
	util.find = (arr, checker) => {
		for (const item of arr) if (checker(item)) return item;
	};
	util.isInteger = typeof Number.isInteger === "function" ? (val) => Number.isInteger(val) : (val) => typeof val === "number" && Number.isFinite(val) && Math.floor(val) === val;
	function joinValues(array, separator = " | ") {
		return array.map((val) => typeof val === "string" ? `'${val}'` : val).join(separator);
	}
	util.joinValues = joinValues;
	util.jsonStringifyReplacer = (_, value) => {
		if (typeof value === "bigint") return value.toString();
		return value;
	};
})(util || (util = {}));
var objectUtil;
(function(objectUtil) {
	objectUtil.mergeShapes = (first, second) => {
		return {
			...first,
			...second
		};
	};
})(objectUtil || (objectUtil = {}));
var ZodParsedType = util.arrayToEnum([
	"string",
	"nan",
	"number",
	"integer",
	"float",
	"boolean",
	"date",
	"bigint",
	"symbol",
	"function",
	"undefined",
	"null",
	"array",
	"object",
	"unknown",
	"promise",
	"void",
	"never",
	"map",
	"set"
]);
var getParsedType = (data) => {
	switch (typeof data) {
		case "undefined": return ZodParsedType.undefined;
		case "string": return ZodParsedType.string;
		case "number": return Number.isNaN(data) ? ZodParsedType.nan : ZodParsedType.number;
		case "boolean": return ZodParsedType.boolean;
		case "function": return ZodParsedType.function;
		case "bigint": return ZodParsedType.bigint;
		case "symbol": return ZodParsedType.symbol;
		case "object":
			if (Array.isArray(data)) return ZodParsedType.array;
			if (data === null) return ZodParsedType.null;
			if (data.then && typeof data.then === "function" && data.catch && typeof data.catch === "function") return ZodParsedType.promise;
			if (typeof Map !== "undefined" && data instanceof Map) return ZodParsedType.map;
			if (typeof Set !== "undefined" && data instanceof Set) return ZodParsedType.set;
			if (typeof Date !== "undefined" && data instanceof Date) return ZodParsedType.date;
			return ZodParsedType.object;
		default: return ZodParsedType.unknown;
	}
};
//#endregion
//#region node_modules/zod/v3/ZodError.js
var ZodIssueCode = util.arrayToEnum([
	"invalid_type",
	"invalid_literal",
	"custom",
	"invalid_union",
	"invalid_union_discriminator",
	"invalid_enum_value",
	"unrecognized_keys",
	"invalid_arguments",
	"invalid_return_type",
	"invalid_date",
	"invalid_string",
	"too_small",
	"too_big",
	"invalid_intersection_types",
	"not_multiple_of",
	"not_finite"
]);
var ZodError = class ZodError extends Error {
	get errors() {
		return this.issues;
	}
	constructor(issues) {
		super();
		this.issues = [];
		this.addIssue = (sub) => {
			this.issues = [...this.issues, sub];
		};
		this.addIssues = (subs = []) => {
			this.issues = [...this.issues, ...subs];
		};
		const actualProto = new.target.prototype;
		if (Object.setPrototypeOf) Object.setPrototypeOf(this, actualProto);
		else this.__proto__ = actualProto;
		this.name = "ZodError";
		this.issues = issues;
	}
	format(_mapper) {
		const mapper = _mapper || function(issue) {
			return issue.message;
		};
		const fieldErrors = { _errors: [] };
		const processError = (error) => {
			for (const issue of error.issues) if (issue.code === "invalid_union") issue.unionErrors.map(processError);
			else if (issue.code === "invalid_return_type") processError(issue.returnTypeError);
			else if (issue.code === "invalid_arguments") processError(issue.argumentsError);
			else if (issue.path.length === 0) fieldErrors._errors.push(mapper(issue));
			else {
				let curr = fieldErrors;
				let i = 0;
				while (i < issue.path.length) {
					const el = issue.path[i];
					if (!(i === issue.path.length - 1)) curr[el] = curr[el] || { _errors: [] };
					else {
						curr[el] = curr[el] || { _errors: [] };
						curr[el]._errors.push(mapper(issue));
					}
					curr = curr[el];
					i++;
				}
			}
		};
		processError(this);
		return fieldErrors;
	}
	static assert(value) {
		if (!(value instanceof ZodError)) throw new Error(`Not a ZodError: ${value}`);
	}
	toString() {
		return this.message;
	}
	get message() {
		return JSON.stringify(this.issues, util.jsonStringifyReplacer, 2);
	}
	get isEmpty() {
		return this.issues.length === 0;
	}
	flatten(mapper = (issue) => issue.message) {
		const fieldErrors = {};
		const formErrors = [];
		for (const sub of this.issues) if (sub.path.length > 0) {
			const firstEl = sub.path[0];
			fieldErrors[firstEl] = fieldErrors[firstEl] || [];
			fieldErrors[firstEl].push(mapper(sub));
		} else formErrors.push(mapper(sub));
		return {
			formErrors,
			fieldErrors
		};
	}
	get formErrors() {
		return this.flatten();
	}
};
ZodError.create = (issues) => {
	return new ZodError(issues);
};
//#endregion
//#region node_modules/zod/v3/locales/en.js
var errorMap = (issue, _ctx) => {
	let message;
	switch (issue.code) {
		case ZodIssueCode.invalid_type:
			if (issue.received === ZodParsedType.undefined) message = "Required";
			else message = `Expected ${issue.expected}, received ${issue.received}`;
			break;
		case ZodIssueCode.invalid_literal:
			message = `Invalid literal value, expected ${JSON.stringify(issue.expected, util.jsonStringifyReplacer)}`;
			break;
		case ZodIssueCode.unrecognized_keys:
			message = `Unrecognized key(s) in object: ${util.joinValues(issue.keys, ", ")}`;
			break;
		case ZodIssueCode.invalid_union:
			message = `Invalid input`;
			break;
		case ZodIssueCode.invalid_union_discriminator:
			message = `Invalid discriminator value. Expected ${util.joinValues(issue.options)}`;
			break;
		case ZodIssueCode.invalid_enum_value:
			message = `Invalid enum value. Expected ${util.joinValues(issue.options)}, received '${issue.received}'`;
			break;
		case ZodIssueCode.invalid_arguments:
			message = `Invalid function arguments`;
			break;
		case ZodIssueCode.invalid_return_type:
			message = `Invalid function return type`;
			break;
		case ZodIssueCode.invalid_date:
			message = `Invalid date`;
			break;
		case ZodIssueCode.invalid_string:
			if (typeof issue.validation === "object") if ("includes" in issue.validation) {
				message = `Invalid input: must include "${issue.validation.includes}"`;
				if (typeof issue.validation.position === "number") message = `${message} at one or more positions greater than or equal to ${issue.validation.position}`;
			} else if ("startsWith" in issue.validation) message = `Invalid input: must start with "${issue.validation.startsWith}"`;
			else if ("endsWith" in issue.validation) message = `Invalid input: must end with "${issue.validation.endsWith}"`;
			else util.assertNever(issue.validation);
			else if (issue.validation !== "regex") message = `Invalid ${issue.validation}`;
			else message = "Invalid";
			break;
		case ZodIssueCode.too_small:
			if (issue.type === "array") message = `Array must contain ${issue.exact ? "exactly" : issue.inclusive ? `at least` : `more than`} ${issue.minimum} element(s)`;
			else if (issue.type === "string") message = `String must contain ${issue.exact ? "exactly" : issue.inclusive ? `at least` : `over`} ${issue.minimum} character(s)`;
			else if (issue.type === "number") message = `Number must be ${issue.exact ? `exactly equal to ` : issue.inclusive ? `greater than or equal to ` : `greater than `}${issue.minimum}`;
			else if (issue.type === "bigint") message = `Number must be ${issue.exact ? `exactly equal to ` : issue.inclusive ? `greater than or equal to ` : `greater than `}${issue.minimum}`;
			else if (issue.type === "date") message = `Date must be ${issue.exact ? `exactly equal to ` : issue.inclusive ? `greater than or equal to ` : `greater than `}${new Date(Number(issue.minimum))}`;
			else message = "Invalid input";
			break;
		case ZodIssueCode.too_big:
			if (issue.type === "array") message = `Array must contain ${issue.exact ? `exactly` : issue.inclusive ? `at most` : `less than`} ${issue.maximum} element(s)`;
			else if (issue.type === "string") message = `String must contain ${issue.exact ? `exactly` : issue.inclusive ? `at most` : `under`} ${issue.maximum} character(s)`;
			else if (issue.type === "number") message = `Number must be ${issue.exact ? `exactly` : issue.inclusive ? `less than or equal to` : `less than`} ${issue.maximum}`;
			else if (issue.type === "bigint") message = `BigInt must be ${issue.exact ? `exactly` : issue.inclusive ? `less than or equal to` : `less than`} ${issue.maximum}`;
			else if (issue.type === "date") message = `Date must be ${issue.exact ? `exactly` : issue.inclusive ? `smaller than or equal to` : `smaller than`} ${new Date(Number(issue.maximum))}`;
			else message = "Invalid input";
			break;
		case ZodIssueCode.custom:
			message = `Invalid input`;
			break;
		case ZodIssueCode.invalid_intersection_types:
			message = `Intersection results could not be merged`;
			break;
		case ZodIssueCode.not_multiple_of:
			message = `Number must be a multiple of ${issue.multipleOf}`;
			break;
		case ZodIssueCode.not_finite:
			message = "Number must be finite";
			break;
		default:
			message = _ctx.defaultError;
			util.assertNever(issue);
	}
	return { message };
};
//#endregion
//#region node_modules/zod/v3/errors.js
var overrideErrorMap = errorMap;
function getErrorMap() {
	return overrideErrorMap;
}
//#endregion
//#region node_modules/zod/v3/helpers/parseUtil.js
var makeIssue = (params) => {
	const { data, path, errorMaps, issueData } = params;
	const fullPath = [...path, ...issueData.path || []];
	const fullIssue = {
		...issueData,
		path: fullPath
	};
	if (issueData.message !== void 0) return {
		...issueData,
		path: fullPath,
		message: issueData.message
	};
	let errorMessage = "";
	const maps = errorMaps.filter((m) => !!m).slice().reverse();
	for (const map of maps) errorMessage = map(fullIssue, {
		data,
		defaultError: errorMessage
	}).message;
	return {
		...issueData,
		path: fullPath,
		message: errorMessage
	};
};
function addIssueToContext(ctx, issueData) {
	const overrideMap = getErrorMap();
	const issue = makeIssue({
		issueData,
		data: ctx.data,
		path: ctx.path,
		errorMaps: [
			ctx.common.contextualErrorMap,
			ctx.schemaErrorMap,
			overrideMap,
			overrideMap === errorMap ? void 0 : errorMap
		].filter((x) => !!x)
	});
	ctx.common.issues.push(issue);
}
var ParseStatus = class ParseStatus {
	constructor() {
		this.value = "valid";
	}
	dirty() {
		if (this.value === "valid") this.value = "dirty";
	}
	abort() {
		if (this.value !== "aborted") this.value = "aborted";
	}
	static mergeArray(status, results) {
		const arrayValue = [];
		for (const s of results) {
			if (s.status === "aborted") return INVALID;
			if (s.status === "dirty") status.dirty();
			arrayValue.push(s.value);
		}
		return {
			status: status.value,
			value: arrayValue
		};
	}
	static async mergeObjectAsync(status, pairs) {
		const syncPairs = [];
		for (const pair of pairs) {
			const key = await pair.key;
			const value = await pair.value;
			syncPairs.push({
				key,
				value
			});
		}
		return ParseStatus.mergeObjectSync(status, syncPairs);
	}
	static mergeObjectSync(status, pairs) {
		const finalObject = {};
		for (const pair of pairs) {
			const { key, value } = pair;
			if (key.status === "aborted") return INVALID;
			if (value.status === "aborted") return INVALID;
			if (key.status === "dirty") status.dirty();
			if (value.status === "dirty") status.dirty();
			if (key.value !== "__proto__" && (typeof value.value !== "undefined" || pair.alwaysSet)) finalObject[key.value] = value.value;
		}
		return {
			status: status.value,
			value: finalObject
		};
	}
};
var INVALID = Object.freeze({ status: "aborted" });
var DIRTY = (value) => ({
	status: "dirty",
	value
});
var OK = (value) => ({
	status: "valid",
	value
});
var isAborted = (x) => x.status === "aborted";
var isDirty = (x) => x.status === "dirty";
var isValid = (x) => x.status === "valid";
var isAsync = (x) => typeof Promise !== "undefined" && x instanceof Promise;
//#endregion
//#region node_modules/zod/v3/helpers/errorUtil.js
var errorUtil;
(function(errorUtil) {
	errorUtil.errToObj = (message) => typeof message === "string" ? { message } : message || {};
	errorUtil.toString = (message) => typeof message === "string" ? message : message?.message;
})(errorUtil || (errorUtil = {}));
//#endregion
//#region node_modules/zod/v3/types.js
var ParseInputLazyPath = class {
	constructor(parent, value, path, key) {
		this._cachedPath = [];
		this.parent = parent;
		this.data = value;
		this._path = path;
		this._key = key;
	}
	get path() {
		if (!this._cachedPath.length) if (Array.isArray(this._key)) this._cachedPath.push(...this._path, ...this._key);
		else this._cachedPath.push(...this._path, this._key);
		return this._cachedPath;
	}
};
var handleResult = (ctx, result) => {
	if (isValid(result)) return {
		success: true,
		data: result.value
	};
	else {
		if (!ctx.common.issues.length) throw new Error("Validation failed but no issues detected.");
		return {
			success: false,
			get error() {
				if (this._error) return this._error;
				const error = new ZodError(ctx.common.issues);
				this._error = error;
				return this._error;
			}
		};
	}
};
function processCreateParams(params) {
	if (!params) return {};
	const { errorMap, invalid_type_error, required_error, description } = params;
	if (errorMap && (invalid_type_error || required_error)) throw new Error(`Can't use "invalid_type_error" or "required_error" in conjunction with custom error map.`);
	if (errorMap) return {
		errorMap,
		description
	};
	const customMap = (iss, ctx) => {
		const { message } = params;
		if (iss.code === "invalid_enum_value") return { message: message ?? ctx.defaultError };
		if (typeof ctx.data === "undefined") return { message: message ?? required_error ?? ctx.defaultError };
		if (iss.code !== "invalid_type") return { message: ctx.defaultError };
		return { message: message ?? invalid_type_error ?? ctx.defaultError };
	};
	return {
		errorMap: customMap,
		description
	};
}
var ZodType = class {
	get description() {
		return this._def.description;
	}
	_getType(input) {
		return getParsedType(input.data);
	}
	_getOrReturnCtx(input, ctx) {
		return ctx || {
			common: input.parent.common,
			data: input.data,
			parsedType: getParsedType(input.data),
			schemaErrorMap: this._def.errorMap,
			path: input.path,
			parent: input.parent
		};
	}
	_processInputParams(input) {
		return {
			status: new ParseStatus(),
			ctx: {
				common: input.parent.common,
				data: input.data,
				parsedType: getParsedType(input.data),
				schemaErrorMap: this._def.errorMap,
				path: input.path,
				parent: input.parent
			}
		};
	}
	_parseSync(input) {
		const result = this._parse(input);
		if (isAsync(result)) throw new Error("Synchronous parse encountered promise.");
		return result;
	}
	_parseAsync(input) {
		const result = this._parse(input);
		return Promise.resolve(result);
	}
	parse(data, params) {
		const result = this.safeParse(data, params);
		if (result.success) return result.data;
		throw result.error;
	}
	safeParse(data, params) {
		const ctx = {
			common: {
				issues: [],
				async: params?.async ?? false,
				contextualErrorMap: params?.errorMap
			},
			path: params?.path || [],
			schemaErrorMap: this._def.errorMap,
			parent: null,
			data,
			parsedType: getParsedType(data)
		};
		return handleResult(ctx, this._parseSync({
			data,
			path: ctx.path,
			parent: ctx
		}));
	}
	"~validate"(data) {
		const ctx = {
			common: {
				issues: [],
				async: !!this["~standard"].async
			},
			path: [],
			schemaErrorMap: this._def.errorMap,
			parent: null,
			data,
			parsedType: getParsedType(data)
		};
		if (!this["~standard"].async) try {
			const result = this._parseSync({
				data,
				path: [],
				parent: ctx
			});
			return isValid(result) ? { value: result.value } : { issues: ctx.common.issues };
		} catch (err) {
			if (err?.message?.toLowerCase()?.includes("encountered")) this["~standard"].async = true;
			ctx.common = {
				issues: [],
				async: true
			};
		}
		return this._parseAsync({
			data,
			path: [],
			parent: ctx
		}).then((result) => isValid(result) ? { value: result.value } : { issues: ctx.common.issues });
	}
	async parseAsync(data, params) {
		const result = await this.safeParseAsync(data, params);
		if (result.success) return result.data;
		throw result.error;
	}
	async safeParseAsync(data, params) {
		const ctx = {
			common: {
				issues: [],
				contextualErrorMap: params?.errorMap,
				async: true
			},
			path: params?.path || [],
			schemaErrorMap: this._def.errorMap,
			parent: null,
			data,
			parsedType: getParsedType(data)
		};
		const maybeAsyncResult = this._parse({
			data,
			path: ctx.path,
			parent: ctx
		});
		return handleResult(ctx, await (isAsync(maybeAsyncResult) ? maybeAsyncResult : Promise.resolve(maybeAsyncResult)));
	}
	refine(check, message) {
		const getIssueProperties = (val) => {
			if (typeof message === "string" || typeof message === "undefined") return { message };
			else if (typeof message === "function") return message(val);
			else return message;
		};
		return this._refinement((val, ctx) => {
			const result = check(val);
			const setError = () => ctx.addIssue({
				code: ZodIssueCode.custom,
				...getIssueProperties(val)
			});
			if (typeof Promise !== "undefined" && result instanceof Promise) return result.then((data) => {
				if (!data) {
					setError();
					return false;
				} else return true;
			});
			if (!result) {
				setError();
				return false;
			} else return true;
		});
	}
	refinement(check, refinementData) {
		return this._refinement((val, ctx) => {
			if (!check(val)) {
				ctx.addIssue(typeof refinementData === "function" ? refinementData(val, ctx) : refinementData);
				return false;
			} else return true;
		});
	}
	_refinement(refinement) {
		return new ZodEffects({
			schema: this,
			typeName: ZodFirstPartyTypeKind.ZodEffects,
			effect: {
				type: "refinement",
				refinement
			}
		});
	}
	superRefine(refinement) {
		return this._refinement(refinement);
	}
	constructor(def) {
		/** Alias of safeParseAsync */
		this.spa = this.safeParseAsync;
		this._def = def;
		this.parse = this.parse.bind(this);
		this.safeParse = this.safeParse.bind(this);
		this.parseAsync = this.parseAsync.bind(this);
		this.safeParseAsync = this.safeParseAsync.bind(this);
		this.spa = this.spa.bind(this);
		this.refine = this.refine.bind(this);
		this.refinement = this.refinement.bind(this);
		this.superRefine = this.superRefine.bind(this);
		this.optional = this.optional.bind(this);
		this.nullable = this.nullable.bind(this);
		this.nullish = this.nullish.bind(this);
		this.array = this.array.bind(this);
		this.promise = this.promise.bind(this);
		this.or = this.or.bind(this);
		this.and = this.and.bind(this);
		this.transform = this.transform.bind(this);
		this.brand = this.brand.bind(this);
		this.default = this.default.bind(this);
		this.catch = this.catch.bind(this);
		this.describe = this.describe.bind(this);
		this.pipe = this.pipe.bind(this);
		this.readonly = this.readonly.bind(this);
		this.isNullable = this.isNullable.bind(this);
		this.isOptional = this.isOptional.bind(this);
		this["~standard"] = {
			version: 1,
			vendor: "zod",
			validate: (data) => this["~validate"](data)
		};
	}
	optional() {
		return ZodOptional.create(this, this._def);
	}
	nullable() {
		return ZodNullable.create(this, this._def);
	}
	nullish() {
		return this.nullable().optional();
	}
	array() {
		return ZodArray.create(this);
	}
	promise() {
		return ZodPromise.create(this, this._def);
	}
	or(option) {
		return ZodUnion.create([this, option], this._def);
	}
	and(incoming) {
		return ZodIntersection.create(this, incoming, this._def);
	}
	transform(transform) {
		return new ZodEffects({
			...processCreateParams(this._def),
			schema: this,
			typeName: ZodFirstPartyTypeKind.ZodEffects,
			effect: {
				type: "transform",
				transform
			}
		});
	}
	default(def) {
		const defaultValueFunc = typeof def === "function" ? def : () => def;
		return new ZodDefault({
			...processCreateParams(this._def),
			innerType: this,
			defaultValue: defaultValueFunc,
			typeName: ZodFirstPartyTypeKind.ZodDefault
		});
	}
	brand() {
		return new ZodBranded({
			typeName: ZodFirstPartyTypeKind.ZodBranded,
			type: this,
			...processCreateParams(this._def)
		});
	}
	catch(def) {
		const catchValueFunc = typeof def === "function" ? def : () => def;
		return new ZodCatch({
			...processCreateParams(this._def),
			innerType: this,
			catchValue: catchValueFunc,
			typeName: ZodFirstPartyTypeKind.ZodCatch
		});
	}
	describe(description) {
		const This = this.constructor;
		return new This({
			...this._def,
			description
		});
	}
	pipe(target) {
		return ZodPipeline.create(this, target);
	}
	readonly() {
		return ZodReadonly.create(this);
	}
	isOptional() {
		return this.safeParse(void 0).success;
	}
	isNullable() {
		return this.safeParse(null).success;
	}
};
var cuidRegex = /^c[^\s-]{8,}$/i;
var cuid2Regex = /^[0-9a-z]+$/;
var ulidRegex = /^[0-9A-HJKMNP-TV-Z]{26}$/i;
var uuidRegex = /^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$/i;
var nanoidRegex = /^[a-z0-9_-]{21}$/i;
var jwtRegex = /^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]*$/;
var durationRegex = /^[-+]?P(?!$)(?:(?:[-+]?\d+Y)|(?:[-+]?\d+[.,]\d+Y$))?(?:(?:[-+]?\d+M)|(?:[-+]?\d+[.,]\d+M$))?(?:(?:[-+]?\d+W)|(?:[-+]?\d+[.,]\d+W$))?(?:(?:[-+]?\d+D)|(?:[-+]?\d+[.,]\d+D$))?(?:T(?=[\d+-])(?:(?:[-+]?\d+H)|(?:[-+]?\d+[.,]\d+H$))?(?:(?:[-+]?\d+M)|(?:[-+]?\d+[.,]\d+M$))?(?:[-+]?\d+(?:[.,]\d+)?S)?)??$/;
var emailRegex = /^(?!\.)(?!.*\.\.)([A-Z0-9_'+\-\.]*)[A-Z0-9_+-]@([A-Z0-9][A-Z0-9\-]*\.)+[A-Z]{2,}$/i;
var _emojiRegex = `^(\\p{Extended_Pictographic}|\\p{Emoji_Component})+$`;
var emojiRegex;
var ipv4Regex = /^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])$/;
var ipv4CidrRegex = /^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\/(3[0-2]|[12]?[0-9])$/;
var ipv6Regex = /^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$/;
var ipv6CidrRegex = /^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))\/(12[0-8]|1[01][0-9]|[1-9]?[0-9])$/;
var base64Regex = /^([0-9a-zA-Z+/]{4})*(([0-9a-zA-Z+/]{2}==)|([0-9a-zA-Z+/]{3}=))?$/;
var base64urlRegex = /^([0-9a-zA-Z-_]{4})*(([0-9a-zA-Z-_]{2}(==)?)|([0-9a-zA-Z-_]{3}(=)?))?$/;
var dateRegexSource = `((\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-((0[13578]|1[02])-(0[1-9]|[12]\\d|3[01])|(0[469]|11)-(0[1-9]|[12]\\d|30)|(02)-(0[1-9]|1\\d|2[0-8])))`;
var dateRegex = new RegExp(`^${dateRegexSource}$`);
function timeRegexSource(args) {
	let secondsRegexSource = `[0-5]\\d`;
	if (args.precision) secondsRegexSource = `${secondsRegexSource}\\.\\d{${args.precision}}`;
	else if (args.precision == null) secondsRegexSource = `${secondsRegexSource}(\\.\\d+)?`;
	const secondsQuantifier = args.precision ? "+" : "?";
	return `([01]\\d|2[0-3]):[0-5]\\d(:${secondsRegexSource})${secondsQuantifier}`;
}
function timeRegex(args) {
	return new RegExp(`^${timeRegexSource(args)}$`);
}
function datetimeRegex(args) {
	let regex = `${dateRegexSource}T${timeRegexSource(args)}`;
	const opts = [];
	opts.push(args.local ? `Z?` : `Z`);
	if (args.offset) opts.push(`([+-]\\d{2}:?\\d{2})`);
	regex = `${regex}(${opts.join("|")})`;
	return new RegExp(`^${regex}$`);
}
function isValidIP(ip, version) {
	if ((version === "v4" || !version) && ipv4Regex.test(ip)) return true;
	if ((version === "v6" || !version) && ipv6Regex.test(ip)) return true;
	return false;
}
function isValidJWT(jwt, alg) {
	if (!jwtRegex.test(jwt)) return false;
	try {
		const [header] = jwt.split(".");
		if (!header) return false;
		const base64 = header.replace(/-/g, "+").replace(/_/g, "/").padEnd(header.length + (4 - header.length % 4) % 4, "=");
		const decoded = JSON.parse(atob(base64));
		if (typeof decoded !== "object" || decoded === null) return false;
		if ("typ" in decoded && decoded?.typ !== "JWT") return false;
		if (!decoded.alg) return false;
		if (alg && decoded.alg !== alg) return false;
		return true;
	} catch {
		return false;
	}
}
function isValidCidr(ip, version) {
	if ((version === "v4" || !version) && ipv4CidrRegex.test(ip)) return true;
	if ((version === "v6" || !version) && ipv6CidrRegex.test(ip)) return true;
	return false;
}
var ZodString = class ZodString extends ZodType {
	_parse(input) {
		if (this._def.coerce) input.data = String(input.data);
		if (this._getType(input) !== ZodParsedType.string) {
			const ctx = this._getOrReturnCtx(input);
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.string,
				received: ctx.parsedType
			});
			return INVALID;
		}
		const status = new ParseStatus();
		let ctx = void 0;
		for (const check of this._def.checks) if (check.kind === "min") {
			if (input.data.length < check.value) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_small,
					minimum: check.value,
					type: "string",
					inclusive: true,
					exact: false,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "max") {
			if (input.data.length > check.value) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_big,
					maximum: check.value,
					type: "string",
					inclusive: true,
					exact: false,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "length") {
			const tooBig = input.data.length > check.value;
			const tooSmall = input.data.length < check.value;
			if (tooBig || tooSmall) {
				ctx = this._getOrReturnCtx(input, ctx);
				if (tooBig) addIssueToContext(ctx, {
					code: ZodIssueCode.too_big,
					maximum: check.value,
					type: "string",
					inclusive: true,
					exact: true,
					message: check.message
				});
				else if (tooSmall) addIssueToContext(ctx, {
					code: ZodIssueCode.too_small,
					minimum: check.value,
					type: "string",
					inclusive: true,
					exact: true,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "email") {
			if (!emailRegex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "email",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "emoji") {
			if (!emojiRegex) emojiRegex = new RegExp(_emojiRegex, "u");
			if (!emojiRegex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "emoji",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "uuid") {
			if (!uuidRegex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "uuid",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "nanoid") {
			if (!nanoidRegex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "nanoid",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "cuid") {
			if (!cuidRegex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "cuid",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "cuid2") {
			if (!cuid2Regex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "cuid2",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "ulid") {
			if (!ulidRegex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "ulid",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "url") try {
			new URL(input.data);
		} catch {
			ctx = this._getOrReturnCtx(input, ctx);
			addIssueToContext(ctx, {
				validation: "url",
				code: ZodIssueCode.invalid_string,
				message: check.message
			});
			status.dirty();
		}
		else if (check.kind === "regex") {
			check.regex.lastIndex = 0;
			if (!check.regex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "regex",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "trim") input.data = input.data.trim();
		else if (check.kind === "includes") {
			if (!input.data.includes(check.value, check.position)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.invalid_string,
					validation: {
						includes: check.value,
						position: check.position
					},
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "toLowerCase") input.data = input.data.toLowerCase();
		else if (check.kind === "toUpperCase") input.data = input.data.toUpperCase();
		else if (check.kind === "startsWith") {
			if (!input.data.startsWith(check.value)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.invalid_string,
					validation: { startsWith: check.value },
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "endsWith") {
			if (!input.data.endsWith(check.value)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.invalid_string,
					validation: { endsWith: check.value },
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "datetime") {
			if (!datetimeRegex(check).test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.invalid_string,
					validation: "datetime",
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "date") {
			if (!dateRegex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.invalid_string,
					validation: "date",
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "time") {
			if (!timeRegex(check).test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.invalid_string,
					validation: "time",
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "duration") {
			if (!durationRegex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "duration",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "ip") {
			if (!isValidIP(input.data, check.version)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "ip",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "jwt") {
			if (!isValidJWT(input.data, check.alg)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "jwt",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "cidr") {
			if (!isValidCidr(input.data, check.version)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "cidr",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "base64") {
			if (!base64Regex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "base64",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "base64url") {
			if (!base64urlRegex.test(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					validation: "base64url",
					code: ZodIssueCode.invalid_string,
					message: check.message
				});
				status.dirty();
			}
		} else util.assertNever(check);
		return {
			status: status.value,
			value: input.data
		};
	}
	_regex(regex, validation, message) {
		return this.refinement((data) => regex.test(data), {
			validation,
			code: ZodIssueCode.invalid_string,
			...errorUtil.errToObj(message)
		});
	}
	_addCheck(check) {
		return new ZodString({
			...this._def,
			checks: [...this._def.checks, check]
		});
	}
	email(message) {
		return this._addCheck({
			kind: "email",
			...errorUtil.errToObj(message)
		});
	}
	url(message) {
		return this._addCheck({
			kind: "url",
			...errorUtil.errToObj(message)
		});
	}
	emoji(message) {
		return this._addCheck({
			kind: "emoji",
			...errorUtil.errToObj(message)
		});
	}
	uuid(message) {
		return this._addCheck({
			kind: "uuid",
			...errorUtil.errToObj(message)
		});
	}
	nanoid(message) {
		return this._addCheck({
			kind: "nanoid",
			...errorUtil.errToObj(message)
		});
	}
	cuid(message) {
		return this._addCheck({
			kind: "cuid",
			...errorUtil.errToObj(message)
		});
	}
	cuid2(message) {
		return this._addCheck({
			kind: "cuid2",
			...errorUtil.errToObj(message)
		});
	}
	ulid(message) {
		return this._addCheck({
			kind: "ulid",
			...errorUtil.errToObj(message)
		});
	}
	base64(message) {
		return this._addCheck({
			kind: "base64",
			...errorUtil.errToObj(message)
		});
	}
	base64url(message) {
		return this._addCheck({
			kind: "base64url",
			...errorUtil.errToObj(message)
		});
	}
	jwt(options) {
		return this._addCheck({
			kind: "jwt",
			...errorUtil.errToObj(options)
		});
	}
	ip(options) {
		return this._addCheck({
			kind: "ip",
			...errorUtil.errToObj(options)
		});
	}
	cidr(options) {
		return this._addCheck({
			kind: "cidr",
			...errorUtil.errToObj(options)
		});
	}
	datetime(options) {
		if (typeof options === "string") return this._addCheck({
			kind: "datetime",
			precision: null,
			offset: false,
			local: false,
			message: options
		});
		return this._addCheck({
			kind: "datetime",
			precision: typeof options?.precision === "undefined" ? null : options?.precision,
			offset: options?.offset ?? false,
			local: options?.local ?? false,
			...errorUtil.errToObj(options?.message)
		});
	}
	date(message) {
		return this._addCheck({
			kind: "date",
			message
		});
	}
	time(options) {
		if (typeof options === "string") return this._addCheck({
			kind: "time",
			precision: null,
			message: options
		});
		return this._addCheck({
			kind: "time",
			precision: typeof options?.precision === "undefined" ? null : options?.precision,
			...errorUtil.errToObj(options?.message)
		});
	}
	duration(message) {
		return this._addCheck({
			kind: "duration",
			...errorUtil.errToObj(message)
		});
	}
	regex(regex, message) {
		return this._addCheck({
			kind: "regex",
			regex,
			...errorUtil.errToObj(message)
		});
	}
	includes(value, options) {
		return this._addCheck({
			kind: "includes",
			value,
			position: options?.position,
			...errorUtil.errToObj(options?.message)
		});
	}
	startsWith(value, message) {
		return this._addCheck({
			kind: "startsWith",
			value,
			...errorUtil.errToObj(message)
		});
	}
	endsWith(value, message) {
		return this._addCheck({
			kind: "endsWith",
			value,
			...errorUtil.errToObj(message)
		});
	}
	min(minLength, message) {
		return this._addCheck({
			kind: "min",
			value: minLength,
			...errorUtil.errToObj(message)
		});
	}
	max(maxLength, message) {
		return this._addCheck({
			kind: "max",
			value: maxLength,
			...errorUtil.errToObj(message)
		});
	}
	length(len, message) {
		return this._addCheck({
			kind: "length",
			value: len,
			...errorUtil.errToObj(message)
		});
	}
	/**
	* Equivalent to `.min(1)`
	*/
	nonempty(message) {
		return this.min(1, errorUtil.errToObj(message));
	}
	trim() {
		return new ZodString({
			...this._def,
			checks: [...this._def.checks, { kind: "trim" }]
		});
	}
	toLowerCase() {
		return new ZodString({
			...this._def,
			checks: [...this._def.checks, { kind: "toLowerCase" }]
		});
	}
	toUpperCase() {
		return new ZodString({
			...this._def,
			checks: [...this._def.checks, { kind: "toUpperCase" }]
		});
	}
	get isDatetime() {
		return !!this._def.checks.find((ch) => ch.kind === "datetime");
	}
	get isDate() {
		return !!this._def.checks.find((ch) => ch.kind === "date");
	}
	get isTime() {
		return !!this._def.checks.find((ch) => ch.kind === "time");
	}
	get isDuration() {
		return !!this._def.checks.find((ch) => ch.kind === "duration");
	}
	get isEmail() {
		return !!this._def.checks.find((ch) => ch.kind === "email");
	}
	get isURL() {
		return !!this._def.checks.find((ch) => ch.kind === "url");
	}
	get isEmoji() {
		return !!this._def.checks.find((ch) => ch.kind === "emoji");
	}
	get isUUID() {
		return !!this._def.checks.find((ch) => ch.kind === "uuid");
	}
	get isNANOID() {
		return !!this._def.checks.find((ch) => ch.kind === "nanoid");
	}
	get isCUID() {
		return !!this._def.checks.find((ch) => ch.kind === "cuid");
	}
	get isCUID2() {
		return !!this._def.checks.find((ch) => ch.kind === "cuid2");
	}
	get isULID() {
		return !!this._def.checks.find((ch) => ch.kind === "ulid");
	}
	get isIP() {
		return !!this._def.checks.find((ch) => ch.kind === "ip");
	}
	get isCIDR() {
		return !!this._def.checks.find((ch) => ch.kind === "cidr");
	}
	get isBase64() {
		return !!this._def.checks.find((ch) => ch.kind === "base64");
	}
	get isBase64url() {
		return !!this._def.checks.find((ch) => ch.kind === "base64url");
	}
	get minLength() {
		let min = null;
		for (const ch of this._def.checks) if (ch.kind === "min") {
			if (min === null || ch.value > min) min = ch.value;
		}
		return min;
	}
	get maxLength() {
		let max = null;
		for (const ch of this._def.checks) if (ch.kind === "max") {
			if (max === null || ch.value < max) max = ch.value;
		}
		return max;
	}
};
ZodString.create = (params) => {
	return new ZodString({
		checks: [],
		typeName: ZodFirstPartyTypeKind.ZodString,
		coerce: params?.coerce ?? false,
		...processCreateParams(params)
	});
};
function floatSafeRemainder(val, step) {
	const valDecCount = (val.toString().split(".")[1] || "").length;
	const stepDecCount = (step.toString().split(".")[1] || "").length;
	const decCount = valDecCount > stepDecCount ? valDecCount : stepDecCount;
	return Number.parseInt(val.toFixed(decCount).replace(".", "")) % Number.parseInt(step.toFixed(decCount).replace(".", "")) / 10 ** decCount;
}
var ZodNumber = class ZodNumber extends ZodType {
	constructor() {
		super(...arguments);
		this.min = this.gte;
		this.max = this.lte;
		this.step = this.multipleOf;
	}
	_parse(input) {
		if (this._def.coerce) input.data = Number(input.data);
		if (this._getType(input) !== ZodParsedType.number) {
			const ctx = this._getOrReturnCtx(input);
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.number,
				received: ctx.parsedType
			});
			return INVALID;
		}
		let ctx = void 0;
		const status = new ParseStatus();
		for (const check of this._def.checks) if (check.kind === "int") {
			if (!util.isInteger(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.invalid_type,
					expected: "integer",
					received: "float",
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "min") {
			if (check.inclusive ? input.data < check.value : input.data <= check.value) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_small,
					minimum: check.value,
					type: "number",
					inclusive: check.inclusive,
					exact: false,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "max") {
			if (check.inclusive ? input.data > check.value : input.data >= check.value) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_big,
					maximum: check.value,
					type: "number",
					inclusive: check.inclusive,
					exact: false,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "multipleOf") {
			if (floatSafeRemainder(input.data, check.value) !== 0) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.not_multiple_of,
					multipleOf: check.value,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "finite") {
			if (!Number.isFinite(input.data)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.not_finite,
					message: check.message
				});
				status.dirty();
			}
		} else util.assertNever(check);
		return {
			status: status.value,
			value: input.data
		};
	}
	gte(value, message) {
		return this.setLimit("min", value, true, errorUtil.toString(message));
	}
	gt(value, message) {
		return this.setLimit("min", value, false, errorUtil.toString(message));
	}
	lte(value, message) {
		return this.setLimit("max", value, true, errorUtil.toString(message));
	}
	lt(value, message) {
		return this.setLimit("max", value, false, errorUtil.toString(message));
	}
	setLimit(kind, value, inclusive, message) {
		return new ZodNumber({
			...this._def,
			checks: [...this._def.checks, {
				kind,
				value,
				inclusive,
				message: errorUtil.toString(message)
			}]
		});
	}
	_addCheck(check) {
		return new ZodNumber({
			...this._def,
			checks: [...this._def.checks, check]
		});
	}
	int(message) {
		return this._addCheck({
			kind: "int",
			message: errorUtil.toString(message)
		});
	}
	positive(message) {
		return this._addCheck({
			kind: "min",
			value: 0,
			inclusive: false,
			message: errorUtil.toString(message)
		});
	}
	negative(message) {
		return this._addCheck({
			kind: "max",
			value: 0,
			inclusive: false,
			message: errorUtil.toString(message)
		});
	}
	nonpositive(message) {
		return this._addCheck({
			kind: "max",
			value: 0,
			inclusive: true,
			message: errorUtil.toString(message)
		});
	}
	nonnegative(message) {
		return this._addCheck({
			kind: "min",
			value: 0,
			inclusive: true,
			message: errorUtil.toString(message)
		});
	}
	multipleOf(value, message) {
		return this._addCheck({
			kind: "multipleOf",
			value,
			message: errorUtil.toString(message)
		});
	}
	finite(message) {
		return this._addCheck({
			kind: "finite",
			message: errorUtil.toString(message)
		});
	}
	safe(message) {
		return this._addCheck({
			kind: "min",
			inclusive: true,
			value: Number.MIN_SAFE_INTEGER,
			message: errorUtil.toString(message)
		})._addCheck({
			kind: "max",
			inclusive: true,
			value: Number.MAX_SAFE_INTEGER,
			message: errorUtil.toString(message)
		});
	}
	get minValue() {
		let min = null;
		for (const ch of this._def.checks) if (ch.kind === "min") {
			if (min === null || ch.value > min) min = ch.value;
		}
		return min;
	}
	get maxValue() {
		let max = null;
		for (const ch of this._def.checks) if (ch.kind === "max") {
			if (max === null || ch.value < max) max = ch.value;
		}
		return max;
	}
	get isInt() {
		return !!this._def.checks.find((ch) => ch.kind === "int" || ch.kind === "multipleOf" && util.isInteger(ch.value));
	}
	get isFinite() {
		let max = null;
		let min = null;
		for (const ch of this._def.checks) if (ch.kind === "finite" || ch.kind === "int" || ch.kind === "multipleOf") return true;
		else if (ch.kind === "min") {
			if (min === null || ch.value > min) min = ch.value;
		} else if (ch.kind === "max") {
			if (max === null || ch.value < max) max = ch.value;
		}
		return Number.isFinite(min) && Number.isFinite(max);
	}
};
ZodNumber.create = (params) => {
	return new ZodNumber({
		checks: [],
		typeName: ZodFirstPartyTypeKind.ZodNumber,
		coerce: params?.coerce || false,
		...processCreateParams(params)
	});
};
var ZodBigInt = class ZodBigInt extends ZodType {
	constructor() {
		super(...arguments);
		this.min = this.gte;
		this.max = this.lte;
	}
	_parse(input) {
		if (this._def.coerce) try {
			input.data = BigInt(input.data);
		} catch {
			return this._getInvalidInput(input);
		}
		if (this._getType(input) !== ZodParsedType.bigint) return this._getInvalidInput(input);
		let ctx = void 0;
		const status = new ParseStatus();
		for (const check of this._def.checks) if (check.kind === "min") {
			if (check.inclusive ? input.data < check.value : input.data <= check.value) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_small,
					type: "bigint",
					minimum: check.value,
					inclusive: check.inclusive,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "max") {
			if (check.inclusive ? input.data > check.value : input.data >= check.value) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_big,
					type: "bigint",
					maximum: check.value,
					inclusive: check.inclusive,
					message: check.message
				});
				status.dirty();
			}
		} else if (check.kind === "multipleOf") {
			if (input.data % check.value !== BigInt(0)) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.not_multiple_of,
					multipleOf: check.value,
					message: check.message
				});
				status.dirty();
			}
		} else util.assertNever(check);
		return {
			status: status.value,
			value: input.data
		};
	}
	_getInvalidInput(input) {
		const ctx = this._getOrReturnCtx(input);
		addIssueToContext(ctx, {
			code: ZodIssueCode.invalid_type,
			expected: ZodParsedType.bigint,
			received: ctx.parsedType
		});
		return INVALID;
	}
	gte(value, message) {
		return this.setLimit("min", value, true, errorUtil.toString(message));
	}
	gt(value, message) {
		return this.setLimit("min", value, false, errorUtil.toString(message));
	}
	lte(value, message) {
		return this.setLimit("max", value, true, errorUtil.toString(message));
	}
	lt(value, message) {
		return this.setLimit("max", value, false, errorUtil.toString(message));
	}
	setLimit(kind, value, inclusive, message) {
		return new ZodBigInt({
			...this._def,
			checks: [...this._def.checks, {
				kind,
				value,
				inclusive,
				message: errorUtil.toString(message)
			}]
		});
	}
	_addCheck(check) {
		return new ZodBigInt({
			...this._def,
			checks: [...this._def.checks, check]
		});
	}
	positive(message) {
		return this._addCheck({
			kind: "min",
			value: BigInt(0),
			inclusive: false,
			message: errorUtil.toString(message)
		});
	}
	negative(message) {
		return this._addCheck({
			kind: "max",
			value: BigInt(0),
			inclusive: false,
			message: errorUtil.toString(message)
		});
	}
	nonpositive(message) {
		return this._addCheck({
			kind: "max",
			value: BigInt(0),
			inclusive: true,
			message: errorUtil.toString(message)
		});
	}
	nonnegative(message) {
		return this._addCheck({
			kind: "min",
			value: BigInt(0),
			inclusive: true,
			message: errorUtil.toString(message)
		});
	}
	multipleOf(value, message) {
		return this._addCheck({
			kind: "multipleOf",
			value,
			message: errorUtil.toString(message)
		});
	}
	get minValue() {
		let min = null;
		for (const ch of this._def.checks) if (ch.kind === "min") {
			if (min === null || ch.value > min) min = ch.value;
		}
		return min;
	}
	get maxValue() {
		let max = null;
		for (const ch of this._def.checks) if (ch.kind === "max") {
			if (max === null || ch.value < max) max = ch.value;
		}
		return max;
	}
};
ZodBigInt.create = (params) => {
	return new ZodBigInt({
		checks: [],
		typeName: ZodFirstPartyTypeKind.ZodBigInt,
		coerce: params?.coerce ?? false,
		...processCreateParams(params)
	});
};
var ZodBoolean = class extends ZodType {
	_parse(input) {
		if (this._def.coerce) input.data = Boolean(input.data);
		if (this._getType(input) !== ZodParsedType.boolean) {
			const ctx = this._getOrReturnCtx(input);
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.boolean,
				received: ctx.parsedType
			});
			return INVALID;
		}
		return OK(input.data);
	}
};
ZodBoolean.create = (params) => {
	return new ZodBoolean({
		typeName: ZodFirstPartyTypeKind.ZodBoolean,
		coerce: params?.coerce || false,
		...processCreateParams(params)
	});
};
var ZodDate = class ZodDate extends ZodType {
	_parse(input) {
		if (this._def.coerce) input.data = new Date(input.data);
		if (this._getType(input) !== ZodParsedType.date) {
			const ctx = this._getOrReturnCtx(input);
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.date,
				received: ctx.parsedType
			});
			return INVALID;
		}
		if (Number.isNaN(input.data.getTime())) {
			addIssueToContext(this._getOrReturnCtx(input), { code: ZodIssueCode.invalid_date });
			return INVALID;
		}
		const status = new ParseStatus();
		let ctx = void 0;
		for (const check of this._def.checks) if (check.kind === "min") {
			if (input.data.getTime() < check.value) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_small,
					message: check.message,
					inclusive: true,
					exact: false,
					minimum: check.value,
					type: "date"
				});
				status.dirty();
			}
		} else if (check.kind === "max") {
			if (input.data.getTime() > check.value) {
				ctx = this._getOrReturnCtx(input, ctx);
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_big,
					message: check.message,
					inclusive: true,
					exact: false,
					maximum: check.value,
					type: "date"
				});
				status.dirty();
			}
		} else util.assertNever(check);
		return {
			status: status.value,
			value: new Date(input.data.getTime())
		};
	}
	_addCheck(check) {
		return new ZodDate({
			...this._def,
			checks: [...this._def.checks, check]
		});
	}
	min(minDate, message) {
		return this._addCheck({
			kind: "min",
			value: minDate.getTime(),
			message: errorUtil.toString(message)
		});
	}
	max(maxDate, message) {
		return this._addCheck({
			kind: "max",
			value: maxDate.getTime(),
			message: errorUtil.toString(message)
		});
	}
	get minDate() {
		let min = null;
		for (const ch of this._def.checks) if (ch.kind === "min") {
			if (min === null || ch.value > min) min = ch.value;
		}
		return min != null ? new Date(min) : null;
	}
	get maxDate() {
		let max = null;
		for (const ch of this._def.checks) if (ch.kind === "max") {
			if (max === null || ch.value < max) max = ch.value;
		}
		return max != null ? new Date(max) : null;
	}
};
ZodDate.create = (params) => {
	return new ZodDate({
		checks: [],
		coerce: params?.coerce || false,
		typeName: ZodFirstPartyTypeKind.ZodDate,
		...processCreateParams(params)
	});
};
var ZodSymbol = class extends ZodType {
	_parse(input) {
		if (this._getType(input) !== ZodParsedType.symbol) {
			const ctx = this._getOrReturnCtx(input);
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.symbol,
				received: ctx.parsedType
			});
			return INVALID;
		}
		return OK(input.data);
	}
};
ZodSymbol.create = (params) => {
	return new ZodSymbol({
		typeName: ZodFirstPartyTypeKind.ZodSymbol,
		...processCreateParams(params)
	});
};
var ZodUndefined = class extends ZodType {
	_parse(input) {
		if (this._getType(input) !== ZodParsedType.undefined) {
			const ctx = this._getOrReturnCtx(input);
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.undefined,
				received: ctx.parsedType
			});
			return INVALID;
		}
		return OK(input.data);
	}
};
ZodUndefined.create = (params) => {
	return new ZodUndefined({
		typeName: ZodFirstPartyTypeKind.ZodUndefined,
		...processCreateParams(params)
	});
};
var ZodNull = class extends ZodType {
	_parse(input) {
		if (this._getType(input) !== ZodParsedType.null) {
			const ctx = this._getOrReturnCtx(input);
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.null,
				received: ctx.parsedType
			});
			return INVALID;
		}
		return OK(input.data);
	}
};
ZodNull.create = (params) => {
	return new ZodNull({
		typeName: ZodFirstPartyTypeKind.ZodNull,
		...processCreateParams(params)
	});
};
var ZodAny = class extends ZodType {
	constructor() {
		super(...arguments);
		this._any = true;
	}
	_parse(input) {
		return OK(input.data);
	}
};
ZodAny.create = (params) => {
	return new ZodAny({
		typeName: ZodFirstPartyTypeKind.ZodAny,
		...processCreateParams(params)
	});
};
var ZodUnknown = class extends ZodType {
	constructor() {
		super(...arguments);
		this._unknown = true;
	}
	_parse(input) {
		return OK(input.data);
	}
};
ZodUnknown.create = (params) => {
	return new ZodUnknown({
		typeName: ZodFirstPartyTypeKind.ZodUnknown,
		...processCreateParams(params)
	});
};
var ZodNever = class extends ZodType {
	_parse(input) {
		const ctx = this._getOrReturnCtx(input);
		addIssueToContext(ctx, {
			code: ZodIssueCode.invalid_type,
			expected: ZodParsedType.never,
			received: ctx.parsedType
		});
		return INVALID;
	}
};
ZodNever.create = (params) => {
	return new ZodNever({
		typeName: ZodFirstPartyTypeKind.ZodNever,
		...processCreateParams(params)
	});
};
var ZodVoid = class extends ZodType {
	_parse(input) {
		if (this._getType(input) !== ZodParsedType.undefined) {
			const ctx = this._getOrReturnCtx(input);
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.void,
				received: ctx.parsedType
			});
			return INVALID;
		}
		return OK(input.data);
	}
};
ZodVoid.create = (params) => {
	return new ZodVoid({
		typeName: ZodFirstPartyTypeKind.ZodVoid,
		...processCreateParams(params)
	});
};
var ZodArray = class ZodArray extends ZodType {
	_parse(input) {
		const { ctx, status } = this._processInputParams(input);
		const def = this._def;
		if (ctx.parsedType !== ZodParsedType.array) {
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.array,
				received: ctx.parsedType
			});
			return INVALID;
		}
		if (def.exactLength !== null) {
			const tooBig = ctx.data.length > def.exactLength.value;
			const tooSmall = ctx.data.length < def.exactLength.value;
			if (tooBig || tooSmall) {
				addIssueToContext(ctx, {
					code: tooBig ? ZodIssueCode.too_big : ZodIssueCode.too_small,
					minimum: tooSmall ? def.exactLength.value : void 0,
					maximum: tooBig ? def.exactLength.value : void 0,
					type: "array",
					inclusive: true,
					exact: true,
					message: def.exactLength.message
				});
				status.dirty();
			}
		}
		if (def.minLength !== null) {
			if (ctx.data.length < def.minLength.value) {
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_small,
					minimum: def.minLength.value,
					type: "array",
					inclusive: true,
					exact: false,
					message: def.minLength.message
				});
				status.dirty();
			}
		}
		if (def.maxLength !== null) {
			if (ctx.data.length > def.maxLength.value) {
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_big,
					maximum: def.maxLength.value,
					type: "array",
					inclusive: true,
					exact: false,
					message: def.maxLength.message
				});
				status.dirty();
			}
		}
		if (ctx.common.async) return Promise.all([...ctx.data].map((item, i) => {
			return def.type._parseAsync(new ParseInputLazyPath(ctx, item, ctx.path, i));
		})).then((result) => {
			return ParseStatus.mergeArray(status, result);
		});
		const result = [...ctx.data].map((item, i) => {
			return def.type._parseSync(new ParseInputLazyPath(ctx, item, ctx.path, i));
		});
		return ParseStatus.mergeArray(status, result);
	}
	get element() {
		return this._def.type;
	}
	min(minLength, message) {
		return new ZodArray({
			...this._def,
			minLength: {
				value: minLength,
				message: errorUtil.toString(message)
			}
		});
	}
	max(maxLength, message) {
		return new ZodArray({
			...this._def,
			maxLength: {
				value: maxLength,
				message: errorUtil.toString(message)
			}
		});
	}
	length(len, message) {
		return new ZodArray({
			...this._def,
			exactLength: {
				value: len,
				message: errorUtil.toString(message)
			}
		});
	}
	nonempty(message) {
		return this.min(1, message);
	}
};
ZodArray.create = (schema, params) => {
	return new ZodArray({
		type: schema,
		minLength: null,
		maxLength: null,
		exactLength: null,
		typeName: ZodFirstPartyTypeKind.ZodArray,
		...processCreateParams(params)
	});
};
function deepPartialify(schema) {
	if (schema instanceof ZodObject) {
		const newShape = {};
		for (const key in schema.shape) {
			const fieldSchema = schema.shape[key];
			newShape[key] = ZodOptional.create(deepPartialify(fieldSchema));
		}
		return new ZodObject({
			...schema._def,
			shape: () => newShape
		});
	} else if (schema instanceof ZodArray) return new ZodArray({
		...schema._def,
		type: deepPartialify(schema.element)
	});
	else if (schema instanceof ZodOptional) return ZodOptional.create(deepPartialify(schema.unwrap()));
	else if (schema instanceof ZodNullable) return ZodNullable.create(deepPartialify(schema.unwrap()));
	else if (schema instanceof ZodTuple) return ZodTuple.create(schema.items.map((item) => deepPartialify(item)));
	else return schema;
}
var ZodObject = class ZodObject extends ZodType {
	constructor() {
		super(...arguments);
		this._cached = null;
		/**
		* @deprecated In most cases, this is no longer needed - unknown properties are now silently stripped.
		* If you want to pass through unknown properties, use `.passthrough()` instead.
		*/
		this.nonstrict = this.passthrough;
		/**
		* @deprecated Use `.extend` instead
		*  */
		this.augment = this.extend;
	}
	_getCached() {
		if (this._cached !== null) return this._cached;
		const shape = this._def.shape();
		const keys = util.objectKeys(shape);
		this._cached = {
			shape,
			keys
		};
		return this._cached;
	}
	_parse(input) {
		if (this._getType(input) !== ZodParsedType.object) {
			const ctx = this._getOrReturnCtx(input);
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.object,
				received: ctx.parsedType
			});
			return INVALID;
		}
		const { status, ctx } = this._processInputParams(input);
		const { shape, keys: shapeKeys } = this._getCached();
		const extraKeys = [];
		if (!(this._def.catchall instanceof ZodNever && this._def.unknownKeys === "strip")) {
			for (const key in ctx.data) if (!shapeKeys.includes(key)) extraKeys.push(key);
		}
		const pairs = [];
		for (const key of shapeKeys) {
			const keyValidator = shape[key];
			const value = ctx.data[key];
			pairs.push({
				key: {
					status: "valid",
					value: key
				},
				value: keyValidator._parse(new ParseInputLazyPath(ctx, value, ctx.path, key)),
				alwaysSet: key in ctx.data
			});
		}
		if (this._def.catchall instanceof ZodNever) {
			const unknownKeys = this._def.unknownKeys;
			if (unknownKeys === "passthrough") for (const key of extraKeys) pairs.push({
				key: {
					status: "valid",
					value: key
				},
				value: {
					status: "valid",
					value: ctx.data[key]
				}
			});
			else if (unknownKeys === "strict") {
				if (extraKeys.length > 0) {
					addIssueToContext(ctx, {
						code: ZodIssueCode.unrecognized_keys,
						keys: extraKeys
					});
					status.dirty();
				}
			} else if (unknownKeys === "strip") {} else throw new Error(`Internal ZodObject error: invalid unknownKeys value.`);
		} else {
			const catchall = this._def.catchall;
			for (const key of extraKeys) {
				const value = ctx.data[key];
				pairs.push({
					key: {
						status: "valid",
						value: key
					},
					value: catchall._parse(new ParseInputLazyPath(ctx, value, ctx.path, key)),
					alwaysSet: key in ctx.data
				});
			}
		}
		if (ctx.common.async) return Promise.resolve().then(async () => {
			const syncPairs = [];
			for (const pair of pairs) {
				const key = await pair.key;
				const value = await pair.value;
				syncPairs.push({
					key,
					value,
					alwaysSet: pair.alwaysSet
				});
			}
			return syncPairs;
		}).then((syncPairs) => {
			return ParseStatus.mergeObjectSync(status, syncPairs);
		});
		else return ParseStatus.mergeObjectSync(status, pairs);
	}
	get shape() {
		return this._def.shape();
	}
	strict(message) {
		errorUtil.errToObj;
		return new ZodObject({
			...this._def,
			unknownKeys: "strict",
			...message !== void 0 ? { errorMap: (issue, ctx) => {
				const defaultError = this._def.errorMap?.(issue, ctx).message ?? ctx.defaultError;
				if (issue.code === "unrecognized_keys") return { message: errorUtil.errToObj(message).message ?? defaultError };
				return { message: defaultError };
			} } : {}
		});
	}
	strip() {
		return new ZodObject({
			...this._def,
			unknownKeys: "strip"
		});
	}
	passthrough() {
		return new ZodObject({
			...this._def,
			unknownKeys: "passthrough"
		});
	}
	extend(augmentation) {
		return new ZodObject({
			...this._def,
			shape: () => ({
				...this._def.shape(),
				...augmentation
			})
		});
	}
	/**
	* Prior to zod@1.0.12 there was a bug in the
	* inferred type of merged objects. Please
	* upgrade if you are experiencing issues.
	*/
	merge(merging) {
		return new ZodObject({
			unknownKeys: merging._def.unknownKeys,
			catchall: merging._def.catchall,
			shape: () => ({
				...this._def.shape(),
				...merging._def.shape()
			}),
			typeName: ZodFirstPartyTypeKind.ZodObject
		});
	}
	setKey(key, schema) {
		return this.augment({ [key]: schema });
	}
	catchall(index) {
		return new ZodObject({
			...this._def,
			catchall: index
		});
	}
	pick(mask) {
		const shape = {};
		for (const key of util.objectKeys(mask)) if (mask[key] && this.shape[key]) shape[key] = this.shape[key];
		return new ZodObject({
			...this._def,
			shape: () => shape
		});
	}
	omit(mask) {
		const shape = {};
		for (const key of util.objectKeys(this.shape)) if (!mask[key]) shape[key] = this.shape[key];
		return new ZodObject({
			...this._def,
			shape: () => shape
		});
	}
	/**
	* @deprecated
	*/
	deepPartial() {
		return deepPartialify(this);
	}
	partial(mask) {
		const newShape = {};
		for (const key of util.objectKeys(this.shape)) {
			const fieldSchema = this.shape[key];
			if (mask && !mask[key]) newShape[key] = fieldSchema;
			else newShape[key] = fieldSchema.optional();
		}
		return new ZodObject({
			...this._def,
			shape: () => newShape
		});
	}
	required(mask) {
		const newShape = {};
		for (const key of util.objectKeys(this.shape)) if (mask && !mask[key]) newShape[key] = this.shape[key];
		else {
			let newField = this.shape[key];
			while (newField instanceof ZodOptional) newField = newField._def.innerType;
			newShape[key] = newField;
		}
		return new ZodObject({
			...this._def,
			shape: () => newShape
		});
	}
	keyof() {
		return createZodEnum(util.objectKeys(this.shape));
	}
};
ZodObject.create = (shape, params) => {
	return new ZodObject({
		shape: () => shape,
		unknownKeys: "strip",
		catchall: ZodNever.create(),
		typeName: ZodFirstPartyTypeKind.ZodObject,
		...processCreateParams(params)
	});
};
ZodObject.strictCreate = (shape, params) => {
	return new ZodObject({
		shape: () => shape,
		unknownKeys: "strict",
		catchall: ZodNever.create(),
		typeName: ZodFirstPartyTypeKind.ZodObject,
		...processCreateParams(params)
	});
};
ZodObject.lazycreate = (shape, params) => {
	return new ZodObject({
		shape,
		unknownKeys: "strip",
		catchall: ZodNever.create(),
		typeName: ZodFirstPartyTypeKind.ZodObject,
		...processCreateParams(params)
	});
};
var ZodUnion = class extends ZodType {
	_parse(input) {
		const { ctx } = this._processInputParams(input);
		const options = this._def.options;
		function handleResults(results) {
			for (const result of results) if (result.result.status === "valid") return result.result;
			for (const result of results) if (result.result.status === "dirty") {
				ctx.common.issues.push(...result.ctx.common.issues);
				return result.result;
			}
			const unionErrors = results.map((result) => new ZodError(result.ctx.common.issues));
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_union,
				unionErrors
			});
			return INVALID;
		}
		if (ctx.common.async) return Promise.all(options.map(async (option) => {
			const childCtx = {
				...ctx,
				common: {
					...ctx.common,
					issues: []
				},
				parent: null
			};
			return {
				result: await option._parseAsync({
					data: ctx.data,
					path: ctx.path,
					parent: childCtx
				}),
				ctx: childCtx
			};
		})).then(handleResults);
		else {
			let dirty = void 0;
			const issues = [];
			for (const option of options) {
				const childCtx = {
					...ctx,
					common: {
						...ctx.common,
						issues: []
					},
					parent: null
				};
				const result = option._parseSync({
					data: ctx.data,
					path: ctx.path,
					parent: childCtx
				});
				if (result.status === "valid") return result;
				else if (result.status === "dirty" && !dirty) dirty = {
					result,
					ctx: childCtx
				};
				if (childCtx.common.issues.length) issues.push(childCtx.common.issues);
			}
			if (dirty) {
				ctx.common.issues.push(...dirty.ctx.common.issues);
				return dirty.result;
			}
			const unionErrors = issues.map((issues) => new ZodError(issues));
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_union,
				unionErrors
			});
			return INVALID;
		}
	}
	get options() {
		return this._def.options;
	}
};
ZodUnion.create = (types, params) => {
	return new ZodUnion({
		options: types,
		typeName: ZodFirstPartyTypeKind.ZodUnion,
		...processCreateParams(params)
	});
};
var getDiscriminator = (type) => {
	if (type instanceof ZodLazy) return getDiscriminator(type.schema);
	else if (type instanceof ZodEffects) return getDiscriminator(type.innerType());
	else if (type instanceof ZodLiteral) return [type.value];
	else if (type instanceof ZodEnum) return type.options;
	else if (type instanceof ZodNativeEnum) return util.objectValues(type.enum);
	else if (type instanceof ZodDefault) return getDiscriminator(type._def.innerType);
	else if (type instanceof ZodUndefined) return [void 0];
	else if (type instanceof ZodNull) return [null];
	else if (type instanceof ZodOptional) return [void 0, ...getDiscriminator(type.unwrap())];
	else if (type instanceof ZodNullable) return [null, ...getDiscriminator(type.unwrap())];
	else if (type instanceof ZodBranded) return getDiscriminator(type.unwrap());
	else if (type instanceof ZodReadonly) return getDiscriminator(type.unwrap());
	else if (type instanceof ZodCatch) return getDiscriminator(type._def.innerType);
	else return [];
};
var ZodDiscriminatedUnion = class ZodDiscriminatedUnion extends ZodType {
	_parse(input) {
		const { ctx } = this._processInputParams(input);
		if (ctx.parsedType !== ZodParsedType.object) {
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.object,
				received: ctx.parsedType
			});
			return INVALID;
		}
		const discriminator = this.discriminator;
		const discriminatorValue = ctx.data[discriminator];
		const option = this.optionsMap.get(discriminatorValue);
		if (!option) {
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_union_discriminator,
				options: Array.from(this.optionsMap.keys()),
				path: [discriminator]
			});
			return INVALID;
		}
		if (ctx.common.async) return option._parseAsync({
			data: ctx.data,
			path: ctx.path,
			parent: ctx
		});
		else return option._parseSync({
			data: ctx.data,
			path: ctx.path,
			parent: ctx
		});
	}
	get discriminator() {
		return this._def.discriminator;
	}
	get options() {
		return this._def.options;
	}
	get optionsMap() {
		return this._def.optionsMap;
	}
	/**
	* The constructor of the discriminated union schema. Its behaviour is very similar to that of the normal z.union() constructor.
	* However, it only allows a union of objects, all of which need to share a discriminator property. This property must
	* have a different value for each object in the union.
	* @param discriminator the name of the discriminator property
	* @param types an array of object schemas
	* @param params
	*/
	static create(discriminator, options, params) {
		const optionsMap = /* @__PURE__ */ new Map();
		for (const type of options) {
			const discriminatorValues = getDiscriminator(type.shape[discriminator]);
			if (!discriminatorValues.length) throw new Error(`A discriminator value for key \`${discriminator}\` could not be extracted from all schema options`);
			for (const value of discriminatorValues) {
				if (optionsMap.has(value)) throw new Error(`Discriminator property ${String(discriminator)} has duplicate value ${String(value)}`);
				optionsMap.set(value, type);
			}
		}
		return new ZodDiscriminatedUnion({
			typeName: ZodFirstPartyTypeKind.ZodDiscriminatedUnion,
			discriminator,
			options,
			optionsMap,
			...processCreateParams(params)
		});
	}
};
function mergeValues(a, b) {
	const aType = getParsedType(a);
	const bType = getParsedType(b);
	if (a === b) return {
		valid: true,
		data: a
	};
	else if (aType === ZodParsedType.object && bType === ZodParsedType.object) {
		const bKeys = util.objectKeys(b);
		const sharedKeys = util.objectKeys(a).filter((key) => bKeys.indexOf(key) !== -1);
		const newObj = {
			...a,
			...b
		};
		for (const key of sharedKeys) {
			const sharedValue = mergeValues(a[key], b[key]);
			if (!sharedValue.valid) return { valid: false };
			newObj[key] = sharedValue.data;
		}
		return {
			valid: true,
			data: newObj
		};
	} else if (aType === ZodParsedType.array && bType === ZodParsedType.array) {
		if (a.length !== b.length) return { valid: false };
		const newArray = [];
		for (let index = 0; index < a.length; index++) {
			const itemA = a[index];
			const itemB = b[index];
			const sharedValue = mergeValues(itemA, itemB);
			if (!sharedValue.valid) return { valid: false };
			newArray.push(sharedValue.data);
		}
		return {
			valid: true,
			data: newArray
		};
	} else if (aType === ZodParsedType.date && bType === ZodParsedType.date && +a === +b) return {
		valid: true,
		data: a
	};
	else return { valid: false };
}
var ZodIntersection = class extends ZodType {
	_parse(input) {
		const { status, ctx } = this._processInputParams(input);
		const handleParsed = (parsedLeft, parsedRight) => {
			if (isAborted(parsedLeft) || isAborted(parsedRight)) return INVALID;
			const merged = mergeValues(parsedLeft.value, parsedRight.value);
			if (!merged.valid) {
				addIssueToContext(ctx, { code: ZodIssueCode.invalid_intersection_types });
				return INVALID;
			}
			if (isDirty(parsedLeft) || isDirty(parsedRight)) status.dirty();
			return {
				status: status.value,
				value: merged.data
			};
		};
		if (ctx.common.async) return Promise.all([this._def.left._parseAsync({
			data: ctx.data,
			path: ctx.path,
			parent: ctx
		}), this._def.right._parseAsync({
			data: ctx.data,
			path: ctx.path,
			parent: ctx
		})]).then(([left, right]) => handleParsed(left, right));
		else return handleParsed(this._def.left._parseSync({
			data: ctx.data,
			path: ctx.path,
			parent: ctx
		}), this._def.right._parseSync({
			data: ctx.data,
			path: ctx.path,
			parent: ctx
		}));
	}
};
ZodIntersection.create = (left, right, params) => {
	return new ZodIntersection({
		left,
		right,
		typeName: ZodFirstPartyTypeKind.ZodIntersection,
		...processCreateParams(params)
	});
};
var ZodTuple = class ZodTuple extends ZodType {
	_parse(input) {
		const { status, ctx } = this._processInputParams(input);
		if (ctx.parsedType !== ZodParsedType.array) {
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.array,
				received: ctx.parsedType
			});
			return INVALID;
		}
		if (ctx.data.length < this._def.items.length) {
			addIssueToContext(ctx, {
				code: ZodIssueCode.too_small,
				minimum: this._def.items.length,
				inclusive: true,
				exact: false,
				type: "array"
			});
			return INVALID;
		}
		if (!this._def.rest && ctx.data.length > this._def.items.length) {
			addIssueToContext(ctx, {
				code: ZodIssueCode.too_big,
				maximum: this._def.items.length,
				inclusive: true,
				exact: false,
				type: "array"
			});
			status.dirty();
		}
		const items = [...ctx.data].map((item, itemIndex) => {
			const schema = this._def.items[itemIndex] || this._def.rest;
			if (!schema) return null;
			return schema._parse(new ParseInputLazyPath(ctx, item, ctx.path, itemIndex));
		}).filter((x) => !!x);
		if (ctx.common.async) return Promise.all(items).then((results) => {
			return ParseStatus.mergeArray(status, results);
		});
		else return ParseStatus.mergeArray(status, items);
	}
	get items() {
		return this._def.items;
	}
	rest(rest) {
		return new ZodTuple({
			...this._def,
			rest
		});
	}
};
ZodTuple.create = (schemas, params) => {
	if (!Array.isArray(schemas)) throw new Error("You must pass an array of schemas to z.tuple([ ... ])");
	return new ZodTuple({
		items: schemas,
		typeName: ZodFirstPartyTypeKind.ZodTuple,
		rest: null,
		...processCreateParams(params)
	});
};
var ZodRecord = class ZodRecord extends ZodType {
	get keySchema() {
		return this._def.keyType;
	}
	get valueSchema() {
		return this._def.valueType;
	}
	_parse(input) {
		const { status, ctx } = this._processInputParams(input);
		if (ctx.parsedType !== ZodParsedType.object) {
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.object,
				received: ctx.parsedType
			});
			return INVALID;
		}
		const pairs = [];
		const keyType = this._def.keyType;
		const valueType = this._def.valueType;
		for (const key in ctx.data) pairs.push({
			key: keyType._parse(new ParseInputLazyPath(ctx, key, ctx.path, key)),
			value: valueType._parse(new ParseInputLazyPath(ctx, ctx.data[key], ctx.path, key)),
			alwaysSet: key in ctx.data
		});
		if (ctx.common.async) return ParseStatus.mergeObjectAsync(status, pairs);
		else return ParseStatus.mergeObjectSync(status, pairs);
	}
	get element() {
		return this._def.valueType;
	}
	static create(first, second, third) {
		if (second instanceof ZodType) return new ZodRecord({
			keyType: first,
			valueType: second,
			typeName: ZodFirstPartyTypeKind.ZodRecord,
			...processCreateParams(third)
		});
		return new ZodRecord({
			keyType: ZodString.create(),
			valueType: first,
			typeName: ZodFirstPartyTypeKind.ZodRecord,
			...processCreateParams(second)
		});
	}
};
var ZodMap = class extends ZodType {
	get keySchema() {
		return this._def.keyType;
	}
	get valueSchema() {
		return this._def.valueType;
	}
	_parse(input) {
		const { status, ctx } = this._processInputParams(input);
		if (ctx.parsedType !== ZodParsedType.map) {
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.map,
				received: ctx.parsedType
			});
			return INVALID;
		}
		const keyType = this._def.keyType;
		const valueType = this._def.valueType;
		const pairs = [...ctx.data.entries()].map(([key, value], index) => {
			return {
				key: keyType._parse(new ParseInputLazyPath(ctx, key, ctx.path, [index, "key"])),
				value: valueType._parse(new ParseInputLazyPath(ctx, value, ctx.path, [index, "value"]))
			};
		});
		if (ctx.common.async) {
			const finalMap = /* @__PURE__ */ new Map();
			return Promise.resolve().then(async () => {
				for (const pair of pairs) {
					const key = await pair.key;
					const value = await pair.value;
					if (key.status === "aborted" || value.status === "aborted") return INVALID;
					if (key.status === "dirty" || value.status === "dirty") status.dirty();
					finalMap.set(key.value, value.value);
				}
				return {
					status: status.value,
					value: finalMap
				};
			});
		} else {
			const finalMap = /* @__PURE__ */ new Map();
			for (const pair of pairs) {
				const key = pair.key;
				const value = pair.value;
				if (key.status === "aborted" || value.status === "aborted") return INVALID;
				if (key.status === "dirty" || value.status === "dirty") status.dirty();
				finalMap.set(key.value, value.value);
			}
			return {
				status: status.value,
				value: finalMap
			};
		}
	}
};
ZodMap.create = (keyType, valueType, params) => {
	return new ZodMap({
		valueType,
		keyType,
		typeName: ZodFirstPartyTypeKind.ZodMap,
		...processCreateParams(params)
	});
};
var ZodSet = class ZodSet extends ZodType {
	_parse(input) {
		const { status, ctx } = this._processInputParams(input);
		if (ctx.parsedType !== ZodParsedType.set) {
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.set,
				received: ctx.parsedType
			});
			return INVALID;
		}
		const def = this._def;
		if (def.minSize !== null) {
			if (ctx.data.size < def.minSize.value) {
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_small,
					minimum: def.minSize.value,
					type: "set",
					inclusive: true,
					exact: false,
					message: def.minSize.message
				});
				status.dirty();
			}
		}
		if (def.maxSize !== null) {
			if (ctx.data.size > def.maxSize.value) {
				addIssueToContext(ctx, {
					code: ZodIssueCode.too_big,
					maximum: def.maxSize.value,
					type: "set",
					inclusive: true,
					exact: false,
					message: def.maxSize.message
				});
				status.dirty();
			}
		}
		const valueType = this._def.valueType;
		function finalizeSet(elements) {
			const parsedSet = /* @__PURE__ */ new Set();
			for (const element of elements) {
				if (element.status === "aborted") return INVALID;
				if (element.status === "dirty") status.dirty();
				parsedSet.add(element.value);
			}
			return {
				status: status.value,
				value: parsedSet
			};
		}
		const elements = [...ctx.data.values()].map((item, i) => valueType._parse(new ParseInputLazyPath(ctx, item, ctx.path, i)));
		if (ctx.common.async) return Promise.all(elements).then((elements) => finalizeSet(elements));
		else return finalizeSet(elements);
	}
	min(minSize, message) {
		return new ZodSet({
			...this._def,
			minSize: {
				value: minSize,
				message: errorUtil.toString(message)
			}
		});
	}
	max(maxSize, message) {
		return new ZodSet({
			...this._def,
			maxSize: {
				value: maxSize,
				message: errorUtil.toString(message)
			}
		});
	}
	size(size, message) {
		return this.min(size, message).max(size, message);
	}
	nonempty(message) {
		return this.min(1, message);
	}
};
ZodSet.create = (valueType, params) => {
	return new ZodSet({
		valueType,
		minSize: null,
		maxSize: null,
		typeName: ZodFirstPartyTypeKind.ZodSet,
		...processCreateParams(params)
	});
};
var ZodFunction = class ZodFunction extends ZodType {
	constructor() {
		super(...arguments);
		this.validate = this.implement;
	}
	_parse(input) {
		const { ctx } = this._processInputParams(input);
		if (ctx.parsedType !== ZodParsedType.function) {
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.function,
				received: ctx.parsedType
			});
			return INVALID;
		}
		function makeArgsIssue(args, error) {
			return makeIssue({
				data: args,
				path: ctx.path,
				errorMaps: [
					ctx.common.contextualErrorMap,
					ctx.schemaErrorMap,
					getErrorMap(),
					errorMap
				].filter((x) => !!x),
				issueData: {
					code: ZodIssueCode.invalid_arguments,
					argumentsError: error
				}
			});
		}
		function makeReturnsIssue(returns, error) {
			return makeIssue({
				data: returns,
				path: ctx.path,
				errorMaps: [
					ctx.common.contextualErrorMap,
					ctx.schemaErrorMap,
					getErrorMap(),
					errorMap
				].filter((x) => !!x),
				issueData: {
					code: ZodIssueCode.invalid_return_type,
					returnTypeError: error
				}
			});
		}
		const params = { errorMap: ctx.common.contextualErrorMap };
		const fn = ctx.data;
		if (this._def.returns instanceof ZodPromise) {
			const me = this;
			return OK(async function(...args) {
				const error = new ZodError([]);
				const parsedArgs = await me._def.args.parseAsync(args, params).catch((e) => {
					error.addIssue(makeArgsIssue(args, e));
					throw error;
				});
				const result = await Reflect.apply(fn, this, parsedArgs);
				return await me._def.returns._def.type.parseAsync(result, params).catch((e) => {
					error.addIssue(makeReturnsIssue(result, e));
					throw error;
				});
			});
		} else {
			const me = this;
			return OK(function(...args) {
				const parsedArgs = me._def.args.safeParse(args, params);
				if (!parsedArgs.success) throw new ZodError([makeArgsIssue(args, parsedArgs.error)]);
				const result = Reflect.apply(fn, this, parsedArgs.data);
				const parsedReturns = me._def.returns.safeParse(result, params);
				if (!parsedReturns.success) throw new ZodError([makeReturnsIssue(result, parsedReturns.error)]);
				return parsedReturns.data;
			});
		}
	}
	parameters() {
		return this._def.args;
	}
	returnType() {
		return this._def.returns;
	}
	args(...items) {
		return new ZodFunction({
			...this._def,
			args: ZodTuple.create(items).rest(ZodUnknown.create())
		});
	}
	returns(returnType) {
		return new ZodFunction({
			...this._def,
			returns: returnType
		});
	}
	implement(func) {
		return this.parse(func);
	}
	strictImplement(func) {
		return this.parse(func);
	}
	static create(args, returns, params) {
		return new ZodFunction({
			args: args ? args : ZodTuple.create([]).rest(ZodUnknown.create()),
			returns: returns || ZodUnknown.create(),
			typeName: ZodFirstPartyTypeKind.ZodFunction,
			...processCreateParams(params)
		});
	}
};
var ZodLazy = class extends ZodType {
	get schema() {
		return this._def.getter();
	}
	_parse(input) {
		const { ctx } = this._processInputParams(input);
		return this._def.getter()._parse({
			data: ctx.data,
			path: ctx.path,
			parent: ctx
		});
	}
};
ZodLazy.create = (getter, params) => {
	return new ZodLazy({
		getter,
		typeName: ZodFirstPartyTypeKind.ZodLazy,
		...processCreateParams(params)
	});
};
var ZodLiteral = class extends ZodType {
	_parse(input) {
		if (input.data !== this._def.value) {
			const ctx = this._getOrReturnCtx(input);
			addIssueToContext(ctx, {
				received: ctx.data,
				code: ZodIssueCode.invalid_literal,
				expected: this._def.value
			});
			return INVALID;
		}
		return {
			status: "valid",
			value: input.data
		};
	}
	get value() {
		return this._def.value;
	}
};
ZodLiteral.create = (value, params) => {
	return new ZodLiteral({
		value,
		typeName: ZodFirstPartyTypeKind.ZodLiteral,
		...processCreateParams(params)
	});
};
function createZodEnum(values, params) {
	return new ZodEnum({
		values,
		typeName: ZodFirstPartyTypeKind.ZodEnum,
		...processCreateParams(params)
	});
}
var ZodEnum = class ZodEnum extends ZodType {
	_parse(input) {
		if (typeof input.data !== "string") {
			const ctx = this._getOrReturnCtx(input);
			const expectedValues = this._def.values;
			addIssueToContext(ctx, {
				expected: util.joinValues(expectedValues),
				received: ctx.parsedType,
				code: ZodIssueCode.invalid_type
			});
			return INVALID;
		}
		if (!this._cache) this._cache = new Set(this._def.values);
		if (!this._cache.has(input.data)) {
			const ctx = this._getOrReturnCtx(input);
			const expectedValues = this._def.values;
			addIssueToContext(ctx, {
				received: ctx.data,
				code: ZodIssueCode.invalid_enum_value,
				options: expectedValues
			});
			return INVALID;
		}
		return OK(input.data);
	}
	get options() {
		return this._def.values;
	}
	get enum() {
		const enumValues = {};
		for (const val of this._def.values) enumValues[val] = val;
		return enumValues;
	}
	get Values() {
		const enumValues = {};
		for (const val of this._def.values) enumValues[val] = val;
		return enumValues;
	}
	get Enum() {
		const enumValues = {};
		for (const val of this._def.values) enumValues[val] = val;
		return enumValues;
	}
	extract(values, newDef = this._def) {
		return ZodEnum.create(values, {
			...this._def,
			...newDef
		});
	}
	exclude(values, newDef = this._def) {
		return ZodEnum.create(this.options.filter((opt) => !values.includes(opt)), {
			...this._def,
			...newDef
		});
	}
};
ZodEnum.create = createZodEnum;
var ZodNativeEnum = class extends ZodType {
	_parse(input) {
		const nativeEnumValues = util.getValidEnumValues(this._def.values);
		const ctx = this._getOrReturnCtx(input);
		if (ctx.parsedType !== ZodParsedType.string && ctx.parsedType !== ZodParsedType.number) {
			const expectedValues = util.objectValues(nativeEnumValues);
			addIssueToContext(ctx, {
				expected: util.joinValues(expectedValues),
				received: ctx.parsedType,
				code: ZodIssueCode.invalid_type
			});
			return INVALID;
		}
		if (!this._cache) this._cache = new Set(util.getValidEnumValues(this._def.values));
		if (!this._cache.has(input.data)) {
			const expectedValues = util.objectValues(nativeEnumValues);
			addIssueToContext(ctx, {
				received: ctx.data,
				code: ZodIssueCode.invalid_enum_value,
				options: expectedValues
			});
			return INVALID;
		}
		return OK(input.data);
	}
	get enum() {
		return this._def.values;
	}
};
ZodNativeEnum.create = (values, params) => {
	return new ZodNativeEnum({
		values,
		typeName: ZodFirstPartyTypeKind.ZodNativeEnum,
		...processCreateParams(params)
	});
};
var ZodPromise = class extends ZodType {
	unwrap() {
		return this._def.type;
	}
	_parse(input) {
		const { ctx } = this._processInputParams(input);
		if (ctx.parsedType !== ZodParsedType.promise && ctx.common.async === false) {
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.promise,
				received: ctx.parsedType
			});
			return INVALID;
		}
		return OK((ctx.parsedType === ZodParsedType.promise ? ctx.data : Promise.resolve(ctx.data)).then((data) => {
			return this._def.type.parseAsync(data, {
				path: ctx.path,
				errorMap: ctx.common.contextualErrorMap
			});
		}));
	}
};
ZodPromise.create = (schema, params) => {
	return new ZodPromise({
		type: schema,
		typeName: ZodFirstPartyTypeKind.ZodPromise,
		...processCreateParams(params)
	});
};
var ZodEffects = class extends ZodType {
	innerType() {
		return this._def.schema;
	}
	sourceType() {
		return this._def.schema._def.typeName === ZodFirstPartyTypeKind.ZodEffects ? this._def.schema.sourceType() : this._def.schema;
	}
	_parse(input) {
		const { status, ctx } = this._processInputParams(input);
		const effect = this._def.effect || null;
		const checkCtx = {
			addIssue: (arg) => {
				addIssueToContext(ctx, arg);
				if (arg.fatal) status.abort();
				else status.dirty();
			},
			get path() {
				return ctx.path;
			}
		};
		checkCtx.addIssue = checkCtx.addIssue.bind(checkCtx);
		if (effect.type === "preprocess") {
			const processed = effect.transform(ctx.data, checkCtx);
			if (ctx.common.async) return Promise.resolve(processed).then(async (processed) => {
				if (status.value === "aborted") return INVALID;
				const result = await this._def.schema._parseAsync({
					data: processed,
					path: ctx.path,
					parent: ctx
				});
				if (result.status === "aborted") return INVALID;
				if (result.status === "dirty") return DIRTY(result.value);
				if (status.value === "dirty") return DIRTY(result.value);
				return result;
			});
			else {
				if (status.value === "aborted") return INVALID;
				const result = this._def.schema._parseSync({
					data: processed,
					path: ctx.path,
					parent: ctx
				});
				if (result.status === "aborted") return INVALID;
				if (result.status === "dirty") return DIRTY(result.value);
				if (status.value === "dirty") return DIRTY(result.value);
				return result;
			}
		}
		if (effect.type === "refinement") {
			const executeRefinement = (acc) => {
				const result = effect.refinement(acc, checkCtx);
				if (ctx.common.async) return Promise.resolve(result);
				if (result instanceof Promise) throw new Error("Async refinement encountered during synchronous parse operation. Use .parseAsync instead.");
				return acc;
			};
			if (ctx.common.async === false) {
				const inner = this._def.schema._parseSync({
					data: ctx.data,
					path: ctx.path,
					parent: ctx
				});
				if (inner.status === "aborted") return INVALID;
				if (inner.status === "dirty") status.dirty();
				executeRefinement(inner.value);
				return {
					status: status.value,
					value: inner.value
				};
			} else return this._def.schema._parseAsync({
				data: ctx.data,
				path: ctx.path,
				parent: ctx
			}).then((inner) => {
				if (inner.status === "aborted") return INVALID;
				if (inner.status === "dirty") status.dirty();
				return executeRefinement(inner.value).then(() => {
					return {
						status: status.value,
						value: inner.value
					};
				});
			});
		}
		if (effect.type === "transform") if (ctx.common.async === false) {
			const base = this._def.schema._parseSync({
				data: ctx.data,
				path: ctx.path,
				parent: ctx
			});
			if (!isValid(base)) return INVALID;
			const result = effect.transform(base.value, checkCtx);
			if (result instanceof Promise) throw new Error(`Asynchronous transform encountered during synchronous parse operation. Use .parseAsync instead.`);
			return {
				status: status.value,
				value: result
			};
		} else return this._def.schema._parseAsync({
			data: ctx.data,
			path: ctx.path,
			parent: ctx
		}).then((base) => {
			if (!isValid(base)) return INVALID;
			return Promise.resolve(effect.transform(base.value, checkCtx)).then((result) => ({
				status: status.value,
				value: result
			}));
		});
		util.assertNever(effect);
	}
};
ZodEffects.create = (schema, effect, params) => {
	return new ZodEffects({
		schema,
		typeName: ZodFirstPartyTypeKind.ZodEffects,
		effect,
		...processCreateParams(params)
	});
};
ZodEffects.createWithPreprocess = (preprocess, schema, params) => {
	return new ZodEffects({
		schema,
		effect: {
			type: "preprocess",
			transform: preprocess
		},
		typeName: ZodFirstPartyTypeKind.ZodEffects,
		...processCreateParams(params)
	});
};
var ZodOptional = class extends ZodType {
	_parse(input) {
		if (this._getType(input) === ZodParsedType.undefined) return OK(void 0);
		return this._def.innerType._parse(input);
	}
	unwrap() {
		return this._def.innerType;
	}
};
ZodOptional.create = (type, params) => {
	return new ZodOptional({
		innerType: type,
		typeName: ZodFirstPartyTypeKind.ZodOptional,
		...processCreateParams(params)
	});
};
var ZodNullable = class extends ZodType {
	_parse(input) {
		if (this._getType(input) === ZodParsedType.null) return OK(null);
		return this._def.innerType._parse(input);
	}
	unwrap() {
		return this._def.innerType;
	}
};
ZodNullable.create = (type, params) => {
	return new ZodNullable({
		innerType: type,
		typeName: ZodFirstPartyTypeKind.ZodNullable,
		...processCreateParams(params)
	});
};
var ZodDefault = class extends ZodType {
	_parse(input) {
		const { ctx } = this._processInputParams(input);
		let data = ctx.data;
		if (ctx.parsedType === ZodParsedType.undefined) data = this._def.defaultValue();
		return this._def.innerType._parse({
			data,
			path: ctx.path,
			parent: ctx
		});
	}
	removeDefault() {
		return this._def.innerType;
	}
};
ZodDefault.create = (type, params) => {
	return new ZodDefault({
		innerType: type,
		typeName: ZodFirstPartyTypeKind.ZodDefault,
		defaultValue: typeof params.default === "function" ? params.default : () => params.default,
		...processCreateParams(params)
	});
};
var ZodCatch = class extends ZodType {
	_parse(input) {
		const { ctx } = this._processInputParams(input);
		const newCtx = {
			...ctx,
			common: {
				...ctx.common,
				issues: []
			}
		};
		const result = this._def.innerType._parse({
			data: newCtx.data,
			path: newCtx.path,
			parent: { ...newCtx }
		});
		if (isAsync(result)) return result.then((result) => {
			return {
				status: "valid",
				value: result.status === "valid" ? result.value : this._def.catchValue({
					get error() {
						return new ZodError(newCtx.common.issues);
					},
					input: newCtx.data
				})
			};
		});
		else return {
			status: "valid",
			value: result.status === "valid" ? result.value : this._def.catchValue({
				get error() {
					return new ZodError(newCtx.common.issues);
				},
				input: newCtx.data
			})
		};
	}
	removeCatch() {
		return this._def.innerType;
	}
};
ZodCatch.create = (type, params) => {
	return new ZodCatch({
		innerType: type,
		typeName: ZodFirstPartyTypeKind.ZodCatch,
		catchValue: typeof params.catch === "function" ? params.catch : () => params.catch,
		...processCreateParams(params)
	});
};
var ZodNaN = class extends ZodType {
	_parse(input) {
		if (this._getType(input) !== ZodParsedType.nan) {
			const ctx = this._getOrReturnCtx(input);
			addIssueToContext(ctx, {
				code: ZodIssueCode.invalid_type,
				expected: ZodParsedType.nan,
				received: ctx.parsedType
			});
			return INVALID;
		}
		return {
			status: "valid",
			value: input.data
		};
	}
};
ZodNaN.create = (params) => {
	return new ZodNaN({
		typeName: ZodFirstPartyTypeKind.ZodNaN,
		...processCreateParams(params)
	});
};
var ZodBranded = class extends ZodType {
	_parse(input) {
		const { ctx } = this._processInputParams(input);
		const data = ctx.data;
		return this._def.type._parse({
			data,
			path: ctx.path,
			parent: ctx
		});
	}
	unwrap() {
		return this._def.type;
	}
};
var ZodPipeline = class ZodPipeline extends ZodType {
	_parse(input) {
		const { status, ctx } = this._processInputParams(input);
		if (ctx.common.async) {
			const handleAsync = async () => {
				const inResult = await this._def.in._parseAsync({
					data: ctx.data,
					path: ctx.path,
					parent: ctx
				});
				if (inResult.status === "aborted") return INVALID;
				if (inResult.status === "dirty") {
					status.dirty();
					return DIRTY(inResult.value);
				} else return this._def.out._parseAsync({
					data: inResult.value,
					path: ctx.path,
					parent: ctx
				});
			};
			return handleAsync();
		} else {
			const inResult = this._def.in._parseSync({
				data: ctx.data,
				path: ctx.path,
				parent: ctx
			});
			if (inResult.status === "aborted") return INVALID;
			if (inResult.status === "dirty") {
				status.dirty();
				return {
					status: "dirty",
					value: inResult.value
				};
			} else return this._def.out._parseSync({
				data: inResult.value,
				path: ctx.path,
				parent: ctx
			});
		}
	}
	static create(a, b) {
		return new ZodPipeline({
			in: a,
			out: b,
			typeName: ZodFirstPartyTypeKind.ZodPipeline
		});
	}
};
var ZodReadonly = class extends ZodType {
	_parse(input) {
		const result = this._def.innerType._parse(input);
		const freeze = (data) => {
			if (isValid(data)) data.value = Object.freeze(data.value);
			return data;
		};
		return isAsync(result) ? result.then((data) => freeze(data)) : freeze(result);
	}
	unwrap() {
		return this._def.innerType;
	}
};
ZodReadonly.create = (type, params) => {
	return new ZodReadonly({
		innerType: type,
		typeName: ZodFirstPartyTypeKind.ZodReadonly,
		...processCreateParams(params)
	});
};
ZodObject.lazycreate;
var ZodFirstPartyTypeKind;
(function(ZodFirstPartyTypeKind) {
	ZodFirstPartyTypeKind["ZodString"] = "ZodString";
	ZodFirstPartyTypeKind["ZodNumber"] = "ZodNumber";
	ZodFirstPartyTypeKind["ZodNaN"] = "ZodNaN";
	ZodFirstPartyTypeKind["ZodBigInt"] = "ZodBigInt";
	ZodFirstPartyTypeKind["ZodBoolean"] = "ZodBoolean";
	ZodFirstPartyTypeKind["ZodDate"] = "ZodDate";
	ZodFirstPartyTypeKind["ZodSymbol"] = "ZodSymbol";
	ZodFirstPartyTypeKind["ZodUndefined"] = "ZodUndefined";
	ZodFirstPartyTypeKind["ZodNull"] = "ZodNull";
	ZodFirstPartyTypeKind["ZodAny"] = "ZodAny";
	ZodFirstPartyTypeKind["ZodUnknown"] = "ZodUnknown";
	ZodFirstPartyTypeKind["ZodNever"] = "ZodNever";
	ZodFirstPartyTypeKind["ZodVoid"] = "ZodVoid";
	ZodFirstPartyTypeKind["ZodArray"] = "ZodArray";
	ZodFirstPartyTypeKind["ZodObject"] = "ZodObject";
	ZodFirstPartyTypeKind["ZodUnion"] = "ZodUnion";
	ZodFirstPartyTypeKind["ZodDiscriminatedUnion"] = "ZodDiscriminatedUnion";
	ZodFirstPartyTypeKind["ZodIntersection"] = "ZodIntersection";
	ZodFirstPartyTypeKind["ZodTuple"] = "ZodTuple";
	ZodFirstPartyTypeKind["ZodRecord"] = "ZodRecord";
	ZodFirstPartyTypeKind["ZodMap"] = "ZodMap";
	ZodFirstPartyTypeKind["ZodSet"] = "ZodSet";
	ZodFirstPartyTypeKind["ZodFunction"] = "ZodFunction";
	ZodFirstPartyTypeKind["ZodLazy"] = "ZodLazy";
	ZodFirstPartyTypeKind["ZodLiteral"] = "ZodLiteral";
	ZodFirstPartyTypeKind["ZodEnum"] = "ZodEnum";
	ZodFirstPartyTypeKind["ZodEffects"] = "ZodEffects";
	ZodFirstPartyTypeKind["ZodNativeEnum"] = "ZodNativeEnum";
	ZodFirstPartyTypeKind["ZodOptional"] = "ZodOptional";
	ZodFirstPartyTypeKind["ZodNullable"] = "ZodNullable";
	ZodFirstPartyTypeKind["ZodDefault"] = "ZodDefault";
	ZodFirstPartyTypeKind["ZodCatch"] = "ZodCatch";
	ZodFirstPartyTypeKind["ZodPromise"] = "ZodPromise";
	ZodFirstPartyTypeKind["ZodBranded"] = "ZodBranded";
	ZodFirstPartyTypeKind["ZodPipeline"] = "ZodPipeline";
	ZodFirstPartyTypeKind["ZodReadonly"] = "ZodReadonly";
})(ZodFirstPartyTypeKind || (ZodFirstPartyTypeKind = {}));
var stringType = ZodString.create;
var numberType = ZodNumber.create;
ZodNaN.create;
ZodBigInt.create;
var booleanType = ZodBoolean.create;
ZodDate.create;
ZodSymbol.create;
ZodUndefined.create;
ZodNull.create;
ZodAny.create;
ZodUnknown.create;
ZodNever.create;
ZodVoid.create;
var arrayType = ZodArray.create;
var objectType = ZodObject.create;
ZodObject.strictCreate;
var unionType = ZodUnion.create;
var discriminatedUnionType = ZodDiscriminatedUnion.create;
ZodIntersection.create;
ZodTuple.create;
ZodRecord.create;
ZodMap.create;
ZodSet.create;
ZodFunction.create;
ZodLazy.create;
var literalType = ZodLiteral.create;
var enumType = ZodEnum.create;
ZodNativeEnum.create;
ZodPromise.create;
ZodEffects.create;
ZodOptional.create;
ZodNullable.create;
ZodEffects.createWithPreprocess;
ZodPipeline.create;
//#endregion
//#region lib/travel.ts
var priorities = {
	quiet: "Тишина и приватность",
	food: "Гастрономия",
	culture: "Культура",
	nature: "Природа",
	comfort: "Комфорт",
	activity: "Новые впечатления"
};
var inputSchema = objectType({
	formats: arrayType(enumType([
		"beach",
		"city",
		"multi",
		"safari",
		"cruise"
	])).max(5),
	destination: stringType().max(150),
	excluded: stringType().max(150),
	departure: stringType().min(2).max(100),
	date: stringType().max(10).refine((v) => !v || /^\d{4}-\d{2}-\d{2}$/.test(v) && !Number.isNaN(Date.parse(v)) && new Date(v).toISOString().slice(0, 10) === v, "Укажите существующую дату"),
	nights: numberType().int().min(2).max(30),
	adults: numberType().int().min(1).max(8),
	children: arrayType(numberType().int().min(0).max(17)).max(6),
	budget: numberType().int().min(5e4).max(1e8),
	priorities: arrayType(enumType([
		"quiet",
		"food",
		"culture",
		"nature",
		"comfort",
		"activity"
	])).max(3),
	pace: enumType([
		"calm",
		"balanced",
		"active"
	]),
	cabin: enumType(["economy", "business"]),
	directOnly: booleanType(),
	maxHours: numberType().min(0).max(30),
	special: stringType().max(1500),
	hotelChanges: numberType().int().min(0).max(10)
});
var journeySchema = objectType({
	id: stringType().min(3).max(100),
	title: stringType().trim().min(3).max(120),
	subtitle: stringType().max(200),
	country: stringType().min(2).max(200),
	destinations: arrayType(stringType().min(2).max(100)).min(1).max(15),
	format: enumType([
		"beach",
		"city",
		"multi",
		"safari",
		"cruise"
	]),
	tags: arrayType(enumType([
		"quiet",
		"food",
		"culture",
		"nature",
		"comfort",
		"activity"
	])).max(6),
	minNights: numberType().int().min(2).max(30),
	maxNights: numberType().int().min(2).max(30),
	baseNights: numberType().int().min(2).max(30),
	changes: numberType().int().min(0).max(10),
	needsReview: booleanType(),
	active: booleanType(),
	overview: stringType().min(20).max(2e3),
	tradeoff: stringType().min(10).max(1e3),
	hotel: arrayType(objectType({
		name: stringType().min(2).max(200),
		location: stringType().min(2).max(200),
		room: stringType().min(3).max(500),
		description: stringType().min(10).max(1500),
		url: stringType().url().refine((s) => s.startsWith("https://"))
	})).min(1).max(10),
	program: arrayType(objectType({
		title: stringType().min(2).max(150),
		activity: stringType().min(5).max(1500),
		evening: stringType().min(2).max(1e3),
		sources: arrayType(objectType({
			title: stringType().max(100),
			url: stringType().url().refine((s) => s.startsWith("https://"))
		})).max(5).optional()
	})).min(1).max(30),
	includes: arrayType(stringType().min(2).max(500)).min(1).max(20),
	excludes: arrayType(stringType().min(2).max(500)).min(1).max(20),
	logistics: stringType().min(10).max(2e3),
	season: stringType().min(10).max(2e3),
	documents: stringType().min(10).max(2e3),
	perPersonFlight: numberType().int().nonnegative().max(1e7),
	roomNight: numberType().int().nonnegative().max(1e7),
	dailyMeals: numberType().int().nonnegative().max(1e6),
	groupLogistics: numberType().int().nonnegative().max(1e7),
	groupExperiences: numberType().int().nonnegative().max(1e7),
	priceKind: enumType(["demo", "estimate"]),
	priceSource: stringType().min(10).max(1500),
	updatedAt: stringType()
});
var money = (v) => new Intl.NumberFormat("ru-RU", {
	style: "currency",
	currency: "RUB",
	maximumFractionDigits: 0
}).format(v);
function estimate(j, input) {
	if (input.children.length) return null;
	const rooms = Math.ceil(input.adults / 2);
	const lines = [
		{
			name: "Перелёты на всех",
			amount: j.perPersonFlight * input.adults * (input.cabin === "business" ? 2.5 : 1)
		},
		{
			name: "Проживание · " + input.nights + " ночей · " + rooms + " номер(а)",
			amount: j.roomNight * input.nights * rooms
		},
		{
			name: "Питание вне включённого тарифа",
			amount: j.dailyMeals * input.nights * input.adults
		},
		{
			name: "Трансферы и перемещения",
			amount: j.groupLogistics * Math.ceil(input.adults / 4)
		},
		{
			name: "Программа и впечатления",
			amount: j.groupExperiences * Math.ceil(input.adults / 2)
		}
	].map((l) => ({
		...l,
		amount: Math.round(l.amount)
	}));
	const low = lines.reduce((sum, l) => sum + l.amount, 0);
	return {
		low,
		high: Math.ceil(low * 1.18 / 1e3) * 1e3,
		lines,
		kind: j.priceKind,
		source: j.priceSource
	};
}
function reviewReasons(input) {
	const r = [];
	if (input.children.length) r.push("Подтвердить размещение и стоимость для детей указанного возраста");
	if (input.directOnly) r.push("Подтвердить прямые рейсы на выбранные даты");
	if (input.maxHours > 0) r.push("Проверить ограничение дороги: не более " + input.maxHours + " часов");
	if (input.special.trim()) r.push("Проверить индивидуальные обязательные условия");
	return r;
}
var norm = (s) => s.toLowerCase().replaceAll("ё", "е").trim();
var tokens = (s) => norm(s).split(/[,;\n]+/).map((v) => v.trim()).filter(Boolean);
function rankJourneys(all, input) {
	return all.filter((j) => j.active).map((j) => {
		const haystack = norm([
			j.country,
			...j.destinations,
			j.title
		].join(" ")), issues = [];
		if (input.formats.length && !input.formats.includes(j.format)) issues.push("Другой формат путешествия");
		if (input.destination.trim() && !tokens(input.destination).some((t) => haystack.includes(t))) issues.push("Другое направление");
		if (tokens(input.excluded).some((t) => haystack.includes(t))) issues.push("Исключённое направление");
		if (input.nights < j.minNights || input.nights > j.maxNights) issues.push("Нужна другая продолжительность");
		if (j.changes > input.hotelChanges) issues.push("Слишком много смен размещения");
		const cost = estimate(j, input);
		if (cost && cost.high > input.budget) issues.push("Верхняя граница расчёта превышает бюджет");
		const matches = input.priorities.filter((p) => j.tags.includes(p));
		return {
			journey: j,
			estimate: cost,
			issues,
			matches,
			score: matches.length * 10 + (input.pace === "calm" && j.tags.includes("quiet") ? 6 : 0) + (input.pace === "active" && j.tags.includes("activity") ? 6 : 0) + (j.changes === 0 ? 1 : 0),
			review: [...reviewReasons(input), ...j.needsReview ? ["Проверить стыковки и последовательность этапов маршрута"] : []]
		};
	}).sort((a, b) => a.issues.length - b.issues.length || b.score - a.score || (a.estimate?.high ?? Infinity) - (b.estimate?.high ?? Infinity));
}
//#endregion
//#region lib/catalog.ts
var base = {
	active: true,
	priceKind: "demo",
	priceSource: "Условные суммы для проверки сервиса. Не являются тарифами поставщиков.",
	updatedAt: "2026-09-23",
	documents: "Документы и требования к въезду проверяются по гражданству, датам и маршруту до оформления.",
	includes: [
		"Размещение указанного уровня",
		"Перелёты на всех взрослых в расчёте",
		"Основные трансферы и программа"
	],
	excludes: ["Личные покупки и дополнительные услуги", "Страхование, визы и обязательные сборы — до отдельного расчёта"],
	dailyMeals: 7e3,
	groupLogistics: 8e4,
	groupExperiences: 6e4
};
var olhahali = {
	name: "Jumeirah Olhahali Island",
	location: "Мальдивы · остров Олхахали",
	room: "One Bedroom Beach Villa with Pool",
	description: "У этой категории есть собственный бассейн, отдельная гостиная, затенённая зона отдыха и услуги дворецкого.",
	url: "https://www.jumeirah.com/en/stay/maldives/jumeirah-olhahali-island/accommodation/beach-villa-with-pool"
};
var marsa = {
	name: "Jumeirah Marsa Al Arab",
	location: "Дубай · Джумейра",
	room: "Ocean Deluxe Suite",
	description: "Номера и люксы с просторными террасами. Вид на море, марину или город зависит от категории.",
	url: "https://www.jumeirah.com/en/stay/dubai/jumeirah-marsa-al-arab/accommodation/ocean-deluxe-suite"
};
var beachProgram = [
	{
		title: "Пляж и отдых",
		activity: "Свободное утро у моря или собственного бассейна.",
		evening: "Ужин в отеле; ресторан и питание согласуются отдельно."
	},
	{
		title: "Подводный мир",
		activity: "По желанию — водная прогулка или снорклинг с организатором.",
		evening: "Спокойный вечер на острове."
	},
	{
		title: "День без планов",
		activity: "Отдых, прогулка по острову, спа по предварительному запросу.",
		evening: "Ужин и время вдвоём."
	}
];
var seedJourneys = [
	{
		...base,
		id: "island-calm",
		title: "Неделя у лагуны",
		subtitle: "Тёплое море и время только для себя",
		country: "Мальдивы",
		destinations: ["Мале", "Олхахали"],
		format: "beach",
		tags: [
			"quiet",
			"comfort",
			"nature"
		],
		minNights: 4,
		maxNights: 14,
		baseNights: 7,
		changes: 0,
		needsReview: false,
		overview: "Одна островная база, пляжная вилла и свободный ритм. Впечатления можно добавлять по настроению, сохраняя время для моря и отдыха.",
		tradeoff: "Камерный островной формат: выбор занятий и ресторанов связан с инфраструктурой курорта.",
		hotel: [olhahali],
		program: beachProgram,
		logistics: "Перелёт до Мале и островной трансфер. Тип, время и стоимость трансфера подтверждаются под конкретный рейс.",
		season: "Условия моря и сезонные особенности уточняются под даты.",
		perPersonFlight: 11e4,
		roomNight: 8e4
	},
	{
		...base,
		id: "island-active",
		title: "Остров и океан",
		subtitle: "Подводный мир и новые впечатления",
		country: "Мальдивы",
		destinations: ["Мале", "Олхахали"],
		format: "beach",
		tags: [
			"activity",
			"nature",
			"comfort"
		],
		minNights: 5,
		maxNights: 12,
		baseNights: 7,
		changes: 0,
		needsReview: false,
		overview: "Более насыщенная островная неделя на той же базе: свободные дни чередуются с водными впечатлениями.",
		tradeoff: "Водные активности зависят от погоды и подтверждаются организатором.",
		hotel: [olhahali],
		program: beachProgram,
		logistics: "Международный перелёт и островной трансфер согласуются после выбора дат.",
		season: "Доступность активностей на воде проверяется отдельно.",
		perPersonFlight: 11e4,
		roomNight: 8e4,
		groupExperiences: 16e4
	},
	{
		...base,
		id: "dubai-city",
		title: "Дубай в вашем ритме",
		subtitle: "Архитектура, гастрономия и морской берег",
		country: "ОАЭ",
		destinations: ["Дубай"],
		format: "city",
		tags: [
			"food",
			"comfort",
			"culture"
		],
		minNights: 3,
		maxNights: 10,
		baseNights: 5,
		changes: 0,
		needsReview: false,
		overview: "Один отель на побережье и городская программа без смены места проживания. Дни для архитектуры, культуры и ресторанов.",
		tradeoff: "Городские впечатления потребуют поездок из отеля. Время в дороге зависит от выбранной программы.",
		hotel: [marsa],
		program: [
			{
				title: "Архитектура и город",
				activity: "Downtown Dubai и Burj Khalifa. Посещение смотровой площадки — после выбора билетов и времени.",
				evening: "Итальянский ресторан Rialto в Jumeirah Marsa Al Arab — по предварительному бронированию.",
				sources: [{
					title: "Burj Khalifa",
					url: "https://www.burjkhalifa.ae/"
				}, {
					title: "Rialto",
					url: "https://www.jumeirah.com/en/stay/dubai/jumeirah-marsa-al-arab/dining/marsa-al-arab-rialto"
				}]
			},
			{
				title: "Культура и история",
				activity: "Museum of the Future: экспозиции и архитектура музея. Доступность билетов проверяется под дату.",
				evening: "Свободный вечер у моря.",
				sources: [{
					title: "Museum of the Future",
					url: "https://museumofthefuture.ae/en"
				}]
			},
			{
				title: "Морской день",
				activity: "Отдых в отеле и на побережье.",
				evening: "Гастрономический вечер по желанию."
			}
		],
		logistics: "Перелёт в Дубай, трансфер и городские перемещения проверяются под даты.",
		season: "Время прогулок подбирается по сезону.",
		perPersonFlight: 65e3,
		roomNight: 65e3
	},
	{
		...base,
		id: "doha-city",
		title: "Три грани Дохи",
		subtitle: "Современная архитектура, культура и кухня",
		country: "Катар",
		destinations: ["Доха"],
		format: "city",
		tags: [
			"culture",
			"food",
			"activity"
		],
		minNights: 2,
		maxNights: 7,
		baseNights: 3,
		changes: 0,
		needsReview: false,
		overview: "Городская поездка с базой в Msheireb Downtown: время для архитектуры, музеев и гастрономии.",
		tradeoff: "Экскурсии и билеты требуют проверки на нужные даты.",
		hotel: [{
			name: "Mandarin Oriental, Doha",
			location: "Доха · Msheireb Downtown",
			room: "Panoramic Suite",
			description: "Расположение в Msheireb Downtown. В отеле есть рестораны IZU и Liang; среди размещений — семейные люксы и апартаменты.",
			url: "https://www.mandarinoriental.com/en/doha/msheireb/stay"
		}],
		program: [{
			title: "Знакомство с Дохой",
			activity: "Msheireb Downtown и Museum of Islamic Art. Посещение музея — после проверки часов работы.",
			evening: "Ресторан IZU в Mandarin Oriental, Doha — по предварительному бронированию.",
			sources: [{
				title: "Museum of Islamic Art",
				url: "https://mia.org.qa/en/"
			}, {
				title: "IZU",
				url: "https://www.mandarinoriental.com/en/doha/msheireb/dine/izu"
			}]
		}, {
			title: "Музеи и архитектура",
			activity: "National Museum of Qatar: архитектура здания и знакомство с историей страны. Билеты и время проверяются отдельно.",
			evening: "Свободное время в городе.",
			sources: [{
				title: "National Museum of Qatar",
				url: "https://nmoq.org.qa/en/"
			}]
		}],
		logistics: "Перелёт и трансфер в центр Дохи. Объединение с другим городом требует проверки стыковок.",
		season: "Программу на открытом воздухе корректируем по сезону.",
		perPersonFlight: 65e3,
		roomNight: 48e3,
		groupLogistics: 45e3,
		groupExperiences: 4e4
	},
	{
		...base,
		id: "dubai-alula",
		title: "От моря к пустыне",
		subtitle: "Дубай и Аль-Ула в одной поездке",
		country: "ОАЭ, Саудовская Аравия",
		destinations: ["Дубай", "Аль-Ула"],
		format: "multi",
		tags: [
			"culture",
			"nature",
			"comfort"
		],
		minNights: 6,
		maxNights: 12,
		baseNights: 8,
		changes: 1,
		needsReview: true,
		overview: "Сочетание морского отдыха в Дубае и пустынных ландшафтов Аль-Улы. Ночи и перелёты проверяются менеджером.",
		tradeoff: "Нужен перелёт между этапами; программа зависит от его расписания.",
		hotel: [marsa, {
			name: "Banyan Tree AlUla",
			location: "Аль-Ула · долина Ashar",
			room: "Dune One Bedroom Pool Villa",
			description: "Виллы палаточного типа среди пустынного пейзажа. Бассейн есть у отдельных категорий. Rock Pool доступен взрослым проживающим гостям.",
			url: "https://www.banyantree.com/saudi-arabia/alula/accommodation/dune-one-bedroom-pool-villa"
		}],
		program: [{
			title: "Море и город",
			activity: "Отдых на побережье и одна городская активность.",
			evening: "Ресторан и свободный вечер."
		}, {
			title: "Наследие и ландшафты",
			activity: "Программа по официальной коллекции Experience AlUla.",
			evening: "Ужин и отдых в долине."
		}],
		logistics: "Между Дубаем и Аль-Улой нужен согласованный авиационный этап. Менеджер проверяет последовательность и распределение ночей.",
		season: "Активности в пустыне выбираются под сезон.",
		perPersonFlight: 16e4,
		roomNight: 7e4,
		groupLogistics: 14e4,
		groupExperiences: 14e4
	},
	{
		...base,
		id: "tarangire",
		title: "Танзания: ближе к природе",
		subtitle: "Сафари и проживание среди баобабов",
		country: "Танзания",
		destinations: ["Аруша", "Тарангире"],
		format: "safari",
		tags: [
			"nature",
			"activity",
			"comfort"
		],
		minNights: 4,
		maxNights: 10,
		baseNights: 6,
		changes: 1,
		needsReview: true,
		overview: "Идея сафари с проживанием в Elewana Tarangire Treetops. Организатор, переезды и условия участия проверяются до показа предложения.",
		tradeoff: "Ранние выезды и время в автомобиле — часть сафари. Наблюдение конкретных животных не гарантируется.",
		hotel: [{
			name: "Elewana Tarangire Treetops",
			location: "Танзания · у границы Тарангире",
			room: "Treetops Room",
			description: "Двадцать приподнятых номеров с приватными балконами. Главное здание расположено вокруг старого баобаба.",
			url: "https://www.elewanacollection.com/tarangire-treetops/at-a-glance"
		}],
		program: [{
			title: "Сафари с проводником",
			activity: "Выезд по согласованной с организатором программе.",
			evening: "Возвращение в лодж и ужин."
		}, {
			title: "Природа и отдых",
			activity: "Наблюдения чередуются с отдыхом в лодже.",
			evening: "Спокойный вечер."
		}],
		logistics: "Перелёты, наземная логистика и сафари согласуются с принимающей компанией.",
		season: "Условия наблюдений зависят от периода; их проверяет организатор.",
		perPersonFlight: 13e4,
		roomNight: 9e4,
		groupLogistics: 16e4,
		groupExperiences: 12e4
	},
	{
		...base,
		id: "antarctica",
		title: "Антарктика: экспедиция",
		subtitle: "Южный океан с командой HX",
		country: "Аргентина, Антарктика",
		destinations: [
			"Буэнос-Айрес",
			"Ушуая",
			"Антарктика"
		],
		format: "cruise",
		tags: [
			"nature",
			"activity",
			"culture"
		],
		minNights: 10,
		maxNights: 20,
		baseNights: 13,
		changes: 2,
		needsReview: true,
		overview: "Экспедиционное путешествие с HX. Конкретное отправление, судно, каюта и береговые высадки подтверждаются отдельно.",
		tradeoff: "Морской переход и экспедиционная программа. Высадки зависят от погоды и льда.",
		hotel: [{
			name: "MS Fridtjof Nansen · HX",
			location: "Экспедиционный круиз",
			room: "ME — Suite with balcony",
			description: "Science Centre, экспедиционная команда и внешние каюты. Балконы и люксы есть в отдельных категориях.",
			url: "https://www.travelhx.com/en/ships/fridtjof-nansen/"
		}],
		program: [{
			title: "Экспедиционный день",
			activity: "Лекции, наблюдения и программа экспедиционной команды.",
			evening: "Подготовка к следующему дню."
		}, {
			title: "Исследование побережья",
			activity: "Высадки и активности только при решении команды и подходящих условиях.",
			evening: "Возвращение на судно."
		}],
		logistics: "Перелёты до Аргентины, этап до Ушуайи и круиз. Все даты проверяются вместе с запасами времени.",
		season: "Сезон и конкретное отправление определяются по программе организатора.",
		perPersonFlight: 22e4,
		roomNight: 145e3,
		groupLogistics: 13e4,
		groupExperiences: 4e4,
		dailyMeals: 4e3
	}
];
//#endregion
//#region server/mail.mjs
/**
* Tiare Travel transactional mail. Native Node only; this module never retries.
* Persist the generated payload and key in the outbox before sending it.
* Resend idempotency expires after 24 hours: an ambiguous attempt must not be
* retried automatically outside that window.
* https://resend.com/docs/api-reference/emails/send-email
* https://resend.com/docs/api-reference/emails/retrieve-email
* https://resend.com/docs/dashboard/emails/idempotency-keys
*/
var RESEND_TIMEOUT_MS = 8e3;
var API = "https://api.resend.com/emails";
var UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
var FORMAT = {
	beach: "Острова и море",
	city: "Города и культура",
	multi: "Несколько мест",
	safari: "Сафари и природа",
	cruise: "Круизы и экспедиции"
};
var PRIORITY = {
	quiet: "Тишина и приватность",
	food: "Гастрономия",
	culture: "Культура",
	nature: "Природа",
	comfort: "Комфорт",
	activity: "Новые впечатления"
};
var PACE = {
	calm: "Спокойный",
	balanced: "Сбалансированный",
	active: "Насыщенный"
};
var raw = (value, max = 3e3) => String(value ?? "").replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, "").slice(0, max).trim();
var oneLine = (value, max = 160) => raw(value, max).replace(/[\r\n]+/g, " ").replace(/\s+/g, " ");
var esc = (value) => raw(value).replace(/[&<>"']/g, (c) => ({
	"&": "&amp;",
	"<": "&lt;",
	">": "&gt;",
	"\"": "&quot;",
	"'": "&#39;"
})[c]);
var paragraphs = (value) => esc(value).replace(/\r?\n/g, "<br>");
var list = (value) => Array.isArray(value) ? value : [];
var currency = (value) => Number.isFinite(value) ? new Intl.NumberFormat("ru-RU", {
	style: "currency",
	currency: "RUB",
	maximumFractionDigits: 0
}).format(value) : "Не рассчитано";
var defined = (value, fallback = "Не указано") => raw(value) || fallback;
var dateOnlyValid = (value) => typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value)) && new Date(value).toISOString().slice(0, 10) === value;
var day = (value) => dateOnlyValid(value) ? new Intl.DateTimeFormat("ru-RU", {
	timeZone: "UTC",
	day: "numeric",
	month: "long",
	year: "numeric"
}).format(new Date(value)) : "Даты уточняются";
var instantValid = (value) => typeof value === "string" && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d{1,3})?)?(Z|[+-]\d{2}:\d{2})$/.test(value) && Number.isFinite(Date.parse(value));
var instant = (value) => instantValid(value) ? new Intl.DateTimeFormat("ru-RU", {
	timeZone: "Europe/Moscow",
	day: "numeric",
	month: "long",
	year: "numeric",
	hour: "2-digit",
	minute: "2-digit"
}).format(new Date(value)) + " МСК" : "Время не подтверждено";
var EmailProviderError = class extends Error {
	constructor(code, { status = 0, retryable = false, ambiguous = false, retryAfterSeconds = null, message = "Не удалось выполнить операцию с почтовым сервисом." } = {}) {
		super(message);
		this.name = "EmailProviderError";
		this.code = code;
		this.status = status;
		this.retryable = retryable;
		this.ambiguous = ambiguous;
		this.retryAfterSeconds = retryAfterSeconds;
	}
	toJSON() {
		return {
			name: this.name,
			code: this.code,
			message: this.message,
			status: this.status,
			retryable: this.retryable,
			ambiguous: this.ambiguous,
			retryAfterSeconds: this.retryAfterSeconds
		};
	}
};
function invalid(code = "EMAIL_INVALID_INPUT", message = "Проверьте настройки отправки письма.") {
	return new EmailProviderError(code, { message });
}
function safePublicUrl(value) {
	try {
		const url = new URL(value);
		if (url.protocol !== "https:" || url.username || url.password) return null;
		return url.toString();
	} catch {
		return null;
	}
}
function requestLink(appUrl, requestId, view) {
	if (!UUID.test(String(requestId))) throw invalid("EMAIL_INVALID_REQUEST", "Некорректный номер заявки.");
	let url;
	try {
		url = new URL(appUrl);
	} catch {
		throw invalid("EMAIL_INVALID_APP_URL", "Не задан корректный адрес сервиса.");
	}
	const local = [
		"localhost",
		"127.0.0.1",
		"[::1]",
		"terminal.local"
	].includes(url.hostname);
	if (url.protocol !== "https:" && !(url.protocol === "http:" && local) || url.username || url.password) throw invalid("EMAIL_INVALID_APP_URL", "Адрес сервиса должен использовать HTTPS.");
	url.pathname = "/";
	url.search = "";
	url.hash = "";
	url.searchParams.set("view", view);
	url.searchParams.set("request", requestId);
	return url.toString();
}
function factsTable(facts) {
	return "<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" style=\"border-collapse:collapse\">" + facts.map(([label, value]) => `<tr><td style="padding:9px 14px 9px 0;border-bottom:1px solid #e8e0d4;vertical-align:top;width:35%;color:#766d5e;font-size:14px">${esc(label)}</td><td style="padding:9px 0;border-bottom:1px solid #e8e0d4;vertical-align:top;font-size:15px">${paragraphs(value)}</td></tr>`).join("") + "</table>";
}
var factsText = (facts) => facts.map(([label, value]) => `${raw(label)}: ${raw(value)}`).join("\n");
function section(title, body) {
	return `<h2 style="font-family:Georgia,'Times New Roman',serif;font-size:23px;line-height:1.3;font-weight:normal;color:#263d32;margin:30px 0 14px">${esc(title)}</h2>${body}`;
}
function note(message) {
	return `<div style="background:#eee9dc;border-left:3px solid #8b8c65;padding:14px 16px;font-size:14px;line-height:1.6;margin:18px 0">${paragraphs(message)}</div>`;
}
function shell({ title, subtitle, body, url, cta, accessNote = "Для просмотра заявки войдите в кабинет менеджера. Ссылка не содержит данных для входа." }) {
	return `<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${esc(title)}</title></head><body style="margin:0;padding:0;background:#f3efe6;color:#263d32;font-family:Arial,Helvetica,sans-serif;font-size:16px;line-height:1.6"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f3efe6"><tr><td align="center" style="padding:28px 12px"><table role="presentation" width="640" cellpadding="0" cellspacing="0" style="width:100%;max-width:640px;background:#fffcf6;border:1px solid #e5ddcf;border-radius:14px"><tr><td style="padding:30px 30px 18px;border-bottom:1px solid #e5ddcf"><div style="letter-spacing:4px;font-family:Georgia,'Times New Roman',serif;font-size:23px">TIARE TRAVEL</div><div style="color:#81745f;font-size:12px;letter-spacing:2px;margin-top:6px">ВАШЕ ПУТЕШЕСТВИЕ В ДЕТАЛЯХ</div></td></tr><tr><td style="padding:24px 30px 32px;overflow-wrap:anywhere"><h1 style="font-family:Georgia,'Times New Roman',serif;font-size:32px;line-height:1.2;font-weight:normal;margin:0 0 14px">${esc(title)}</h1><p style="margin:0 0 20px;color:#706757">${paragraphs(subtitle)}</p>${body}<table role="presentation" cellpadding="0" cellspacing="0" style="margin-top:28px"><tr><td style="background:#2d4537;border-radius:7px"><a href="${esc(url)}" style="display:inline-block;padding:14px 22px;color:#ffffff;text-decoration:none;font-size:15px;font-weight:bold">${esc(cta)}</a></td></tr></table><p style="font-size:12px;color:#847965;margin:14px 0 0">${paragraphs(accessNote)}</p></td></tr><tr><td style="padding:20px 30px;border-top:1px solid #e5ddcf;color:#847965;font-size:12px">Tiare Travel · Индивидуальные путешествия<br>Письмо относится к вашей заявке. Бронирование выполняется менеджером после согласования.</td></tr></table></td></tr></table></body></html>`;
}
function requestFacts(request) {
	const i = request.input ?? {};
	return [
		["Клиент", defined(request.customerName)],
		["Почта", defined(request.customerEmail)],
		["Связь", defined(request.contact)],
		["Форматы", list(i.formats).map((x) => FORMAT[x] || raw(x)).join(", ") || "Открыт к предложениям"],
		["Направление", defined(i.destination, "Открыт к предложениям")],
		["Исключить", defined(i.excluded, "Нет исключений")],
		["Вылет", defined(i.departure)],
		["Начало поездки", day(i.date)],
		["Продолжительность", `${i.nights ?? "—"} ночей`],
		["Участники", `${i.adults ?? "—"} взрослых${list(i.children).length ? `; дети: ${list(i.children).join(", ")} лет` : "; без детей"}`],
		["Бюджет на всех", currency(i.budget)],
		["Класс перелёта", i.cabin === "business" ? "Бизнес" : "Эконом"],
		["Дорога", `${i.directOnly ? "Только прямые рейсы" : "Пересадки допустимы"}${Number(i.maxHours) > 0 ? `; не более ${i.maxHours} ч` : ""}`],
		["Приоритеты", list(i.priorities).map((x) => PRIORITY[x] || raw(x)).join(", ") || "Не выбраны"],
		["Ритм", PACE[i.pace] || "Не указан"],
		["Смена отелей", `Не более ${i.hotelChanges ?? "—"}`],
		["Обязательные условия", defined(i.special, "Нет дополнительных условий")],
		["Комментарий клиента", defined(request.notes, "Нет комментария")]
	];
}
function candidateParts(candidate, index) {
	const j = candidate?.journey ?? {};
	const e = candidate?.estimate;
	const cost = e && Number.isFinite(e.low) && Number.isFinite(e.high) && e.low >= 0 && e.high >= e.low ? `${currency(e.low)} — ${currency(e.high)} · ${e.kind === "estimate" ? "плановый ориентир, цена и наличие не подтверждены" : "ДЕМОНСТРАЦИОННЫЙ РАСЧЁТ, условные суммы"}` : "Требуется индивидуальный расчёт; цена и наличие не подтверждены";
	const rows = [
		["Направление", list(j.destinations).join(" → ") || defined(j.country)],
		["Отель и номер", list(j.hotel).map((h) => `${raw(h.name)} — ${raw(h.room)} (${raw(h.location)})`).join("\n") || "Требует подбора"],
		["Бюджет", cost],
		["Почему подходит", defined(candidate?.rationale, list(candidate?.matches).map((x) => PRIORITY[x] || raw(x)).join(", ") || "Проверьте соответствие пожеланиям клиента")],
		["Несоответствия", list(candidate?.issues).map((v) => raw(v)).join("\n") || "Не выявлены правилами подбора; требуется проверка менеджера"],
		["Что проверить", list(candidate?.review).map((v) => raw(v)).join("\n") || "Даты, размещение, транспорт, состав услуг и наличие"],
		["Особенности", defined(j.tradeoff)]
	];
	const heading = `${index + 1}. ${defined(j.title, "Идея для рассмотрения")}`;
	return {
		html: section(heading, factsTable(rows)),
		text: `${heading}\n${factsText(rows)}`
	};
}
/** Build a private, manager-facing brief. Candidate facts come only from input. */
function buildManagerRequestEmail({ request, candidates = [], appUrl }) {
	if (!request || typeof request !== "object") throw invalid("EMAIL_INVALID_REQUEST", "Не передана заявка.");
	const url = requestLink(appUrl, request.id, "agent");
	const facts = requestFacts(request);
	if (request.status === "agreed") {
		const title = "Клиент согласовал путешествие";
		const intro = "Заявка передана на оформление. Проверьте актуальность согласованных условий и выполните бронирование. При изменении стоимости или состава услуг получите новое согласование клиента.";
		const j = request.journey, q = currentQuote$2(request);
		const bookingFacts = [
			["Согласованная поездка", defined(j?.title, "Маршрут отсутствует — откройте заявку для проверки")],
			["Начало поездки", day(q?.travelDate || request.input?.date)],
			["Продолжительность", `${request.input?.nights ?? "—"} ночей`],
			["Отель и номер", list(j?.hotel).map((h) => `${raw(h.name)} — ${raw(h.room)} (${raw(h.location)})`).join("\n") || "Требует проверки"]
		];
		const quoteFacts = q ? [
			["Согласованная сумма на всех", currency(q.amount)],
			["Состав услуг", q.scope],
			["Наличие по результату проверки", q.availability],
			["Оплата и отмена", q.terms],
			["Проверено менеджером", instant(q.checkedAt)],
			["Предложение действует до", instant(q.validUntil)]
		] : [];
		const notice = q ? "Клиент согласовал предложение. Наличие проверено на указанное время; бронирование ещё необходимо выполнить." : "Клиент согласовал предложение, но подтверждение стоимости и наличия уже истекло или неполно. Повторно проверьте условия перед оформлением. В письме не приводится неактуальная сумма.";
		const comment = raw(request.managerNote);
		const body = section("К оформлению", factsTable(bookingFacts)) + (q ? section("Согласованные условия", factsTable(quoteFacts)) : "") + note(notice) + (comment ? section("Комментарий к предложению", `<p>${paragraphs(comment)}</p>`) : "") + section("Контакты и пожелания клиента", factsTable(facts));
		return {
			subject: `Tiare Travel · клиент согласовал · ${oneLine(request.id.slice(0, 8).toUpperCase())} · ${oneLine(request.customerName, 80) || "Клиент"}`,
			html: shell({
				title,
				subtitle: intro,
				body,
				url,
				cta: "Открыть заявку для оформления"
			}),
			text: `TIARE TRAVEL\n${title}\n\n${intro}\n\nК ОФОРМЛЕНИЮ\n${factsText(bookingFacts)}${q ? "\n\nСОГЛАСОВАННЫЕ УСЛОВИЯ\n" + factsText(quoteFacts) : ""}\n\n${notice}${comment ? "\n\nКомментарий к предложению\n" + comment : ""}\n\nКОНТАКТЫ И ПОЖЕЛАНИЯ КЛИЕНТА\n${factsText(facts)}\n\nОткрыть заявку для оформления (вход в кабинет менеджера): ${url}`
		};
	}
	const items = list(candidates).slice(0, 5).map(candidateParts);
	const title = "Новая заявка на путешествие";
	const intro = "Подготовлены варианты для вашего рассмотрения. Выберите подходящий, измените маршрут или составьте своё предложение. Клиент получит только опубликованную вами версию.";
	const noCandidates = "Готовых вариантов для этой заявки пока нет. Подготовьте индивидуальный маршрут с учётом обязательных условий клиента.";
	const body = section("Пожелания клиента", factsTable(facts)) + note("Внутреннее письмо агентству. Подборка ещё не является клиентским предложением. Демонстрационные суммы нельзя использовать как подтверждённую цену.") + (items.length ? items.map((x) => x.html).join("") : section("Индивидуальный подбор", `<p>${esc(noCandidates)}</p>`));
	return {
		subject: `Tiare Travel · заявка ${oneLine(request.id.slice(0, 8).toUpperCase())} · ${oneLine(request.customerName, 90) || "Новый клиент"}`,
		html: shell({
			title,
			subtitle: intro,
			body,
			url,
			cta: "Рассмотреть и подготовить предложение"
		}),
		text: `TIARE TRAVEL\n${title}\n\n${intro}\n\nВНУТРЕННЕЕ ПИСЬМО АГЕНТСТВУ. Демонстрационные суммы не подтверждают стоимость и наличие.\n\n${factsText(facts)}\n\n${items.length ? items.map((x) => x.text).join("\n\n") : noCandidates}\n\nОткрыть заявку (вход в сервис): ${url}`
	};
}
function currentQuote$2(request) {
	const q = request.quote, now = Date.now();
	if (!["proposal", "agreed"].includes(request.status) || !q || !Number.isFinite(q.amount) || q.amount <= 0 || !safePublicUrl(q.source) || !dateOnlyValid(q.travelDate) || !instantValid(q.checkedAt) || !instantValid(q.validUntil) || Date.parse(q.checkedAt) > now || Date.parse(q.validUntil) <= now || Date.parse(q.validUntil) <= Date.parse(q.checkedAt) || raw(q.scope).length < 15 || raw(q.availability).length < 10 || raw(q.terms).length < 10) return null;
	return q;
}
function publishedJourneyParts(j) {
	const rows = [
		["Маршрут", list(j.destinations).map((v) => raw(v)).join(" → ") || defined(j.country)],
		["Проживание", list(j.hotel).map((h) => `${raw(h.name)} · ${raw(h.room)}\n${raw(h.location)}. ${raw(h.description)}`).join("\n\n")],
		["Логистика", defined(j.logistics)],
		["Особенности", defined(j.tradeoff)],
		["Сезонность", defined(j.season)],
		["Документы", defined(j.documents)],
		["Предусмотрено программой", list(j.includes).map((v) => raw(v)).join("\n")],
		["Не включено / уточняется", list(j.excludes).map((v) => raw(v)).join("\n")]
	];
	const program = list(j.program).slice(0, 30).map((d, i) => `${i + 1}. ${raw(d.title)}\n${raw(d.activity)}\n${raw(d.evening)}`);
	return {
		html: section("Ваш маршрут", `<p>${paragraphs(j.overview)}</p>${factsTable(rows)}`) + (program.length ? section("Эскиз программы", program.map((p) => `<p style="margin:0 0 16px">${paragraphs(p)}</p>`).join("")) : ""),
		text: `${raw(j.overview)}\n\n${factsText(rows)}${program.length ? "\n\nЭСКИЗ ПРОГРАММЫ\n" + program.join("\n\n") : ""}`
	};
}
/**
* Only an explicitly published journey may be emailed to a client. Candidate
* lists, internalNote, supplier confirmation links and planning prices are not
* serialized. managerNote is the explicitly client-facing comment in our model.
*/
function buildClientProposalEmail({ request, appUrl }) {
	if (!request || ![
		"selection",
		"pricing",
		"proposal",
		"agreed"
	].includes(request.status) || !request.journey || typeof request.journey !== "object") throw invalid("EMAIL_NOT_PUBLISHED", "Сначала опубликуйте одобренное предложение для клиента.");
	const url = requestLink(appUrl, request.id, "requests");
	const j = request.journey, q = currentQuote$2(request), parts = publishedJourneyParts(j), i = request.input ?? {};
	const title = defined(j.title, "Ваше путешествие");
	const intro = `${defined(request.customerName, "Здравствуйте")}, менеджер подготовил для вас предложение. ${request.status === "agreed" ? "Вы согласовали его; следующий шаг — оформление менеджером." : "Посмотрите маршрут и обсудите необходимые изменения с менеджером."}`;
	const summary = [
		["Начало поездки", day(q?.travelDate || i.date)],
		["Продолжительность", `${i.nights ?? "—"} ночей`],
		["Участники", `${i.adults ?? "—"} взрослых${list(i.children).length ? `; детей: ${list(i.children).length}` : ""}`]
	];
	const quoteRows = q ? [
		["Сумма на всех", currency(q.amount)],
		["Состав услуг", q.scope],
		["Наличие по результату проверки", q.availability],
		["Оплата и отмена", q.terms],
		["Проверено менеджером", instant(q.checkedAt)],
		["Предложение действует до", instant(q.validUntil)]
	] : [];
	const quoteMessage = q ? "Цена и наличие проверены менеджером на указанное время. Наличие может измениться до оформления; письмо не подтверждает бронирование." : ["proposal", "agreed"].includes(request.status) ? "Сумма и наличие требуют повторного подтверждения. В письме не приводится просроченный или неполный расчёт. Обратитесь к менеджеру за актуальными условиями." : j.priceKind === "demo" ? "Идея одобрена менеджером. В тестовой коллекции используются демонстрационные суммы; актуальная стоимость и наличие для вашей поездки пока не подтверждены." : "Идея одобрена менеджером. Плановые ориентиры коллекции не подтверждают цену: менеджер уточнит стоимость, наличие и условия для ваших дат.";
	const comment = raw(request.managerNote);
	const body = factsTable(summary) + (comment ? section("Комментарий менеджера", `<p>${paragraphs(comment)}</p>`) : "") + parts.html + (q ? section("Стоимость и условия", factsTable(quoteRows)) : "") + note(quoteMessage);
	return {
		subject: `Tiare Travel · ${request.status === "agreed" ? "Согласованное путешествие" : "Ваше предложение"} · ${oneLine(title, 110)}`,
		html: shell({
			title,
			subtitle: intro,
			body,
			url,
			cta: "Открыть моё предложение",
			accessNote: "Откройте ссылку в том же браузере, в котором заполняли анкету: доступ к заявке сохранён в этом браузере."
		}),
		text: `TIARE TRAVEL\n${title}\n\n${intro}\n\n${factsText(summary)}${comment ? "\n\nКомментарий менеджера\n" + comment : ""}\n\n${parts.text}${q ? "\n\nСТОИМОСТЬ И УСЛОВИЯ\n" + factsText(quoteRows) : ""}\n\n${quoteMessage}\n\nОткрыть предложение в том же браузере, в котором заполняли анкету: ${url}`
	};
}
var providerNames = new Set([
	"invalid_idempotency_key",
	"validation_error",
	"missing_api_key",
	"restricted_api_key",
	"email_above_quota",
	"invalid_permission",
	"suspended_api_key",
	"not_found",
	"method_not_allowed",
	"concurrent_idempotent_requests",
	"invalid_idempotent_request",
	"resource_locked",
	"invalid_attachment",
	"invalid_parameter",
	"missing_required_field",
	"missing_required_parameter",
	"daily_quota_exceeded",
	"monthly_quota_exceeded",
	"rate_limit_exceeded",
	"application_error",
	"service_unavailable"
]);
function retryAfter(header) {
	if (!header) return null;
	const seconds = /^\d+(\.\d+)?$/.test(header.trim()) ? Math.ceil(Number(header)) : Math.ceil((Date.parse(header) - Date.now()) / 1e3);
	return Number.isFinite(seconds) && seconds >= 0 ? Math.min(seconds, 744 * 3600) : null;
}
function statusError(response, data, mutating) {
	const status = Number(response.status) || 0;
	const name = providerNames.has(data?.name) ? data.name : "provider_error";
	const quota = ["daily_quota_exceeded", "monthly_quota_exceeded"].includes(name);
	const concurrent = name === "concurrent_idempotent_requests";
	const retryable = !quota && (status === 408 || status === 429 || status >= 500 || concurrent || name === "resource_locked");
	return new EmailProviderError("RESEND_" + name.toUpperCase(), {
		status,
		retryable,
		ambiguous: mutating && (status === 408 || status >= 500 || concurrent),
		retryAfterSeconds: retryAfter(response.headers?.get?.("retry-after")),
		message: status === 401 || status === 403 ? "Почтовый сервис отклонил доступ. Проверьте ключ API, права и домен отправителя." : quota ? "Достигнут лимит почтового сервиса. Отправка приостановлена до проверки лимита." : status === 429 ? "Почтовый сервис просит уменьшить частоту запросов." : name === "invalid_idempotent_request" ? "Содержимое повторной отправки отличается от исходного. Требуется проверка очереди." : "Почтовый сервис отклонил запрос. Код ошибки сохранён без персональных данных."
	});
}
function credential(apiKey) {
	if (typeof apiKey !== "string" || !/^re_[A-Za-z0-9_-]{3,500}$/.test(apiKey)) throw invalid("EMAIL_NOT_CONFIGURED", "Не настроен ключ почтового сервиса.");
	return apiKey;
}
function address(value, displayName = false) {
	if (typeof value !== "string" || value.length > 320 || /[\r\n\u0000]/.test(value)) throw invalid();
	const input = value.trim();
	const bracket = displayName ? input.match(/^[^<>]{1,120}\s*<([^<>]+)>$/) : null;
	const mailbox = bracket ? bracket[1] : input;
	if (!/^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\.[A-Za-z]{2,63}$/.test(mailbox) || mailbox.includes("..")) throw invalid();
	return input;
}
function addresses(value) {
	const items = Array.isArray(value) ? value : [value];
	if (!items.length || items.length > 50) throw invalid();
	return items.map((v) => address(v));
}
async function providerRequest({ apiKey, path = "", method, body, idempotencyKey, fetchImpl }) {
	const token = credential(apiKey), mutating = method === "POST", controller = new AbortController();
	let timer;
	const timeout = new Promise((_, reject) => {
		timer = setTimeout(() => {
			controller.abort();
			reject(new EmailProviderError("RESEND_TIMEOUT", {
				retryable: true,
				ambiguous: mutating,
				message: "Почтовый сервис не ответил вовремя. Результат отправки требует проверки."
			}));
		}, RESEND_TIMEOUT_MS);
	});
	const operation = (async () => {
		let response;
		try {
			response = await fetchImpl(API + path, {
				method,
				headers: {
					Authorization: `Bearer ${token}`,
					Accept: "application/json",
					...body ? { "Content-Type": "application/json" } : {},
					...idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {}
				},
				...body ? { body: JSON.stringify(body) } : {},
				signal: controller.signal,
				redirect: "error"
			});
		} catch {
			throw new EmailProviderError(controller.signal.aborted ? "RESEND_TIMEOUT" : "RESEND_NETWORK_ERROR", {
				retryable: true,
				ambiguous: mutating,
				message: "Не получен ответ почтового сервиса. Проверьте результат до новой отправки."
			});
		}
		let data;
		try {
			data = await response.json();
		} catch {
			if (!response.ok) throw statusError(response, null, mutating);
			throw new EmailProviderError("RESEND_INVALID_RESPONSE", {
				status: response.status,
				retryable: true,
				ambiguous: mutating,
				message: "Почтовый сервис вернул неполный ответ. Результат требует проверки."
			});
		}
		if (!response.ok) throw statusError(response, data, mutating);
		if (!data || typeof data !== "object" || !UUID.test(data.id)) throw new EmailProviderError("RESEND_INVALID_RESPONSE", {
			status: response.status,
			retryable: true,
			ambiguous: mutating,
			message: "Почтовый сервис не вернул идентификатор письма. Результат требует проверки."
		});
		return data;
	})();
	try {
		return await Promise.race([operation, timeout]);
	} finally {
		clearTimeout(timer);
	}
}
/** A successful POST means accepted by Resend, never delivered to an inbox. */
async function sendResendEmail({ apiKey, from, to, replyTo, subject, html, text, idempotencyKey, fetchImpl = globalThis.fetch }) {
	if (typeof fetchImpl !== "function") throw invalid();
	if (typeof idempotencyKey !== "string" || !/^[\x21-\x7e]{1,256}$/.test(idempotencyKey)) throw invalid("EMAIL_INVALID_IDEMPOTENCY_KEY", "Для отправки нужен постоянный уникальный ключ очереди.");
	if (typeof subject !== "string" || !subject.trim() || subject.length > 300 || /[\r\n\u0000]/.test(subject)) throw invalid();
	if (typeof html !== "string" || !html.trim() || html.length > 5e5 || typeof text !== "string" || !text.trim() || text.length > 2e5) throw invalid();
	return {
		id: (await providerRequest({
			apiKey,
			method: "POST",
			body: {
				from: address(from, true),
				to: addresses(to),
				subject,
				html,
				text,
				...replyTo ? { reply_to: addresses(replyTo) } : {}
			},
			idempotencyKey,
			fetchImpl
		})).id,
		status: "accepted"
	};
}
var knownEvents = new Set([
	"sent",
	"queued",
	"scheduled",
	"delivery_delayed",
	"delivered",
	"opened",
	"clicked",
	"bounced",
	"complained",
	"failed",
	"canceled",
	"suppressed"
]);
/** Only expose provider delivery metadata, never retrieved message bodies. */
async function getResendEmail({ apiKey, id, fetchImpl = globalThis.fetch }) {
	if (!UUID.test(String(id)) || typeof fetchImpl !== "function") throw invalid();
	const data = await providerRequest({
		apiKey,
		path: `/${encodeURIComponent(id)}`,
		method: "GET",
		fetchImpl
	});
	if (data.id.toLowerCase() !== id.toLowerCase()) throw new EmailProviderError("RESEND_INVALID_RESPONSE", {
		retryable: true,
		message: "Почтовый сервис вернул другой идентификатор письма."
	});
	const reportedEvent = knownEvents.has(data.last_event) ? data.last_event : "unknown";
	const status = [
		"sent",
		"queued",
		"scheduled",
		"delivery_delayed"
	].includes(reportedEvent) ? "accepted" : [
		"delivered",
		"opened",
		"clicked"
	].includes(reportedEvent) ? "delivered" : [
		"failed",
		"canceled",
		"suppressed"
	].includes(reportedEvent) ? "failed" : reportedEvent;
	return {
		id: data.id,
		status,
		reportedEvent,
		...typeof data.created_at === "string" && Number.isFinite(Date.parse(data.created_at)) ? { createdAt: new Date(data.created_at).toISOString() } : {}
	};
}
//#endregion
//#region server/outbox.mjs
var emailPattern = /^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$/;
var errorText = (error) => error?.code ? `Ошибка почтового сервиса: ${String(error.code).replace(/[^A-Z0-9_]/g, "").slice(0, 70)}` : "Почтовый сервис временно недоступен.";
var safe = (value) => String(value).replace(/[&<>"']/g, (c) => ({
	"&": "&amp;",
	"<": "&lt;",
	">": "&gt;",
	"\"": "&quot;",
	"'": "&#39;"
})[c]);
function appUrl(env) {
	const source = env.APP_BASE_URL || env.RENDER_EXTERNAL_URL || "http://localhost:" + String(env.PORT || 3e3);
	const url = new URL(source);
	if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) throw new Error("APP_BASE_URL должен быть адресом сервиса без учётных данных.");
	return url.origin;
}
function mailConfiguration(env, mode = env.MAIL_MODE || "test") {
	const missing = [];
	if (!["test", "live"].includes(mode)) missing.push("MAIL_MODE");
	if (!env.RESEND_API_KEY) missing.push("RESEND_API_KEY");
	if (!env.MAIL_FROM || /[\r\n]/.test(env.MAIL_FROM)) missing.push("MAIL_FROM");
	if (mode === "test" && !emailPattern.test(env.TEST_RECIPIENT_EMAIL || "")) missing.push("TEST_RECIPIENT_EMAIL");
	const clientConfigured = !missing.length, managerConfigured = clientConfigured && emailPattern.test(env.MANAGER_EMAIL || "");
	return {
		mode,
		configured: managerConfigured,
		missing: [...missing, ...!emailPattern.test(env.MANAGER_EMAIL || "") ? ["MANAGER_EMAIL"] : []],
		managerConfigured,
		clientConfigured
	};
}
function recordConfiguration(env, kind, mode, intendedTo) {
	const missing = mailConfiguration(env, mode).missing.filter((v) => kind === "manager" || v !== "MANAGER_EMAIL");
	if (!intendedTo && kind === "client") missing.push("CUSTOMER_EMAIL");
	return {
		ready: !missing.length,
		missing
	};
}
function outboxRecord(row) {
	return {
		id: row.id,
		requestId: row.request_id,
		requestRevision: row.request_revision,
		kind: row.kind,
		mode: row.mode,
		intendedTo: row.intended_to,
		recipient: row.recipient,
		from: row.sender || "",
		subject: row.subject,
		status: row.status,
		providerId: row.provider_id,
		attempts: row.attempts,
		lastError: row.last_error,
		createdAt: row.created_at,
		updatedAt: row.updated_at,
		acceptedAt: row.accepted_at,
		deliveredAt: row.delivered_at
	};
}
async function enqueueMail(tx, env, request, kind, dedupeSuffix) {
	const key = `${kind}:${request.id}:${dedupeSuffix}`;
	const existing = await tx.query("SELECT * FROM email_outbox WHERE dedupe_key=?", [key]);
	if (existing.rows[0]) return outboxRecord(existing.rows[0]);
	const mode = env.MAIL_MODE || "test", intendedTo = kind === "manager" ? env.MANAGER_EMAIL || "" : request.customerEmail;
	const recipient = mode === "test" ? env.TEST_RECIPIENT_EMAIL || "" : intendedTo;
	const content = kind === "manager" ? buildManagerRequestEmail({
		request,
		candidates: request.candidates,
		appUrl: appUrl(env)
	}) : buildClientProposalEmail({
		request,
		appUrl: appUrl(env)
	});
	const banner = `ТЕСТОВАЯ СРЕДА TIARE TRAVEL. Предназначено: ${intendedTo || "менеджеру (адрес не настроен)"}. Письмо направлено только на тестовый адрес.`;
	const subject = mode === "test" ? `[ТЕСТ] ${content.subject}` : content.subject, html = mode === "test" ? `<div style="padding:14px;background:#fff1cc;color:#423b20;font:14px Arial">${safe(banner)}</div>${content.html}` : content.html, text = mode === "test" ? `${banner}\n\n${content.text}` : content.text;
	const cfg = recordConfiguration(env, kind, mode, intendedTo), id = randomUUID(), now = (/* @__PURE__ */ new Date()).toISOString(), status = cfg.ready ? "queued" : "blocked", lastError = cfg.ready ? "" : `Не настроено: ${cfg.missing.join(", ")}.`;
	await tx.query("INSERT INTO email_outbox(id,dedupe_key,request_id,request_revision,kind,mode,intended_to,recipient,sender,subject,html,text_body,reply_to,status,next_attempt_at,last_error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
		id,
		key,
		request.id,
		request.revision,
		kind,
		mode,
		intendedTo,
		recipient,
		env.MAIL_FROM || "",
		subject,
		html,
		text,
		kind === "manager" ? request.customerEmail : "",
		status,
		now,
		lastError,
		now,
		now
	]);
	return {
		id,
		status
	};
}
function createOutbox(db, env, providers = {
	send: sendResendEmail,
	get: getResendEmail
}) {
	let running = false, stopped = false, timer;
	async function processOne() {
		const now = (/* @__PURE__ */ new Date()).toISOString();
		const row = (await db.query("SELECT * FROM email_outbox WHERE ((status='queued' AND next_attempt_at<=?) OR (status='sending' AND lease_until<?)) ORDER BY created_at LIMIT 1", [now, now])).rows[0];
		if (!row) return false;
		const cfg = recordConfiguration(env, row.kind, row.mode, row.intended_to);
		if (!cfg.ready) {
			await db.query("UPDATE email_outbox SET status='blocked',last_error=?,updated_at=? WHERE id=? AND status IN ('queued','sending')", [
				`Не настроено: ${cfg.missing.join(", ")}.`,
				now,
				row.id
			]);
			return true;
		}
		if (!row.sender || !row.recipient) {
			await db.query("UPDATE email_outbox SET status='blocked',last_error=?,updated_at=? WHERE id=? AND status IN ('queued','sending')", [
				"Не зафиксированы адреса письма. Проверьте настройки и повторите отправку.",
				now,
				row.id
			]);
			return true;
		}
		if (row.attempts > 0 && Date.now() - Date.parse(row.created_at) > 23 * 36e5) {
			await db.query("UPDATE email_outbox SET status='unknown',last_error=?,updated_at=? WHERE id=? AND status IN ('queued','sending')", [
				"Прошло окно безопасного повтора. Проверьте письмо у провайдера перед повторной отправкой.",
				now,
				row.id
			]);
			return true;
		}
		if (row.kind === "client") {
			const request = (await db.query("SELECT revision,status,quote_json FROM travel_requests WHERE id=?", [row.request_id])).rows[0];
			const quote = request?.quote_json ? JSON.parse(request.quote_json) : null;
			if (!request || request.revision !== row.request_revision || !["selection", "proposal"].includes(request.status) || request.status === "proposal" && (!quote || Date.parse(quote.validUntil) <= Date.now())) {
				await db.query("UPDATE email_outbox SET status='cancelled',last_error=?,updated_at=? WHERE id=? AND status IN ('queued','sending')", [
					"Предложение изменилось или срок цены истёк. Подготовьте письмо из актуальной заявки.",
					now,
					row.id
				]);
				return true;
			}
		}
		const leaseUntil = new Date(Date.now() + 12e4).toISOString();
		if ((await db.query("UPDATE email_outbox SET status='sending',attempts=attempts+1,lease_until=?,updated_at=? WHERE id=? AND ((status='queued' AND next_attempt_at<=?) OR (status='sending' AND lease_until<?))", [
			leaseUntil,
			now,
			row.id,
			now,
			now
		])).changes !== 1) return true;
		try {
			const result = await providers.send({
				apiKey: env.RESEND_API_KEY,
				from: row.sender,
				to: row.recipient,
				replyTo: row.reply_to || void 0,
				subject: row.subject,
				html: row.html,
				text: row.text_body,
				idempotencyKey: `tiare-${row.id}`
			});
			if (!result?.id) throw Object.assign(/* @__PURE__ */ new Error("Provider returned no id"), {
				code: "NO_PROVIDER_ID",
				retryable: true,
				ambiguous: true
			});
			const accepted = (/* @__PURE__ */ new Date()).toISOString();
			await db.query("UPDATE email_outbox SET status='accepted',provider_id=?,accepted_at=?,updated_at=?,lease_until=NULL,last_error='',next_attempt_at=? WHERE id=? AND lease_until=?", [
				result.id,
				accepted,
				accepted,
				new Date(Date.now() + 6e4).toISOString(),
				row.id,
				leaseUntil
			]);
		} catch (error) {
			const ambiguous = error?.ambiguous ? 1 : 0, attempts = row.attempts + 1, retry = !!error?.retryable && attempts < 5;
			const delay = Math.min(15 * 6e4, Math.max(3e4, Number(error?.retryAfterSeconds || 0) * 1e3, 3e4 * 2 ** (attempts - 1)));
			const status = retry ? "queued" : ambiguous ? "unknown" : "failed";
			await db.query("UPDATE email_outbox SET status=?,ambiguous=?,next_attempt_at=?,lease_until=NULL,last_error=?,updated_at=? WHERE id=? AND lease_until=?", [
				status,
				ambiguous,
				new Date(Date.now() + delay).toISOString(),
				errorText(error),
				(/* @__PURE__ */ new Date()).toISOString(),
				row.id,
				leaseUntil
			]);
		}
		return true;
	}
	async function refresh(emailId) {
		const row = (await db.query("SELECT * FROM email_outbox WHERE id=?", [emailId])).rows[0];
		if (!row) {
			const e = /* @__PURE__ */ new Error("Письмо не найдено.");
			e.status = 404;
			throw e;
		}
		if (!row.provider_id) {
			const e = /* @__PURE__ */ new Error("Провайдер ещё не подтвердил приём письма.");
			e.status = 409;
			throw e;
		}
		if (!env.RESEND_API_KEY) {
			const e = /* @__PURE__ */ new Error("RESEND_API_KEY не настроен.");
			e.status = 503;
			throw e;
		}
		try {
			const result = await providers.get({
				apiKey: env.RESEND_API_KEY,
				id: row.provider_id
			});
			const now = (/* @__PURE__ */ new Date()).toISOString();
			const status = [
				"accepted",
				"delivered",
				"bounced",
				"complained",
				"failed",
				"unknown"
			].includes(result.status) ? result.status : "unknown";
			await db.query("UPDATE email_outbox SET status=?,updated_at=?,delivered_at=?,last_error=?,next_attempt_at=? WHERE id=?", [
				status,
				now,
				status === "delivered" ? row.delivered_at || now : row.delivered_at,
				status === "unknown" ? "Провайдер не сообщил однозначный статус." : "",
				new Date(Date.now() + 3e5).toISOString(),
				row.id
			]);
			return { status };
		} catch (error) {
			await db.query("UPDATE email_outbox SET last_error=?,next_attempt_at=?,updated_at=? WHERE id=?", [
				errorText(error),
				new Date(Date.now() + 3e5).toISOString(),
				(/* @__PURE__ */ new Date()).toISOString(),
				row.id
			]);
			const e = new Error(errorText(error));
			e.status = 502;
			throw e;
		}
	}
	async function retry(emailId) {
		return db.transaction(async (tx) => {
			const { rows } = await tx.query("SELECT * FROM email_outbox WHERE id=?", [emailId]);
			const row = rows[0];
			if (!row) {
				const e = /* @__PURE__ */ new Error("Письмо не найдено.");
				e.status = 404;
				throw e;
			}
			if (![
				"blocked",
				"failed",
				"unknown"
			].includes(row.status) || row.provider_id) {
				const e = /* @__PURE__ */ new Error("Это письмо уже отправлено или ожидает отправки. Повтор не требуется.");
				e.status = 409;
				throw e;
			}
			if (row.ambiguous && Date.now() - Date.parse(row.created_at) > 23 * 36e5) {
				const e = /* @__PURE__ */ new Error("Возможно, провайдер уже принял письмо. Проверьте его журнал: безопасный срок повторной отправки истёк.");
				e.status = 409;
				throw e;
			}
			const intended = row.intended_to || (row.kind === "manager" ? env.MANAGER_EMAIL || "" : ""), cfg = recordConfiguration(env, row.kind, row.mode, intended), status = cfg.ready ? "queued" : "blocked", recipient = row.recipient || (row.mode === "test" ? env.TEST_RECIPIENT_EMAIL || "" : intended);
			if (row.attempts && (!row.recipient || !row.intended_to || !row.sender)) {
				const e = /* @__PURE__ */ new Error("Нельзя менять адрес после попытки отправки.");
				e.status = 409;
				throw e;
			}
			const now = (/* @__PURE__ */ new Date()).toISOString();
			await tx.query("UPDATE email_outbox SET status=?,intended_to=?,recipient=?,sender=?,next_attempt_at=?,last_error=?,updated_at=? WHERE id=?", [
				status,
				intended,
				recipient,
				row.sender || env.MAIL_FROM || "",
				now,
				cfg.ready ? "" : `Не настроено: ${cfg.missing.join(", ")}.`,
				now,
				row.id
			]);
			return { status };
		});
	}
	async function drain() {
		if (running || stopped) return;
		running = true;
		try {
			for (let i = 0; i < 10 && !stopped; i++) if (!await processOne()) break;
		} catch {} finally {
			running = false;
		}
	}
	function trigger() {
		queueMicrotask(() => {
			drain();
		});
	}
	function start() {
		if (timer) return;
		timer = setInterval(() => {
			drain();
		}, 15e3);
		timer.unref();
		trigger();
	}
	async function stop() {
		stopped = true;
		if (timer) clearInterval(timer);
		while (running) await new Promise((resolve) => setTimeout(resolve, 25));
	}
	return {
		start,
		stop,
		trigger,
		drain,
		retry,
		refresh
	};
}
//#endregion
//#region server/workspace.mjs
var json = (value) => JSON.stringify(value);
var now = () => (/* @__PURE__ */ new Date()).toISOString();
var fail$1 = (status, message) => {
	const error = new Error(message);
	error.status = status;
	throw error;
};
var uuid$1 = stringType().uuid(), revision = numberType().int().nonnegative(), https = stringType().url().refine((s) => s.startsWith("https://") && !new URL(s).username && !new URL(s).password);
var quoteSchema$1 = objectType({
	amount: numberType().int().positive().max(1e8),
	source: https,
	travelDate: stringType(),
	scope: stringType().trim().min(15).max(4e3),
	availability: stringType().trim().min(10).max(2e3),
	terms: stringType().trim().min(10).max(4e3),
	validUntil: stringType(),
	checkedAt: stringType().optional()
});
var managerNote = stringType().max(4e3).default("");
var actionSchema = discriminatedUnionType("action", [
	objectType({
		action: literalType("create_request"),
		id: uuid$1,
		input: inputSchema,
		journeyId: stringType().nullable().optional(),
		customerName: stringType().trim().min(2).max(150),
		customerEmail: stringType().trim().email().max(254),
		contact: stringType().trim().max(300).default(""),
		notes: stringType().max(4e3).default("")
	}),
	objectType({
		action: literalType("regenerate_candidates"),
		id: uuid$1,
		revision
	}),
	objectType({
		action: literalType("save_candidate"),
		id: uuid$1,
		revision,
		candidateId: uuid$1,
		journey: journeySchema
	}),
	objectType({
		action: literalType("save_candidate_note"),
		id: uuid$1,
		revision,
		candidateId: uuid$1,
		reviewNote: stringType().max(4e3)
	}),
	objectType({
		action: literalType("publish_candidate"),
		id: uuid$1,
		revision,
		candidateId: uuid$1,
		managerNote
	}),
	objectType({
		action: literalType("save_internal_note"),
		id: uuid$1,
		revision,
		internalNote: stringType().max(8e3)
	}),
	objectType({
		action: literalType("update_request"),
		id: uuid$1,
		revision,
		status: enumType([
			"new",
			"review",
			"selection",
			"pricing",
			"proposal",
			"agreed"
		]),
		journeyId: stringType().nullable().optional(),
		managerNote,
		quote: quoteSchema$1.nullable().optional()
	}),
	objectType({
		action: literalType("select_request"),
		id: uuid$1,
		revision
	}),
	objectType({
		action: literalType("agree_request"),
		id: uuid$1,
		revision
	}),
	objectType({
		action: literalType("send_client_email"),
		id: uuid$1,
		revision
	}),
	objectType({
		action: literalType("retry_email"),
		emailId: uuid$1
	}),
	objectType({
		action: literalType("refresh_email"),
		emailId: uuid$1
	}),
	objectType({
		action: literalType("save_catalog"),
		journey: journeySchema
	}),
	objectType({
		action: literalType("add_material"),
		title: stringType().trim().min(3).max(200),
		category: stringType().trim().min(2).max(100),
		sourceUrl: unionType([literalType(""), https]).default(""),
		content: stringType().trim().min(10).max(16e3)
	})
]);
async function getCatalog(db) {
	const { rows } = await db.query("SELECT data_json FROM travel_catalog");
	const stored = new Map(rows.map((row) => {
		const j = JSON.parse(row.data_json);
		return [j.id, j];
	}));
	const catalog = seedJourneys.map((j) => stored.get(j.id) || structuredClone(j));
	for (const [id, j] of stored) if (!seedJourneys.some((s) => s.id === id)) catalog.push(j);
	return catalog;
}
function candidateFromRank(r, id = randomUUID()) {
	const matching = r.matches.map((p) => priorities[p]);
	const rationale = [
		matching.length ? `Совпадает с интересами: ${matching.join(", ")}.` : "Альтернативная идея для обсуждения с клиентом.",
		r.estimate ? `Расчёт ${r.estimate.kind === "demo" ? "демонстрационный" : "ориентировочный"}: ${money(r.estimate.low)}–${money(r.estimate.high)}.` : "Семейная стоимость требует отдельного расчёта.",
		r.issues.length ? `До предложения клиенту устранить: ${r.issues.join("; ")}.` : "По структурированным ограничениям анкеты конфликтов не найдено.",
		r.review.length ? `Менеджеру проверить: ${r.review.join("; ")}.` : "Цены, рейсы и наличие подтверждаются отдельно."
	].join(" ");
	return {
		id,
		journey: structuredClone(r.journey),
		estimate: r.estimate,
		matches: r.matches,
		issues: r.issues,
		review: r.review,
		rationale
	};
}
function makeCandidates(catalog, input) {
	const ranked = rankJourneys(catalog, input), chosen = [], keys = /* @__PURE__ */ new Set();
	for (const r of ranked) {
		const key = [r.journey.country, ...r.journey.hotel.map((h) => h.name)].join("|");
		if (keys.has(key)) continue;
		keys.add(key);
		chosen.push(candidateFromRank(r));
		if (chosen.length === 5) break;
	}
	return chosen;
}
function validateJourney(j) {
	if (j.minNights > j.maxNights || j.baseNights < j.minNights || j.baseNights > j.maxNights) fail$1(400, "Проверьте минимальную, базовую и максимальную продолжительность.");
	if (j.priceKind === "estimate" && !/^https:\/\/\S+/.test(j.priceSource)) fail$1(400, "Для рабочего бюджетного ориентира нужна ссылка на источник.");
	return j;
}
function validateRequestDate(input) {
	if (input.date && input.date < (/* @__PURE__ */ new Date()).toISOString().slice(0, 10)) fail$1(400, "Дата поездки уже прошла. Укажите новую дату.");
}
function validatePublished(request) {
	if (!request.journey) fail$1(409, "Сначала выберите и одобрите вариант маршрута.");
	const ranked = rankJourneys([{
		...request.journey,
		active: true
	}], request.input)[0];
	if (ranked.issues.length) fail$1(409, `Маршрут противоречит анкете: ${ranked.issues.join("; ")}. Измените вариант или оформите новую согласованную анкету.`);
	if (ranked.review.length && request.managerNote.trim().length < 20) fail$1(400, "Опишите клиенту, как проверены индивидуальные условия и логистика маршрута.");
	validateRequestDate(request.input);
	return ranked;
}
function validateQuote(quote, request) {
	const parsed = quoteSchema$1.safeParse(quote);
	if (!parsed.success) fail$1(400, "Нужны полная стоимость, источник проверки, даты, наличие, состав услуг и условия оплаты и отмены.");
	const q = parsed.data;
	if (q.amount > request.input.budget) fail$1(400, "Подтверждённая стоимость превышает бюджет клиента. Нужна новая согласованная анкета.");
	if (!inputSchema.shape.date.safeParse(q.travelDate).success || !q.travelDate || q.travelDate < (/* @__PURE__ */ new Date()).toISOString().slice(0, 10)) fail$1(400, "Проверьте дату поездки.");
	if (request.input.date && q.travelDate !== request.input.date) fail$1(400, "Дата предложения не совпадает с анкетой. Изменение дат оформляется новой согласованной заявкой.");
	const expiry = Date.parse(q.validUntil);
	if (!Number.isFinite(expiry) || expiry <= Date.now()) fail$1(400, "Срок действия подтверждённой цены должен быть в будущем.");
	if (expiry > Date.parse(q.travelDate + "T23:59:59.999Z")) fail$1(400, "Срок действия цены не может быть позже даты начала поездки.");
	return {
		...q,
		validUntil: new Date(expiry).toISOString(),
		checkedAt: now()
	};
}
function currentQuote$1(request) {
	if (!request.quote) fail$1(409, "Подтверждённое предложение ещё не готово.");
	const checked = Date.parse(request.quote.checkedAt);
	if (!Number.isFinite(checked) || checked > Date.now() + 1e3) fail$1(409, "Данные проверки цены некорректны.");
	validateQuote(request.quote, request);
	return request.quote;
}
function clientView(request) {
	const { clientId, candidates, internalNote, candidateId, ...publicRequest } = request;
	if (![
		"selection",
		"pricing",
		"proposal",
		"agreed"
	].includes(request.status)) {
		publicRequest.journey = null;
		publicRequest.journeyId = null;
		publicRequest.quote = null;
		publicRequest.managerNote = "";
	}
	if (!["proposal", "agreed"].includes(request.status)) publicRequest.quote = null;
	return publicRequest;
}
async function ownedRequest(tx, session, id) {
	const { rows } = await tx.query("SELECT * FROM travel_requests WHERE id=?", [id]);
	if (!rows[0]) fail$1(404, "Заявка не найдена.");
	const request = requestFromRow(rows[0]);
	if (session.role !== "manager" && request.clientId !== session.clientId) fail$1(404, "Заявка не найдена.");
	return request;
}
function assertRevision(request, expected) {
	if (request.revision !== expected) fail$1(409, "Заявка уже изменена. Обновите её перед сохранением.");
}
async function readWorkspace(db, session, env) {
	if (session.role !== "manager") {
		const { rows } = await db.query("SELECT * FROM travel_requests WHERE client_id=? ORDER BY created_at DESC LIMIT 100", [session.clientId]);
		return {
			role: "client",
			signedIn: true,
			catalog: [],
			requests: rows.map(requestFromRow).map(clientView),
			materials: []
		};
	}
	const [catalog, requests, materials, outbox, history] = await Promise.all([
		getCatalog(db),
		db.query("SELECT * FROM travel_requests ORDER BY created_at DESC LIMIT 200"),
		db.query("SELECT * FROM travel_materials ORDER BY created_at DESC LIMIT 100"),
		db.query("SELECT * FROM email_outbox ORDER BY created_at DESC LIMIT 200"),
		db.query("SELECT request_id,revision,action,created_at FROM request_versions ORDER BY created_at DESC LIMIT 1000")
	]);
	return {
		role: "manager",
		signedIn: true,
		catalog,
		requests: requests.rows.map(requestFromRow).map((r) => ({
			...r,
			history: history.rows.filter((h) => h.request_id === r.id).map((h) => ({
				revision: h.revision,
				action: h.action,
				createdAt: h.created_at
			}))
		})),
		materials: materials.rows.map((r) => ({
			id: r.id,
			title: r.title,
			category: r.category,
			sourceUrl: r.source_url,
			content: r.content,
			createdAt: r.created_at
		})),
		mail: {
			...mailConfiguration(env),
			testRecipientEmail: env.TEST_RECIPIENT_EMAIL || "",
			managerEmail: env.MANAGER_EMAIL || "",
			from: env.MAIL_FROM || ""
		},
		outbox: outbox.rows.map(outboxRecord)
	};
}
async function mutateWorkspace(db, session, env, outbox, body) {
	const parsed = actionSchema.safeParse(body);
	if (!parsed.success) fail$1(400, "Проверьте заполненные поля. " + parsed.error.issues.slice(0, 3).map((i) => i.path.join(".") + ": " + i.message).join("; "));
	const data = parsed.data;
	if (["select_request", "agree_request"].includes(data.action) && session.role !== "client") fail$1(403, "Выбор и согласование выполняет клиент в своём кабинете.");
	if (![
		"create_request",
		"select_request",
		"agree_request"
	].includes(data.action)) requireManager(session);
	if (data.action === "retry_email") return outbox.retry(data.emailId);
	if (data.action === "refresh_email") return outbox.refresh(data.emailId);
	if (data.action === "save_catalog") {
		const j = validateJourney({
			...data.journey,
			updatedAt: now()
		});
		await db.query("INSERT INTO travel_catalog(id,data_json,updated_at) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET data_json=excluded.data_json,updated_at=excluded.updated_at", [
			j.id,
			json(j),
			j.updatedAt
		]);
		return { ok: true };
	}
	if (data.action === "add_material") {
		const id = randomUUID();
		await db.query("INSERT INTO travel_materials(id,title,category,source_url,content,created_at) VALUES(?,?,?,?,?,?)", [
			id,
			data.title,
			data.category,
			data.sourceUrl,
			data.content,
			now()
		]);
		return { id };
	}
	if (data.action === "create_request") {
		validateRequestDate(data.input);
		return db.transaction(async (tx) => {
			const existing = await tx.query("SELECT * FROM travel_requests WHERE id=?", [data.id]);
			if (existing.rows[0]) {
				const r = requestFromRow(existing.rows[0]);
				if (r.clientId !== session.clientId) fail$1(409, "Идентификатор заявки уже используется. Начните новую заявку.");
				if (json(r.input) !== json(data.input) || r.customerEmail !== data.customerEmail || r.customerName !== data.customerName || r.notes !== data.notes || r.contact !== data.contact) fail$1(409, "Эта заявка уже сохранена с другими данными. Создайте новую заявку.");
				return {
					id: r.id,
					status: r.status,
					revision: r.revision,
					duplicate: true
				};
			}
			const count = await tx.query("SELECT COUNT(*) AS total FROM travel_requests WHERE client_id=? AND created_at>?", [session.clientId, (/* @__PURE__ */ new Date(Date.now() - 36e5)).toISOString()]);
			if (Number(count.rows[0].total) >= 12) fail$1(429, "Слишком много заявок за короткое время. Повторите позже.");
			const timestamp = now(), candidates = makeCandidates(await getCatalog(tx), data.input);
			const r = {
				id: data.id,
				clientId: session.clientId,
				input: data.input,
				journeyId: null,
				journey: null,
				candidateId: null,
				status: "review",
				customerName: data.customerName,
				customerEmail: data.customerEmail,
				contact: data.contact,
				notes: data.notes,
				managerNote: "",
				internalNote: "",
				candidates,
				quote: null,
				revision: 0,
				createdAt: timestamp,
				updatedAt: timestamp
			};
			if ((await tx.query("INSERT INTO travel_requests(id,client_id,input_json,journey_id,journey_json,candidate_id,status,customer_name,customer_email,contact,notes,manager_note,internal_note,candidates_json,quote_json,revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING", [
				r.id,
				r.clientId,
				json(r.input),
				null,
				null,
				null,
				r.status,
				r.customerName,
				r.customerEmail,
				r.contact,
				r.notes,
				"",
				"",
				json(candidates),
				null,
				0,
				timestamp,
				timestamp
			])).changes !== 1) fail$1(409, "Заявка уже сохраняется. Обновите список заявок.");
			await saveVersion(tx, r, "create_request", session.role);
			const email = await enqueueMail(tx, env, r, "manager", "new");
			return {
				id: r.id,
				status: r.status,
				revision: 0,
				email
			};
		});
	}
	return db.transaction(async (tx) => {
		const r = await ownedRequest(tx, session, data.id);
		assertRevision(r, data.revision);
		const previousRevision = r.revision;
		if (data.action === "send_client_email") {
			if (!["selection", "proposal"].includes(r.status)) fail$1(409, "Письмо клиенту можно отправить после публикации идеи или подтверждения полной стоимости.");
			validatePublished(r);
			if (r.status === "proposal") currentQuote$1(r);
			return {
				ok: true,
				email: await enqueueMail(tx, env, r, "client", `revision-${r.revision}`)
			};
		}
		if (data.action === "save_internal_note") r.internalNote = data.internalNote;
		else if (data.action === "regenerate_candidates") {
			if (!["review", "selection"].includes(r.status)) fail$1(409, "Верните заявку на проверку маршрута перед повторным подбором.");
			r.candidates = makeCandidates(await getCatalog(tx), r.input);
		} else if (data.action === "save_candidate") {
			const idx = r.candidates.findIndex((c) => c.id === data.candidateId);
			if (idx === -1) fail$1(404, "Вариант не найден.");
			const j = validateJourney({
				...data.journey,
				updatedAt: now()
			});
			if (!j.active) fail$1(400, "Вариант заявки должен быть активным.");
			const previous = r.candidates[idx];
			r.candidates[idx] = {
				...candidateFromRank(rankJourneys([j], r.input)[0], data.candidateId),
				...typeof previous.reviewNote === "string" ? { reviewNote: previous.reviewNote } : {}
			};
		} else if (data.action === "save_candidate_note") {
			const candidate = r.candidates.find((c) => c.id === data.candidateId);
			if (!candidate) fail$1(404, "Вариант не найден.");
			candidate.reviewNote = data.reviewNote;
		} else if (data.action === "publish_candidate") {
			if (!["review", "selection"].includes(r.status)) fail$1(409, "Верните заявку на проверку маршрута перед заменой предложения.");
			const candidate = r.candidates.find((c) => c.id === data.candidateId);
			if (!candidate) fail$1(404, "Вариант не найден.");
			candidate.reviewNote = data.managerNote;
			r.journey = structuredClone(candidate.journey);
			r.journeyId = r.journey.id;
			r.candidateId = candidate.id;
			r.managerNote = data.managerNote;
			r.status = "selection";
			r.quote = null;
			validatePublished(r);
		} else if (data.action === "select_request") {
			if (r.status !== "selection") fail$1(409, "Выбрать можно только идею, опубликованную менеджером.");
			validatePublished(r);
			r.status = "pricing";
			r.quote = null;
		} else if (data.action === "agree_request") {
			if (r.status !== "proposal") fail$1(409, "Согласовать можно только актуальное предложение с подтверждённой стоимостью.");
			validatePublished(r);
			currentQuote$1(r);
			r.status = "agreed";
		} else if (data.action === "update_request") {
			if (!{
				new: ["review"],
				review: ["review"],
				selection: ["selection", "review"],
				pricing: [
					"pricing",
					"proposal",
					"review"
				],
				proposal: [
					"proposal",
					"pricing",
					"review"
				],
				agreed: ["agreed", "review"]
			}[r.status]?.includes(data.status)) fail$1(409, "Недопустимый переход этапа. Публикация идеи, выбор и согласование выполняются отдельными действиями.");
			if (data.journeyId !== void 0 && data.journeyId !== r.journeyId) fail$1(409, "Выберите маршрут из вариантов заявки и опубликуйте его отдельным действием.");
			r.managerNote = data.managerNote;
			if (data.status === "proposal") {
				validatePublished(r);
				r.quote = validateQuote(data.quote, r);
			} else if (data.status === "agreed") {
				if (json(data.quote) !== json(r.quote)) fail$1(409, "Согласованное предложение нельзя менять. Верните заявку на проверку и запросите новое согласование.");
			} else if ([
				"review",
				"pricing",
				"selection"
			].includes(data.status)) r.quote = null;
			r.status = data.status;
		}
		r.revision++;
		r.updatedAt = now();
		await updateStoredRequest(tx, r, previousRevision, data.action, session.role);
		let email;
		if (data.action === "agree_request") email = await enqueueMail(tx, env, r, "manager", `agreed-${r.revision}`);
		return {
			id: r.id,
			status: r.status,
			revision: r.revision,
			...email ? { email } : {}
		};
	});
}
//#endregion
//#region server/proposal-data.mjs
var fail = (status, message) => {
	throw Object.assign(new Error(message), { status });
};
var uuid = stringType().uuid();
var quoteSchema = objectType({
	amount: numberType().int().positive().max(1e8),
	source: stringType().url().refine((value) => {
		try {
			const url = new URL(value);
			return url.protocol === "https:" && !url.username && !url.password;
		} catch {
			return false;
		}
	}),
	checkedAt: stringType(),
	travelDate: stringType(),
	scope: stringType().trim().min(15).max(4e3),
	availability: stringType().trim().min(10).max(2e3),
	terms: stringType().trim().min(10).max(4e3),
	validUntil: stringType()
});
var descriptiveKeys = [
	"id",
	"title",
	"subtitle",
	"country",
	"destinations",
	"format",
	"tags",
	"minNights",
	"maxNights",
	"baseNights",
	"changes",
	"needsReview",
	"overview",
	"tradeoff",
	"hotel",
	"program",
	"includes",
	"excludes",
	"logistics",
	"season",
	"documents",
	"updatedAt"
];
var clientInputKeys = [
	"formats",
	"destination",
	"departure",
	"date",
	"nights",
	"adults",
	"children",
	"priorities",
	"pace",
	"cabin",
	"directOnly",
	"maxHours",
	"special",
	"hotelChanges"
];
function parseProposalQuery(id, params) {
	if (!uuid.safeParse(id).success) fail(400, "Некорректный номер заявки.");
	const allowed = new Set([
		"mode",
		"revision",
		"candidateId"
	]);
	for (const key of params.keys()) if (!allowed.has(key) || params.getAll(key).length !== 1) fail(400, "Некорректные параметры PDF.");
	const mode = params.get("mode"), revisionText = params.get("revision"), candidateId = params.get("candidateId");
	if (!["review", "client"].includes(mode)) fail(400, "Укажите режим PDF: review или client.");
	if (!revisionText || !/^(0|[1-9][0-9]*)$/.test(revisionText) || !Number.isSafeInteger(Number(revisionText))) fail(400, "Укажите сохранённую версию заявки.");
	if (params.has("candidateId") && (mode !== "review" || !uuid.safeParse(candidateId).success)) fail(400, "Вариант можно указывать только для PDF на проверку.");
	return {
		id,
		mode,
		revision: Number(revisionText),
		candidateId
	};
}
function savedJson(value, label) {
	try {
		return JSON.parse(value);
	} catch {
		fail(409, `Не удалось прочитать сохранённые данные: ${label}.`);
	}
}
function currentQuote(value, input, status, mode, clock) {
	if (!["proposal", "agreed"].includes(status) || !value) return null;
	let decoded;
	try {
		decoded = JSON.parse(value);
	} catch {
		return null;
	}
	const parsed = quoteSchema.safeParse(decoded);
	if (!parsed.success) return null;
	const q = parsed.data, date = inputSchema.shape.date.safeParse(q.travelDate), timestamp = clock.getTime();
	const checked = Date.parse(q.checkedAt), expiry = Date.parse(q.validUntil), today = clock.toISOString().slice(0, 10);
	if (!date.success || !q.travelDate || q.travelDate < today || q.amount > input.budget) return null;
	if (input.date && q.travelDate !== input.date) return null;
	if (!Number.isFinite(checked) || checked > timestamp || !Number.isFinite(expiry) || expiry <= timestamp) return null;
	if (expiry > Date.parse(q.travelDate + "T23:59:59.999Z")) return null;
	const result = {
		amount: q.amount,
		checkedAt: q.checkedAt,
		travelDate: q.travelDate,
		scope: q.scope,
		availability: q.availability,
		validUntil: q.validUntil,
		terms: q.terms
	};
	if (mode === "review") result.source = q.source;
	return result;
}
/** Read one saved request; nothing in this function changes its status, snapshot, or revision. */
async function loadProposalModel(db, session, options, { clock = /* @__PURE__ */ new Date() } = {}) {
	requireManager(session);
	const { id, mode, revision, candidateId } = options;
	const params = new URLSearchParams({
		mode: String(mode),
		revision: String(revision)
	});
	if (candidateId !== null && candidateId !== void 0) params.set("candidateId", String(candidateId));
	parseProposalQuery(id, params);
	const { rows } = await db.query("SELECT id,input_json,journey_json,candidate_id,status,customer_name,manager_note,quote_json,candidates_json,revision FROM travel_requests WHERE id=?", [id]);
	const row = rows[0];
	if (!row) fail(404, "Заявка не найдена.");
	if (row.revision !== revision) fail(409, "Заявка уже изменена. Обновите её и сформируйте PDF из актуальной сохранённой версии.");
	if (mode === "client" && ![
		"selection",
		"pricing",
		"proposal",
		"agreed"
	].includes(row.status)) fail(409, "Сначала опубликуйте одобренный вариант для клиента.");
	const inputResult = inputSchema.safeParse(savedJson(row.input_json, "анкета"));
	if (!inputResult.success) fail(409, "Сохранённая анкета требует проверки перед подготовкой PDF.");
	const input = inputResult.data;
	let selected, reviewNote;
	if (mode === "review" && candidateId) {
		const candidates = savedJson(row.candidates_json, "варианты");
		const candidate = Array.isArray(candidates) ? candidates.find((item) => item?.id === candidateId) : null;
		if (!candidate) fail(404, "Выбранный вариант не принадлежит этой заявке.");
		selected = candidate.journey;
		if (candidate.reviewNote !== void 0) {
			const savedNote = stringType().max(4e3).safeParse(candidate.reviewNote);
			if (!savedNote.success) fail(409, "Сохранённый комментарий к варианту требует проверки перед подготовкой PDF.");
			reviewNote = savedNote.data;
		} else reviewNote = candidateId === row.candidate_id ? String(row.manager_note || "") : "";
	} else {
		if (!row.journey_json) fail(409, mode === "review" ? "Выберите сохранённый вариант для проверки." : "Опубликованный маршрут ещё не готов.");
		if (![
			"selection",
			"pricing",
			"proposal",
			"agreed"
		].includes(row.status)) fail(409, "Для PDF без варианта нужен опубликованный маршрут.");
		selected = savedJson(row.journey_json, "опубликованный маршрут");
	}
	const journeyResult = journeySchema.safeParse(selected);
	if (!journeyResult.success) fail(409, "Сохранённый маршрут требует проверки перед подготовкой PDF.");
	const journey = journeyResult.data;
	const ranked = mode === "review" ? rankJourneys([{
		...journey,
		active: true
	}], input)[0] : null;
	const safeJourney = mode === "review" ? journey : Object.fromEntries(descriptiveKeys.map((key) => [key, journey[key]]));
	return {
		id: row.id,
		revision: row.revision,
		customerName: String(row.customer_name),
		input: mode === "review" ? input : Object.fromEntries(clientInputKeys.map((key) => [key, input[key]])),
		journey: safeJourney,
		managerNote: reviewNote !== void 0 ? reviewNote : String(row.manager_note || ""),
		quote: candidateId ? null : currentQuote(row.quote_json, input, row.status, mode, clock),
		mode,
		generatedAt: clock.toISOString(),
		issues: ranked?.issues || [],
		review: ranked?.review || [],
		estimate: ranked?.estimate || null
	};
}
//#endregion
//#region server/proposal-pdf.mjs
var PALETTE = {
	ivory: "#F5F1E7",
	paper: "#FCFAF5",
	sand: "#E4DAC7",
	taupe: "#84796A",
	olive: "#414A32",
	ink: "#282D24",
	muted: "#706D61",
	gold: "#B39965",
	line: "#D8D0BF",
	light: "#EBE7DC",
	white: "#FFFFFF",
	warning: "#785B33"
};
var PAGE = {
	width: 841.89,
	height: 595.28,
	margin: 42,
	bottom: 540
};
var BODY_WIDTH = PAGE.width - PAGE.margin * 2;
var MAX_PAGES = 180;
var MAX_BYTES = 12 * 1024 * 1024;
var assetCache = /* @__PURE__ */ new Map();
var safeText = (value) => String(value ?? "").replace(/\r\n?/g, "\n").replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, "").replace(/[\u00A0\u202F]/g, " ").replace(/[\u2010-\u2015\u2212]/g, "-");
var canonical = (value) => safeText(value).trim().toLocaleLowerCase("ru-RU").replaceAll("ё", "е");
var rubles = (value) => Number.isFinite(Number(value)) ? `${new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 }).format(Number(value)).replace(/[\u00A0\u202F]/g, " ")} ₽` : "Не указана";
var displayDate = (value) => {
	if (!value) return "Дата уточняется";
	const parsed = new Date(value);
	return Number.isNaN(parsed.getTime()) ? safeText(value) : new Intl.DateTimeFormat("ru-RU", {
		day: "2-digit",
		month: "long",
		year: "numeric",
		timeZone: "UTC"
	}).format(parsed);
};
var displayDateTime = (value) => {
	const parsed = new Date(value);
	return Number.isNaN(parsed.getTime()) ? safeText(value) : `${new Intl.DateTimeFormat("ru-RU", {
		day: "2-digit",
		month: "2-digit",
		year: "numeric",
		hour: "2-digit",
		minute: "2-digit",
		timeZone: "UTC"
	}).format(parsed)} UTC`;
};
var httpsUrl = (value) => {
	try {
		const url = new URL(value);
		return url.protocol === "https:" && !url.username && !url.password ? url.href : null;
	} catch {
		return null;
	}
};
var sourceHost = (value) => {
	try {
		return new URL(value).hostname.replace(/^www\./, "");
	} catch {
		return "Открыть источник";
	}
};
var formatNames = {
	beach: "Острова и море",
	city: "Города и культура",
	multi: "Несколько мест",
	safari: "Сафари и природа",
	cruise: "Круизы и экспедиции"
};
var priorityNames = {
	quiet: "Тишина и приватность",
	food: "Гастрономия",
	culture: "Культура",
	nature: "Природа",
	comfort: "Комфорт",
	activity: "Новые впечатления"
};
var paceNames = {
	calm: "Спокойный",
	balanced: "Сбалансированный",
	active: "Активный"
};
async function assetsAt(root) {
	const key = resolve(root);
	if (!assetCache.has(key)) assetCache.set(key, Promise.all([
		readFile(resolve(key, "tiare-pdf-title.ttf")),
		readFile(resolve(key, "tiare-pdf-title-italic.ttf")),
		readFile(resolve(key, "tiare-pdf-body.ttf")),
		readFile(resolve(key, "tiare-pdf-cover.jpg")),
		readFile(resolve(key, "pdf-geography.json"), "utf8")
	]).then(([title, italic, body, cover, geography]) => ({
		title,
		italic,
		body,
		cover,
		geography: JSON.parse(geography)
	})).catch((error) => {
		assetCache.delete(key);
		throw error;
	}));
	return assetCache.get(key);
}
function flower(doc, x, y, r, { opacity = 1, fill = PALETTE.paper, stroke = PALETTE.olive } = {}) {
	doc.save().opacity(opacity).translate(x, y);
	for (let i = 0; i < 7; i++) {
		doc.save().rotate(i * 360 / 7);
		doc.moveTo(0, 0).bezierCurveTo(-r * .24, -r * .18, -r * .48, -r * .69, -r * .12, -r * .96).bezierCurveTo(r * .22, -r * 1.08, r * .47, -r * .59, r * .15, -r * .17).bezierCurveTo(r * .09, -r * .07, r * .03, 0, 0, 0).fillAndStroke(fill, stroke).restore();
	}
	doc.circle(0, 0, r * .105).fill(PALETTE.gold).restore();
}
function makeLayout(doc, model) {
	const pages = [];
	let current = null;
	const setFont = (font = "body", size = 11) => doc.font(font).fontSize(size);
	const widthOf = (text, font = "body", size = 11) => {
		setFont(font, size);
		return doc.widthOfString(safeText(text));
	};
	function wrap(value, width, font = "body", size = 11) {
		const paragraphs = safeText(value).split("\n");
		const output = [];
		setFont(font, size);
		for (const paragraph of paragraphs) {
			const words = paragraph.trim().split(/\s+/).filter(Boolean);
			if (!words.length) {
				output.push("");
				continue;
			}
			let line = "";
			for (const word of words) {
				const candidate = line ? `${line} ${word}` : word;
				if (doc.widthOfString(candidate) <= width) {
					line = candidate;
					continue;
				}
				if (line) {
					output.push(line);
					line = "";
				}
				if (doc.widthOfString(word) <= width) {
					line = word;
					continue;
				}
				for (const character of Array.from(word)) {
					if (line && doc.widthOfString(line + character) > width) {
						output.push(line);
						line = "";
					}
					line += character;
				}
			}
			if (line) output.push(line);
		}
		return output.length ? output : [""];
	}
	function text(value, x, y, { font = "body", size = 11, color = PALETTE.ink, width = BODY_WIDTH, align = "left", link } = {}) {
		setFont(font, size);
		doc.fillColor(color);
		const options = {
			lineBreak: false,
			align,
			width
		};
		const safeLink = httpsUrl(link);
		if (safeLink) options.link = safeLink;
		doc.text(safeText(value), x, y, options);
	}
	function wrapped(value, x, y, width, { font = "body", size = 11, color = PALETTE.ink, leading = size * 1.45, link } = {}) {
		const lines = wrap(value, width, font, size);
		for (const line of lines) {
			text(line, x, y, {
				font,
				size,
				color,
				width,
				link
			});
			y += leading;
		}
		return y;
	}
	function base({ cover = false, final = false } = {}) {
		if (pages.length >= MAX_PAGES) throw Object.assign(/* @__PURE__ */ new Error("Предложение превышает 180 страниц. Сократите повторяющиеся описания перед экспортом."), { status: 413 });
		doc.addPage({
			size: [PAGE.width, PAGE.height],
			margin: 0
		});
		doc.rect(0, 0, PAGE.width, PAGE.height).fill(final ? "#F0EEE4" : PALETTE.ivory);
		pages.push({
			cover,
			final
		});
		if (!cover) {
			flower(doc, 53, 39, 13, { fill: PALETTE.paper });
			text("Tiare Travel", 75, 25, {
				font: "title",
				size: 24,
				color: PALETTE.olive
			});
			text(model.mode === "review" ? "РАБОЧАЯ ВЕРСИЯ · ДЛЯ ПРОВЕРКИ" : "PRIVATE JOURNEY", 492, 33, {
				size: 8.2,
				color: model.mode === "review" ? PALETTE.warning : PALETTE.muted,
				width: 308,
				align: "right"
			});
			doc.moveTo(PAGE.margin, 68).lineTo(PAGE.width - PAGE.margin, 68).lineWidth(.6).stroke(PALETTE.line);
		}
	}
	function page(title, { subtitle = "", continued = false, section = title } = {}) {
		base();
		current = {
			title,
			subtitle,
			section,
			y: 87,
			continued
		};
		const heading = continued ? `${title} · продолжение` : title;
		current.y = wrapped(heading, PAGE.margin, current.y, BODY_WIDTH, {
			font: "title",
			size: 29,
			color: PALETTE.olive,
			leading: 33
		}) + 7;
		if (subtitle && !continued) current.y = wrapped(subtitle, PAGE.margin, current.y, BODY_WIDTH, {
			size: 9.5,
			color: PALETTE.muted,
			leading: 14
		}) + 14;
		else current.y += 8;
		return current;
	}
	function next() {
		const previous = current;
		page(previous.title, {
			section: previous.section,
			continued: true
		});
	}
	function ensure(height) {
		if (!current) throw new Error("PDF layout has no current page.");
		if (current.y + height > PAGE.bottom) next();
	}
	function gap(height = 12) {
		ensure(height);
		current.y += height;
	}
	function paragraph(value, { label, font = "body", size = 11, color = PALETTE.ink, leading = size * 1.48, after = 14, link } = {}) {
		if (label) {
			ensure(36);
			text(label, PAGE.margin, current.y, {
				font: "body",
				size: 9,
				color: PALETTE.muted
			});
			current.y += 18;
		}
		const lines = wrap(value, BODY_WIDTH, font, size);
		for (const line of lines) {
			ensure(leading + 2);
			text(line, PAGE.margin, current.y, {
				font,
				size,
				color,
				link
			});
			current.y += leading;
		}
		current.y += after;
	}
	function label(value) {
		ensure(32);
		text(value, PAGE.margin, current.y, {
			font: "title",
			size: 20,
			color: PALETTE.olive
		});
		current.y += 30;
	}
	function unitsForCard({ eyebrow, title, titleSize = 22, sections = [], footer }, width) {
		const units = [];
		const add = (value, { font = "body", size = 10.5, color = PALETTE.ink, leading = size * 1.43, after = 7, link } = {}) => {
			if (value === void 0 || value === null || value === "") return;
			const lines = wrap(value, width, font, size);
			for (let i = 0; i < lines.length; i++) units.push({
				text: lines[i],
				font,
				size,
				color,
				leading,
				link,
				last: i === lines.length - 1,
				space: i === lines.length - 1 ? after : 0
			});
		};
		add(eyebrow, {
			size: 8.5,
			color: PALETTE.muted,
			leading: 12,
			after: 6
		});
		add(title, {
			font: "title",
			size: titleSize,
			color: PALETTE.olive,
			leading: titleSize * 1.15,
			after: 10
		});
		for (const section of sections) {
			if (section.label) add(section.label, {
				size: 8.5,
				color: PALETTE.muted,
				leading: 12,
				after: 4
			});
			add(section.text, {
				font: section.font ?? "body",
				size: section.size ?? 10.5,
				color: section.color ?? PALETTE.ink,
				link: section.link,
				after: section.after ?? 9
			});
		}
		add(footer, {
			size: 8.5,
			color: PALETTE.muted,
			leading: 12,
			after: 0
		});
		return units;
	}
	function card(config, { accent = PALETTE.gold } = {}) {
		const inset = 19;
		const units = unitsForCard(config, BODY_WIDTH - inset * 2 - 4);
		const fullHeight = 27 + units.reduce((sum, unit) => sum + unit.leading + unit.space, 0);
		const freshStart = 87 + wrap(`${current.title} · продолжение`, BODY_WIDTH, "title", 29).length * 33 + 15;
		if (fullHeight <= PAGE.bottom - freshStart && current.y + fullHeight > PAGE.bottom) next();
		let index = 0, part = 0;
		while (index < units.length) {
			const continuation = part > 0;
			const topPadding = continuation ? 31 : 17;
			ensure(topPadding + Math.min(3, units.length - index) * 16 + 17);
			let available = PAGE.bottom - current.y - topPadding - 14;
			let count = 0, height = 0;
			while (index + count < units.length) {
				const unit = units[index + count], unitHeight = unit.leading + unit.space;
				if (height + unitHeight > available && count) break;
				if (height + unitHeight > available && !count) {
					next();
					available = PAGE.bottom - current.y - topPadding - 14;
					continue;
				}
				height += unitHeight;
				count++;
			}
			if (!count) throw new Error("PDF card cannot fit a text line.");
			const tail = units.length - index - count;
			if (tail > 0 && tail < 4 && count > 5) {
				const move = 4 - tail;
				for (let i = 0; i < move; i++) {
					const unit = units[index + count - 1];
					height -= unit.leading + unit.space;
					count--;
				}
			}
			const boxHeight = topPadding + height + 10;
			doc.roundedRect(PAGE.margin, current.y, BODY_WIDTH, boxHeight, 3).fill(PALETTE.paper);
			doc.rect(PAGE.margin, current.y, 2, boxHeight).fill(accent);
			let y = current.y + 17;
			if (continuation) {
				text("ПРОДОЛЖЕНИЕ", PAGE.margin + inset + 4, y - 2, {
					size: 7.5,
					color: PALETTE.taupe
				});
				y += 14;
			}
			for (let i = 0; i < count; i++) {
				const unit = units[index + i];
				text(unit.text, PAGE.margin + inset + 4, y, {
					font: unit.font,
					size: unit.size,
					color: unit.color,
					width: BODY_WIDTH - inset * 2 - 4,
					link: unit.link
				});
				y += unit.leading + unit.space;
			}
			current.y += boxHeight + 12;
			index += count;
			part++;
			if (index < units.length) next();
		}
	}
	function cardGrid(configs, { columns = 2, accent = PALETTE.gold } = {}) {
		if (configs.length === 1) {
			card(configs[0], { accent });
			return;
		}
		const gutter = 14, width = (BODY_WIDTH - gutter * (columns - 1)) / columns, inset = 17;
		const grouped = configs.map((config) => unitsForCard(config, width - inset * 2));
		const height = Math.max(...grouped.map((units) => 27 + units.reduce((sum, unit) => sum + unit.leading + unit.space, 0)));
		const freshStart = 87 + wrap(`${current.title} · продолжение`, BODY_WIDTH, "title", 29).length * 33 + 15;
		if (height > PAGE.bottom - freshStart) {
			for (const config of configs) card(config, { accent });
			return;
		}
		ensure(height + 1);
		const top = current.y;
		for (let column = 0; column < grouped.length; column++) {
			const x = PAGE.margin + column * (width + gutter);
			doc.roundedRect(x, top, width, height, 3).fill(PALETTE.paper);
			doc.rect(x, top, 2, height).fill(accent);
			let y = top + 17;
			for (const unit of grouped[column]) {
				text(unit.text, x + inset, y, {
					font: unit.font,
					size: unit.size,
					color: unit.color,
					width: width - inset * 2,
					link: unit.link
				});
				y += unit.leading + unit.space;
			}
		}
		current.y = top + height + 12;
	}
	function factGrid(items) {
		const gutter = 26, width = (BODY_WIDTH - gutter) / 2;
		for (let index = 0; index < items.length; index += 2) {
			const pair = items.slice(index, index + 2).map(([name, value]) => ({
				name,
				lines: wrap(value, width, "body", 10.5)
			}));
			const height = 13 + Math.max(...pair.map((item) => item.lines.length)) * 15.5 + 11;
			if (height > 250) {
				for (const item of items.slice(index, index + 2)) paragraph(item[1], { label: item[0] });
				continue;
			}
			ensure(height);
			for (let column = 0; column < pair.length; column++) {
				const x = PAGE.margin + column * (width + gutter), item = pair[column];
				text(item.name, x, current.y, {
					size: 8.5,
					color: PALETTE.muted,
					width
				});
				let y = current.y + 13;
				for (const line of item.lines) {
					text(line, x, y, {
						size: 10.5,
						color: PALETTE.ink,
						width
					});
					y += 15.5;
				}
				doc.moveTo(x, current.y + height - 5).lineTo(x + width, current.y + height - 5).lineWidth(.45).stroke(PALETTE.line);
			}
			current.y += height;
		}
		current.y += 9;
	}
	function rows(items, { valueColor = PALETTE.ink, labelWidth = 195 } = {}) {
		for (const [name, value] of items) {
			const nameLines = wrap(name, labelWidth, "body", 9.5), valueLines = wrap(value, BODY_WIDTH - labelWidth - 24, "body", 10.5);
			const height = Math.max(nameLines.length, valueLines.length) * 15.5 + 17;
			if (height > 250) {
				paragraph(value, {
					label: name,
					after: 12
				});
				continue;
			}
			ensure(height);
			for (let i = 0; i < nameLines.length; i++) text(nameLines[i], PAGE.margin, current.y + i * 15.5, {
				size: 9.5,
				color: PALETTE.muted,
				width: labelWidth
			});
			for (let i = 0; i < valueLines.length; i++) text(valueLines[i], PAGE.margin + labelWidth + 24, current.y + i * 15.5, {
				size: 10.5,
				color: valueColor,
				width: BODY_WIDTH - labelWidth - 24
			});
			current.y += height;
			doc.moveTo(PAGE.margin, current.y - 6).lineTo(PAGE.width - PAGE.margin, current.y - 6).lineWidth(.45).stroke(PALETTE.line);
		}
		current.y += 9;
	}
	function bullets(items, { color = PALETTE.ink } = {}) {
		for (const value of items ?? []) {
			const lines = wrap(value, BODY_WIDTH - 22, "body", 10.5);
			let first = true;
			for (const line of lines) {
				ensure(17);
				if (first) {
					doc.circle(PAGE.margin + 3, current.y + 6, 1.7).fill(PALETTE.gold);
					first = false;
				}
				text(line, PAGE.margin + 16, current.y, {
					size: 10.5,
					color,
					width: BODY_WIDTH - 22
				});
				current.y += 15.5;
			}
			current.y += 9;
		}
	}
	function footers() {
		const count = pages.length;
		for (let i = 0; i < count; i++) {
			doc.switchToPage(i);
			const { cover } = pages[i], color = cover ? PALETTE.taupe : PALETTE.muted;
			if (!cover) doc.moveTo(PAGE.margin, 557).lineTo(PAGE.width - PAGE.margin, 557).lineWidth(.5).stroke(PALETTE.line);
			text(`TIARE TRAVEL · ЗАЯВКА ${safeText(model.id).slice(0, 8).toUpperCase()} · ВЕРСИЯ ${model.revision}`, PAGE.margin, 571, {
				size: 7.2,
				color,
				width: 590
			});
			text(`${String(i + 1).padStart(2, "0")} / ${String(count).padStart(2, "0")}`, PAGE.width - 104, 570, {
				size: 8,
				color,
				width: 62,
				align: "right"
			});
		}
	}
	return {
		doc,
		model,
		pages,
		base,
		page,
		next,
		ensure,
		gap,
		paragraph,
		label,
		card,
		cardGrid,
		factGrid,
		rows,
		bullets,
		footers,
		wrap,
		text,
		wrapped,
		widthOf,
		get y() {
			return current?.y ?? 0;
		},
		set y(value) {
			current.y = value;
		}
	};
}
function coverPage(layout, assets) {
	const { doc, model } = layout, j = model.journey, input = model.input;
	layout.base({ cover: true });
	doc.save().rect(402, 0, PAGE.width - 402, PAGE.height).clip().image(assets.cover, 402, 0, {
		cover: [PAGE.width - 402, PAGE.height],
		align: "center",
		valign: "center"
	}).restore();
	doc.rect(0, 0, 408, PAGE.height).fill(PALETTE.ivory);
	doc.rect(399, 0, 9, PAGE.height).fill(PALETTE.sand);
	flower(doc, 56, 49, 19, { fill: PALETTE.paper });
	layout.text("Tiare", 88, 24, {
		font: "title",
		size: 33,
		color: PALETTE.olive
	});
	layout.text("TRAVEL", 89, 57, {
		size: 7.4,
		color: PALETTE.muted
	});
	doc.moveTo(42, 105).lineTo(361, 105).lineWidth(.65).stroke(PALETTE.line);
	const status = model.mode === "review" ? "РАБОЧАЯ ВЕРСИЯ ДЛЯ МЕНЕДЖЕРА" : model.quote ? "ИНДИВИДУАЛЬНОЕ ПРЕДЛОЖЕНИЕ" : "ИДЕЯ ВАШЕГО ПУТЕШЕСТВИЯ";
	layout.wrapped(status, 42, 126, 320, {
		size: 8.7,
		color: model.mode === "review" ? PALETTE.warning : PALETTE.muted,
		leading: 13
	});
	let titleSize = 43, titleLines = layout.wrap(j.title, 321, "title", titleSize);
	while (titleLines.length * titleSize * 1.04 > 163 && titleSize > 12) {
		titleSize -= .5;
		titleLines = layout.wrap(j.title, 321, "title", titleSize);
	}
	let y = 180;
	for (const line of titleLines) {
		layout.text(line, 42, y, {
			font: "title",
			size: titleSize,
			color: PALETTE.olive,
			width: 321
		});
		y += titleSize * 1.04;
	}
	y += 18;
	let subSize = 19, subtitleLines = layout.wrap(j.subtitle, 318, "italic", subSize);
	while (subtitleLines.length * subSize * 1.15 > 468 - y && subSize > 9) {
		subSize -= .5;
		subtitleLines = layout.wrap(j.subtitle, 318, "italic", subSize);
	}
	for (const line of subtitleLines) {
		layout.text(line, 43, y, {
			font: "italic",
			size: subSize,
			color: PALETTE.taupe,
			width: 318
		});
		y += subSize * 1.15;
	}
	doc.moveTo(42, 489).lineTo(116, 489).lineWidth(1).stroke(PALETTE.gold);
	const effectiveDate = model.quote?.travelDate || input.date;
	layout.text(effectiveDate ? displayDate(effectiveDate) : "Даты подберём вместе", 42, 507, {
		size: 10.5,
		color: PALETTE.olive,
		width: 321
	});
	const party = `${input.adults} взр.${input.children?.length ? ` · ${input.children.length} дет.` : ""} · ${input.nights} ночей по заявке`;
	layout.text(party, 42, 526, {
		size: 9.2,
		color: PALETTE.muted,
		width: 321
	});
	doc.save().opacity(.87).rect(426, 523, 389, 31).fill(PALETTE.ivory).restore();
	layout.text("Иллюстрация настроения путешествия", 442, 533, {
		size: 8.2,
		color: PALETTE.olive,
		width: 357
	});
}
function overviewPages(layout) {
	const { model } = layout, j = model.journey, input = model.input;
	layout.page("Замысел путешествия", { subtitle: formatNames[j.format] ?? "Индивидуальный маршрут" });
	layout.paragraph(j.overview, {
		font: "title",
		size: 21,
		leading: 27,
		color: PALETTE.olive,
		after: 18
	});
	const effectiveDate = model.quote?.travelDate || input.date;
	const facts = [
		["Для кого", model.customerName || "Гостей Tiare Travel"],
		["Направление и точки маршрута", `${j.country} · ${(j.destinations ?? []).join(" · ")}`],
		[model.quote ? "Дата поездки / длительность" : "Выезд / длительность по заявке", `${effectiveDate ? displayDate(effectiveDate) : "Дата подбирается с менеджером"} · ${input.nights} ночей по заявке`],
		["Состав путешественников", `${input.adults} взрослых${input.children?.length ? `; дети: ${input.children.map((age) => `${age} лет`).join(", ")}` : "; без детей"}`],
		["Ритм и приоритеты", `${paceNames[input.pace] ?? input.pace}. ${(input.priorities ?? []).map((p) => priorityNames[p] ?? p).join(", ") || "Уточняются"}`]
	];
	if (model.mode === "review") facts.push(["Бюджет из заявки", rubles(input.budget)]);
	layout.factGrid(facts);
	layout.card({
		eyebrow: "ОСОБЕННОСТЬ ВАРИАНТА",
		sections: [{ text: j.tradeoff }]
	});
	if (model.managerNote?.trim()) layout.card({
		eyebrow: "КОММЕНТАРИЙ ВАШЕГО МЕНЕДЖЕРА",
		sections: [{ text: model.managerNote }]
	}, { accent: PALETTE.olive });
	if (input.special?.trim()) layout.paragraph(input.special, { label: "Индивидуальные пожелания из заявки" });
}
function programPages(layout) {
	const j = layout.model.journey;
	layout.page("Программа и впечатления", { subtitle: "Содержание выбранного варианта. Распределение этапов по датам и часам согласуется с менеджером." });
	const cards = [];
	for (let i = 0; i < (j.program ?? []).length; i++) {
		const day = j.program[i], sections = [{ text: `Днём. ${day.activity}` }, { text: `Вечером. ${day.evening}` }];
		for (const source of day.sources ?? []) sections.push({
			text: `${source.title} · ${sourceHost(source.url)}`,
			size: 9,
			color: PALETTE.olive,
			link: source.url,
			after: 5
		});
		cards.push({
			eyebrow: `ЭТАП ${String(i + 1).padStart(2, "0")}`,
			title: day.title,
			titleSize: 20,
			sections
		});
	}
	const columns = cards.length === 3 ? 3 : 2;
	for (let index = 0; index < cards.length; index += columns) layout.cardGrid(cards.slice(index, index + columns), { columns });
}
function hotelPages(layout) {
	const j = layout.model.journey;
	layout.page("Отели и пространство отдыха", { subtitle: "Конкретные объекты и категории размещения из выбранного варианта. Наличие на даты проверяется до оформления." });
	for (let i = 0; i < (j.hotel ?? []).length; i++) {
		const hotel = j.hotel[i];
		layout.card({
			eyebrow: `${String(i + 1).padStart(2, "0")} · ${hotel.location}`,
			title: hotel.name,
			sections: [
				{
					label: "КАТЕГОРИЯ НОМЕРА / ВИЛЛЫ / КАЮТЫ",
					text: hotel.room,
					color: PALETTE.olive
				},
				{
					label: "ДЕТАЛИ РАЗМЕЩЕНИЯ",
					text: hotel.description
				},
				{
					text: `Официальный источник · ${sourceHost(hotel.url)}`,
					link: hotel.url,
					size: 9,
					color: PALETTE.olive
				}
			]
		});
	}
}
function logisticsPages(layout) {
	const { model } = layout, input = model.input;
	layout.page("Дорога и билеты", { subtitle: "Логистика маршрута и требования к перелётам" });
	layout.paragraph(model.journey.logistics, {
		font: "title",
		size: 21,
		leading: 28,
		color: PALETTE.olive,
		after: 24
	});
	layout.rows([
		["Отправление из", input.departure || "Город уточняется"],
		[model.quote ? "Дата поездки в расчёте" : "Запрошенная дата выезда", model.quote?.travelDate || input.date ? displayDate(model.quote?.travelDate || input.date) : "Дата уточняется"],
		["Класс перелёта", input.cabin === "business" ? "Бизнес-класс" : "Экономический класс"],
		["Пересадки", input.directOnly ? "По заявке нужны прямые рейсы" : "Допускаются по согласованию"],
		["Ограничение дороги", input.maxHours > 0 ? `По заявке: не более ${input.maxHours} ч. Требует проверки на конкретных рейсах.` : "Отдельный лимит времени в заявке не задан"],
		["Смены размещения", `В выбранном варианте: ${model.journey.changes ?? 0}. Допустимо по заявке: ${input.hotelChanges ?? 0}.`]
	]);
	layout.card({
		eyebrow: "ЧТО ПОДТВЕРЖДАЕТСЯ ДО ОФОРМЛЕНИЯ",
		sections: [{ text: "Конкретные рейсы, аэропорты, время вылета и прилёта, багаж, тарифные правила и условия трансферов. В сохранённом варианте нет отдельной проверенной таблицы билетов." }]
	});
}
function mapPages(layout, geography) {
	const { doc, model } = layout;
	const destinations = model.journey.destinations ?? [];
	const dictionary = geography?.locations ?? {};
	const points = destinations.map((name, index) => {
		const place = dictionary[canonical(name)];
		return {
			name,
			index,
			place: place && Number.isFinite(place.lon) && Number.isFinite(place.lat) && Math.abs(place.lat) <= 90 && Math.abs(place.lon) <= 180 ? place : null
		};
	});
	const known = points.filter((point) => point.place);
	layout.page("География путешествия", { subtitle: "Ориентиры выбранного маршрута" });
	const legendWidth = 231, legendX = PAGE.margin + BODY_WIDTH - legendWidth;
	const sideLegend = points.map((point) => {
		const title = `${String(point.index + 1).padStart(2, "0")} · ${point.name}`;
		const status = point.place ? "Ориентир нанесён на карту." : "Географическая точка уточняется.";
		const source = point.place?.source ? `Источник · ${sourceHost(point.place.source)}` : null;
		const titleLines = layout.wrap(title, legendWidth, "title", 16), statusLines = layout.wrap(status, legendWidth, "body", 10.5), sourceLines = source ? layout.wrap(source, legendWidth, "body", 8.5) : [];
		return {
			point,
			titleLines,
			statusLines,
			sourceLines,
			height: titleLines.length * 19 + 4 + statusLines.length * 15 + 3 + sourceLines.length * 12 + 9
		};
	});
	const useSidebar = points.length <= 5 && sideLegend.reduce((sum, item) => sum + item.height, 0) <= 320;
	const box = {
		x: PAGE.margin,
		y: layout.y + 2,
		w: useSidebar ? BODY_WIDTH - legendWidth - 25 : BODY_WIDTH,
		h: 320
	};
	doc.roundedRect(box.x, box.y, box.w, box.h, 3).fill("#E8E9DE");
	let west = -180, east = 180, south = -63, north = 85;
	if (known.length) {
		const lons = known.map((point) => point.place.lon), lats = known.map((point) => point.place.lat);
		west = Math.min(...lons);
		east = Math.max(...lons);
		south = Math.min(...lats);
		north = Math.max(...lats);
		const centerLat = (south + north) / 2;
		const lonSpread = Math.max(east - west, 5), latSpread = Math.max(north - south, 4);
		const padLon = Math.max(lonSpread * .16, 1), padLat = Math.max(latSpread * .19, 1);
		west -= padLon;
		east += padLon;
		south -= padLat;
		north += padLat;
		const aspect = box.w / box.h, cos = Math.max(.35, Math.cos(centerLat * Math.PI / 180));
		if ((east - west) * cos / (north - south) < aspect) {
			const desired = (north - south) * aspect / cos, mid = (east + west) / 2;
			west = mid - desired / 2;
			east = mid + desired / 2;
		} else {
			const desired = (east - west) * cos / aspect, mid = (north + south) / 2;
			south = mid - desired / 2;
			north = mid + desired / 2;
		}
		west = Math.max(-180, west);
		east = Math.min(180, east);
		south = Math.max(-85, south);
		north = Math.min(89, north);
	}
	const project = ([lon, lat]) => [box.x + (lon - west) / (east - west) * box.w, box.y + box.h - (lat - south) / (north - south) * box.h];
	const polygons = [];
	for (const feature of geography?.land?.features ?? []) if (feature.geometry?.type === "Polygon") polygons.push(feature.geometry.coordinates);
	else if (feature.geometry?.type === "MultiPolygon") polygons.push(...feature.geometry.coordinates);
	doc.save().rect(box.x, box.y, box.w, box.h).clip();
	for (const polygon of polygons) {
		let drew = false;
		for (const ring of polygon) {
			if (!ring.length) continue;
			if (ring.every((point) => point[0] < west) || ring.every((point) => point[0] > east) || ring.every((point) => point[1] < south) || ring.every((point) => point[1] > north)) continue;
			let first = true;
			for (const coordinate of ring) {
				const [x, y] = project(coordinate);
				if (first) {
					doc.moveTo(x, y);
					first = false;
				} else doc.lineTo(x, y);
			}
			doc.closePath();
			drew = true;
		}
		if (drew) doc.lineWidth(.45).fillAndStroke("#D1D6C1", "#A5AD93", "even-odd");
	}
	doc.lineWidth(1.4).strokeColor(PALETTE.gold).dash(5, { space: 4 });
	for (let i = 1; i < points.length; i++) {
		if (!points[i - 1].place || !points[i].place) continue;
		const [x1, y1] = project([points[i - 1].place.lon, points[i - 1].place.lat]);
		const [x2, y2] = project([points[i].place.lon, points[i].place.lat]);
		doc.moveTo(x1, y1).lineTo(x2, y2).stroke();
	}
	doc.undash();
	const labels = [];
	for (const point of known) {
		const [px, py] = project([point.place.lon, point.place.lat]);
		const marker = String(point.index + 1).padStart(2, "0"), markerW = 25, markerH = 21;
		const candidates = [
			[10, -27],
			[10, 7],
			[-35, -27],
			[-35, 7],
			[12, -52],
			[-36, 32],
			[40, -8],
			[-66, -8]
		];
		let label = null;
		const collides = (candidate) => labels.some((other) => candidate.x < other.x + other.w + 3 && candidate.x + candidate.w + 3 > other.x && candidate.y < other.y + other.h + 3 && candidate.y + candidate.h + 3 > other.y);
		for (const [dx, dy] of candidates) {
			const candidate = {
				x: Math.max(box.x + 5, Math.min(box.x + box.w - markerW - 5, px + dx)),
				y: Math.max(box.y + 5, Math.min(box.y + box.h - markerH - 5, py + dy)),
				w: markerW,
				h: markerH
			};
			if (!collides(candidate)) {
				label = candidate;
				break;
			}
		}
		if (!label) {
			let bestDistance = Infinity;
			for (let y = box.y + 6; y + markerH < box.y + box.h - 5; y += markerH + 6) for (let x = box.x + 6; x + markerW < box.x + box.w - 5; x += markerW + 6) {
				const candidate = {
					x,
					y,
					w: markerW,
					h: markerH
				};
				if (collides(candidate)) continue;
				const distance = (x + markerW / 2 - px) ** 2 + (y + markerH / 2 - py) ** 2;
				if (distance < bestDistance) {
					label = candidate;
					bestDistance = distance;
				}
			}
		}
		if (!label) throw new Error("PDF map has no free label position.");
		labels.push(label);
		doc.moveTo(px, py).lineTo(label.x + markerW / 2, label.y + markerH / 2).lineWidth(.6).stroke(PALETTE.olive);
		doc.circle(px, py, 4).fill(PALETTE.olive);
		doc.circle(px, py, 1.6).fill(PALETTE.paper);
		doc.roundedRect(label.x, label.y, markerW, markerH, 3).fill(PALETTE.olive);
		layout.text(marker, label.x + 4, label.y + 5, {
			size: 8.5,
			color: PALETTE.paper,
			width: markerW - 8,
			align: "center"
		});
	}
	doc.restore();
	if (!known.length) {
		const inset = useSidebar ? 31 : 90, panelWidth = box.w - inset * 2;
		doc.save().opacity(.94).roundedRect(box.x + inset, box.y + 83, panelWidth, 151, 3).fill(PALETTE.paper).restore();
		let noteY = layout.wrapped("Географические точки уточняются", box.x + inset + 18, box.y + 106, panelWidth - 36, {
			font: "title",
			size: 22,
			color: PALETTE.olive,
			leading: 25
		});
		layout.wrapped("На карте показан общий географический контекст.", box.x + inset + 18, noteY + 12, panelWidth - 36, {
			size: 10.5,
			color: PALETTE.muted,
			leading: 15
		});
	}
	if (useSidebar) {
		let legendY = box.y + 3;
		for (const item of sideLegend) {
			for (const line of item.titleLines) {
				layout.text(line, legendX, legendY, {
					font: "title",
					size: 16,
					color: PALETTE.olive,
					width: legendWidth
				});
				legendY += 19;
			}
			legendY += 4;
			for (const line of item.statusLines) {
				layout.text(line, legendX, legendY, {
					size: 10.5,
					color: PALETTE.muted,
					width: legendWidth
				});
				legendY += 15;
			}
			legendY += 3;
			for (const line of item.sourceLines) {
				layout.text(line, legendX, legendY, {
					size: 8.5,
					color: PALETTE.olive,
					width: legendWidth,
					link: item.point.place?.source
				});
				legendY += 12;
			}
			legendY += 9;
			doc.moveTo(legendX, legendY - 4).lineTo(legendX + legendWidth, legendY - 4).lineWidth(.45).stroke(PALETTE.line);
		}
	}
	layout.y = box.y + box.h + 14;
	layout.paragraph("Линии соединяют ориентиры; это не траектория перелёта или дороги. Время, транспорт, расстояния по маршруту и точные точки подтверждаются менеджером.", {
		size: 8.7,
		leading: 12.5,
		color: PALETTE.muted,
		after: 9
	});
	layout.paragraph(`Картографическая основа: ${geography?.source?.title ?? "Natural Earth"}. География показана схематично.`, {
		size: 8,
		leading: 12,
		color: PALETTE.muted,
		link: geography?.source?.url,
		after: 17
	});
	if (!useSidebar) for (const point of points) {
		const lines = [{
			text: point.place ? "Географический ориентир нанесён на карту." : "Географическая точка уточняется.",
			size: 9,
			color: PALETTE.muted,
			after: 4
		}];
		if (point.place?.source) lines.push({
			text: `Источник ориентира · ${sourceHost(point.place.source)}`,
			size: 8.5,
			color: PALETTE.olive,
			link: point.place.source,
			after: 3
		});
		layout.card({
			eyebrow: `ТОЧКА ${String(point.index + 1).padStart(2, "0")}`,
			title: point.name,
			sections: lines
		});
	}
}
function costPages(layout) {
	const { model } = layout, quote = model.quote;
	const preview = model.mode === "review";
	const estimate = preview ? model.estimate : null;
	layout.page("Стоимость и условия", { subtitle: quote ? "Данные проверки менеджера по выбранному варианту" : "Статус расчёта выбранного путешествия" });
	if (quote) {
		layout.card({
			eyebrow: "ПРОВЕРЕННАЯ СТОИМОСТЬ",
			title: rubles(quote.amount),
			titleSize: 39,
			sections: [{
				text: "Объём услуг и ограничения расчёта приведены ниже. Стоимость действительна в пределах указанного срока.",
				size: 10.5,
				color: PALETTE.muted
			}]
		}, { accent: PALETTE.olive });
		layout.rows([
			["Проверено менеджером", displayDateTime(quote.checkedAt)],
			["Дата поездки в расчёте", displayDate(quote.travelDate)],
			["Срок действия", displayDateTime(quote.validUntil)]
		]);
		layout.paragraph(quote.scope, { label: "Состав проверенного расчёта" });
		layout.paragraph(quote.availability, { label: "Наличие мест" });
		layout.paragraph(quote.terms, { label: "Условия стоимости и оформления" });
		if (preview && quote.source) layout.paragraph(`Источник проверки · ${sourceHost(quote.source)}`, {
			label: "Ссылка для менеджера",
			link: quote.source,
			color: PALETTE.olive,
			size: 9.5
		});
	} else if (preview && estimate) {
		const kind = estimate.kind === "demo" ? "ДЕМОНСТРАЦИОННЫЙ РАСЧЁТ" : "ПРЕДВАРИТЕЛЬНАЯ ОЦЕНКА";
		layout.card({
			eyebrow: kind,
			title: `${rubles(estimate.low)} - ${rubles(estimate.high)}`,
			titleSize: 34,
			sections: [{
				text: estimate.kind === "demo" ? "Условные суммы для проверки сервиса. Не являются тарифами поставщиков и не подтверждают наличие мест." : "Оценка бюджета без подтверждения тарифов и наличия мест. Для клиентского предложения требуется отдельная проверка.",
				size: 10.5,
				color: PALETTE.warning
			}]
		}, { accent: PALETTE.warning });
		layout.rows((estimate.lines ?? []).map((line) => [line.name, rubles(line.amount)]));
		layout.paragraph(estimate.source, {
			label: "Основа расчёта",
			size: 10.5,
			color: PALETTE.muted
		});
	} else layout.card({
		eyebrow: "СТАТУС СТОИМОСТИ",
		title: "Стоимость уточняется",
		sections: [{ text: "Этот документ описывает выбранную идею путешествия. Менеджер отдельно подтвердит стоимость, наличие, состав услуг и условия оформления под ваши даты." }]
	}, { accent: PALETTE.olive });
	const inclusionHeight = 92 + [...model.journey.includes ?? [], ...model.journey.excludes ?? []].reduce((sum, item) => sum + layout.wrap(item, BODY_WIDTH - 22, "body", 10.5).length * 15.5 + 9, 0);
	if (layout.y + inclusionHeight > PAGE.bottom) layout.page("Что входит в путешествие", { subtitle: "Состав выбранной идеи и услуги, которые согласуются отдельно" });
	else {
		layout.gap(5);
		layout.label("Состав выбранной идеи");
	}
	layout.paragraph("Перечень ниже сохранён в описании маршрута. Подтверждённый объём услуг определяется составом проверенного расчёта и условиями оформления.", {
		size: 10.5,
		color: PALETTE.muted,
		after: 16
	});
	layout.paragraph("Предусмотрено в описании", {
		font: "title",
		size: 19,
		color: PALETTE.olive,
		after: 10
	});
	layout.bullets(model.journey.includes);
	layout.gap(8);
	layout.paragraph("Отдельно или вне состава", {
		font: "title",
		size: 19,
		color: PALETTE.olive,
		after: 10
	});
	layout.bullets(model.journey.excludes);
}
function preparationPages(layout) {
	const j = layout.model.journey;
	layout.page("Детали перед путешествием", { subtitle: "Практическая информация из выбранного варианта" });
	layout.card({
		eyebrow: "СЕЗОН И УСЛОВИЯ",
		sections: [{ text: j.season }]
	});
	layout.card({
		eyebrow: "ДОКУМЕНТЫ И ВЪЕЗД",
		sections: [{ text: j.documents }]
	});
	if (layout.model.mode === "review") {
		const input = layout.model.input;
		const checks = [...layout.model.issues ?? [], ...layout.model.review ?? []];
		if (checks.length) {
			layout.label("Проверить перед отправкой");
			layout.bullets(checks, { color: PALETTE.warning });
		}
		const brief = [];
		if (input.destination?.trim()) brief.push(["Пожелание по направлению", input.destination]);
		if (input.excluded?.trim()) brief.push(["Исключённые направления", input.excluded]);
		if (input.formats?.length) brief.push(["Форматы из заявки", input.formats.map((item) => formatNames[item] ?? item).join(", ")]);
		if (brief.length) {
			layout.gap(8);
			layout.label("Дополнения к анкете");
			layout.rows(brief);
		}
	}
}
function closingPage(layout) {
	const { doc, model } = layout;
	layout.base({ final: true });
	flower(doc, 637, 290, 172, {
		opacity: .15,
		fill: "#ECEBDF",
		stroke: "#B3B9A5"
	});
	flower(doc, 80, 197, 25, {
		fill: PALETTE.paper,
		stroke: PALETTE.olive
	});
	layout.wrapped("Ваше путешествие\nначинается с деталей.", 61, 247, 525, {
		font: "title",
		size: 36,
		color: PALETTE.olive,
		leading: 41
	});
	const ending = model.mode === "review" ? "Проверьте содержание, маршрут и условия. После согласования подготовьте клиентскую версию предложения." : model.quote ? "Обсудите детали с вашим менеджером. После согласования предложения агентство перейдёт к оформлению путешествия." : "Обсудите выбранную идею с вашим менеджером. Следующий шаг - проверка стоимости и наличия на ваши даты.";
	layout.wrapped(ending, 64, 361, 488, {
		size: 11,
		color: PALETTE.muted,
		leading: 17
	});
	layout.text(`Подготовлено ${displayDate(model.generatedAt)}`, 64, 474, {
		size: 8.5,
		color: PALETTE.taupe
	});
}
/**
* Render a sanitized proposal snapshot to a real, self-contained PDF.
* @param {object} model Authorized immutable snapshot from proposal-data.mjs.
* @param {{assetRoot:string}} options Local release asset directory.
* @returns {Promise<Buffer>}
*/
async function renderProposalPdf(model, { assetRoot } = {}) {
	if (!assetRoot) throw new TypeError("PDF assetRoot is required.");
	if (!model?.journey || !model?.input || !["review", "client"].includes(model.mode)) throw new TypeError("Invalid PDF proposal snapshot.");
	const assets = await assetsAt(assetRoot);
	const doc = new PDFDocument({
		autoFirstPage: false,
		bufferPages: true,
		compress: true,
		font: assets.body,
		margin: 0,
		pdfVersion: "1.7",
		info: {
			Title: `Tiare Travel - ${safeText(model.journey.title)}`,
			Author: "Tiare Travel",
			Subject: model.mode === "review" ? "Рабочее предложение для менеджера" : "Индивидуальное предложение путешествия",
			Creator: "Tiare Travel",
			Producer: "Tiare Travel PDF",
			CreationDate: new Date(model.generatedAt || Date.now())
		}
	});
	doc.registerFont("title", assets.title).registerFont("italic", assets.italic).registerFont("body", assets.body);
	const chunks = [];
	let bytes = 0;
	const done = new Promise((resolvePromise, reject) => {
		doc.on("data", (chunk) => {
			bytes += chunk.length;
			if (bytes > MAX_BYTES) {
				doc.destroy(Object.assign(/* @__PURE__ */ new Error("PDF превышает 12 МБ. Сократите повторяющиеся описания перед экспортом."), { status: 413 }));
				return;
			}
			chunks.push(chunk);
		});
		doc.once("end", () => resolvePromise(Buffer.concat(chunks)));
		doc.once("error", reject);
	});
	try {
		const layout = makeLayout(doc, model);
		coverPage(layout, assets);
		overviewPages(layout);
		programPages(layout);
		hotelPages(layout);
		logisticsPages(layout);
		mapPages(layout, assets.geography);
		costPages(layout);
		preparationPages(layout);
		closingPage(layout);
		layout.footers();
		doc.end();
	} catch (error) {
		doc.destroy(error);
	}
	return done;
}
//#endregion
//#region server/app.mjs
var pdfInProgress = false;
var maxPdfBytes = 16 * 1024 * 1024;
var types = {
	".html": "text/html; charset=utf-8",
	".js": "text/javascript; charset=utf-8",
	".css": "text/css; charset=utf-8",
	".json": "application/json; charset=utf-8",
	".svg": "image/svg+xml",
	".webp": "image/webp",
	".png": "image/png",
	".jpg": "image/jpeg",
	".jpeg": "image/jpeg",
	".ico": "image/x-icon",
	".woff": "font/woff",
	".woff2": "font/woff2",
	".txt": "text/plain; charset=utf-8"
};
var clientError = (status, message) => Object.assign(new Error(message), { status });
function securityHeaders(response) {
	response.setHeader("X-Content-Type-Options", "nosniff");
	response.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
	response.setHeader("X-Frame-Options", "DENY");
	response.setHeader("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
	response.setHeader("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; font-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'");
}
function sendJson(response, status, body) {
	response.writeHead(status, {
		"Content-Type": "application/json; charset=utf-8",
		"Cache-Control": "no-store"
	});
	response.end(JSON.stringify(body));
}
async function requestJson(request) {
	if (!String(request.headers["content-type"] || "").toLowerCase().startsWith("application/json")) throw clientError(415, "Нужен запрос JSON.");
	if (Number(request.headers["content-length"] || 0) > 15e4) throw clientError(413, "Слишком большой запрос.");
	let size = 0;
	const chunks = [];
	for await (const chunk of request) {
		size += chunk.length;
		if (size > 15e4) throw clientError(413, "Слишком большой запрос.");
		chunks.push(chunk);
	}
	try {
		return JSON.parse(Buffer.concat(chunks).toString("utf8"));
	} catch {
		throw clientError(400, "Не удалось прочитать запрос.");
	}
}
function verifyOrigin(request, env) {
	if (request.headers["sec-fetch-site"] === "cross-site") throw clientError(403, "Запрос с другого сайта отклонён.");
	const origin = request.headers.origin;
	if (!origin || origin !== appUrl(env)) throw clientError(403, "Источник запроса не совпадает с адресом сервиса. Обновите страницу.");
}
async function serveStatic(request, response, url, root) {
	let pathname;
	try {
		pathname = decodeURIComponent(url.pathname);
	} catch {
		throw clientError(400, "Некорректный адрес.");
	}
	if (pathname.includes("\0") || pathname.split("/").some((part) => part.startsWith("."))) throw clientError(404, "Страница не найдена.");
	let file = resolve(root, "." + pathname);
	if (!file.startsWith(root + sep) && file !== root) throw clientError(404, "Страница не найдена.");
	try {
		if (!(await stat(file)).isFile()) file = resolve(root, "index.html");
	} catch {
		if (extname(pathname)) throw clientError(404, "Файл не найден.");
		file = resolve(root, "index.html");
	}
	const publicName = relative(root, file).split(sep).join("/");
	if (!([
		"index.html",
		"favicon.svg",
		"lagoon.webp",
		"robots.txt",
		"tiare-flower.svg",
		"tiare-wordmark.svg",
		"FONT_LICENSES.txt"
	].includes(publicName) || /^tiare-display(?:-italic)?-(?:latin|cyrillic)\.woff2$/.test(publicName) || /^index-[A-Za-z0-9_-]+\.(js|css)$/.test(publicName) || /^assets\/[A-Za-z0-9_./-]+$/.test(publicName) && [
		".js",
		".css",
		".svg",
		".webp",
		".png",
		".jpg",
		".jpeg",
		".woff",
		".woff2"
	].includes(extname(publicName)))) throw clientError(404, "Файл не найден.");
	try {
		const [rootReal, fileReal] = await Promise.all([realpath(root), realpath(file)]);
		if (!fileReal.startsWith(rootReal + sep)) throw clientError(404, "Файл не найден.");
		const content = await readFile(fileReal);
		response.writeHead(200, {
			"Content-Type": types[extname(file)] || "application/octet-stream",
			"Cache-Control": file.endsWith("index.html") ? "no-cache" : "public, max-age=3600"
		});
		response.end(request.method === "HEAD" ? void 0 : content);
	} catch (error) {
		if (error.status) throw error;
		throw clientError(503, "Интерфейс ещё не собран.");
	}
}
async function createApp({ env = process.env, database, providers, startOutbox = true, pdfRenderer = renderProposalPdf } = {}) {
	appUrl(env);
	const db = database || await createDatabase(env), outbox = createOutbox(db, env, providers), staticRoot = resolve(env.STATIC_DIR || resolve(process.cwd(), "dist"));
	const server = createServer(async (request, response) => {
		securityHeaders(response);
		try {
			const url = new URL(request.url || "/", "http://service.invalid");
			if (url.pathname === "/healthz" && request.method === "GET") {
				await db.query("SELECT 1 AS alive");
				sendJson(response, 200, {
					status: "ok",
					database: "connected"
				});
				return;
			}
			if (url.pathname.startsWith("/api/")) {
				response.setHeader("Cache-Control", "no-store");
				if (!["GET", "POST"].includes(request.method || "")) throw clientError(405, "Метод не поддерживается.");
				if (request.method === "POST") verifyOrigin(request, env);
				const session = await sessionForRequest(db, request, response, env);
				const pdfRoute = url.pathname.match(/^\/api\/requests\/([^/]+)\/proposal\.pdf$/);
				if (pdfRoute) {
					requireManager(session);
					if (request.method !== "GET") throw clientError(405, "PDF доступен только для просмотра.");
					const model = await loadProposalModel(db, session, parseProposalQuery(pdfRoute[1], url.searchParams));
					if (pdfInProgress) {
						response.setHeader("Retry-After", "5");
						throw clientError(429, "Другой PDF ещё формируется. Повторите через несколько секунд.");
					}
					pdfInProgress = true;
					try {
						const pdf = await pdfRenderer(model, { assetRoot: staticRoot });
						if (!Buffer.isBuffer(pdf) || pdf.length > maxPdfBytes || !pdf.subarray(0, 5).equals(Buffer.from("%PDF-"))) throw clientError(503, "Не удалось подготовить PDF. Попробуйте повторить позже.");
						if ((await db.query("SELECT revision FROM travel_requests WHERE id=?", [model.id])).rows[0]?.revision !== model.revision) throw clientError(409, "Заявка изменилась во время подготовки PDF. Обновите её и повторите выгрузку.");
						if (model.quote && Date.parse(model.quote.validUntil) <= Date.now()) throw clientError(409, "Срок подтверждённой цены истёк во время подготовки PDF. Повторите выгрузку: устаревшая цена будет убрана.");
						response.writeHead(200, {
							"Content-Type": "application/pdf",
							"Content-Length": pdf.length,
							"Content-Disposition": `inline; filename="TiareTravel-${model.id.slice(0, 8)}-${model.mode}-r${model.revision}.pdf"`,
							"Cache-Control": "no-store"
						});
						response.end(pdf);
					} finally {
						pdfInProgress = false;
					}
					return;
				}
				if (url.pathname === "/api/workspace" && request.method === "GET") {
					sendJson(response, 200, await readWorkspace(db, session, env));
					return;
				}
				if (url.pathname === "/api/auth/login" && request.method === "POST") {
					const body = await requestJson(request);
					if (typeof body?.password !== "string" || body.password.length > 1e3) throw clientError(400, "Укажите пароль.");
					await managerLogin(db, request, response, env, session, body.password);
					sendJson(response, 200, {
						ok: true,
						role: "manager"
					});
					return;
				}
				if (url.pathname === "/api/auth/logout" && request.method === "POST") {
					await requestJson(request);
					await managerLogout(db, response, env, session);
					sendJson(response, 200, {
						ok: true,
						role: "client"
					});
					return;
				}
				if (url.pathname === "/api/workspace" && request.method === "POST") {
					sendJson(response, 200, await mutateWorkspace(db, session, env, outbox, await requestJson(request)));
					if (startOutbox) outbox.trigger();
					return;
				}
				throw clientError(404, "Запрос не найден.");
			}
			if (!["GET", "HEAD"].includes(request.method || "")) throw clientError(405, "Метод не поддерживается.");
			await serveStatic(request, response, url, staticRoot);
		} catch (error) {
			if (response.headersSent) {
				response.end();
				return;
			}
			const status = Number.isInteger(error?.status) ? error.status : 503;
			sendJson(response, status, { error: status >= 500 && !error?.status ? "Сервис временно недоступен. Ваши сохранённые данные остаются в базе." : error.message });
		}
	});
	server.requestTimeout = 15e3;
	server.headersTimeout = 1e4;
	server.keepAliveTimeout = 5e3;
	if (startOutbox) outbox.start();
	let closed = false;
	return {
		server,
		db,
		outbox,
		async close() {
			if (closed) return;
			closed = true;
			await outbox.stop();
			if (server.listening) {
				server.closeIdleConnections();
				await new Promise((resolve, reject) => server.close((e) => e ? reject(e) : resolve()));
			}
			await db.close();
		}
	};
}
async function listenApp(env = process.env) {
	const app = await createApp({ env });
	const port = Number(env.PORT || 3e3);
	if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("Некорректный PORT.");
	await new Promise((resolve, reject) => {
		app.server.once("error", reject);
		app.server.listen(port, "0.0.0.0", resolve);
	});
	console.info(`Tiare Travel: порт ${port}; база ${app.db.dialect}; почта ${env.MAIL_MODE || "test"}.`);
	const shutdown = () => {
		app.close().then(() => process.exit(0), () => process.exit(1));
	};
	process.once("SIGTERM", shutdown);
	process.once("SIGINT", shutdown);
	return app;
}
//#endregion
//#region server/index.mjs
try {
	await listenApp();
} catch {
	console.error("Tiare Travel не запущен. Проверьте DATABASE_URL и параметры среды. Секреты в журнал не выводятся.");
	process.exitCode = 1;
}
//#endregion
export {};
