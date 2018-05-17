"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const bson_objectid_1 = require("bson-objectid");
const IORedis = require("ioredis");
const isIterable = require("is-iterable");
const micro_signals_1 = require("micro-signals");
const SETIFLESS = `local c = tonumber(redis.call('get', KEYS[1])); if c then if tonumber(ARGV[1]) < c then
redis.call('set', KEYS[1], ARGV[1]) return c - tonumber(ARGV[1]) else return 0 end else
redis.call('set', KEYS[1], ARGV[1]) return -1 end`;
class Flow {
    constructor({ concurrency = 3, flowing = false, maxsize = 2000, prefix: keyPrefix = 'rqm:', uri = 'redis://localhost:6379/0', timeout = 5000, } = {}) {
        const promise = Promise.all([
            new Promise((resolve, reject) => {
                const connection = new IORedis(uri, { keyPrefix });
                const ref = setTimeout(reject, timeout, new Error('Connection to '));
                connection.defineCommand('setlt', {
                    lua: SETIFLESS,
                    numberOfKeys: 1,
                });
                connection.once('error', reject);
                connection.once('connect', () => {
                    clearTimeout(ref);
                    connection.removeListener('error', reject);
                    resolve(connection);
                });
            }),
            new Promise((resolve, reject) => {
                const connection = new IORedis(uri, { keyPrefix });
                const ref = setTimeout(reject, timeout, new Error('Connection timed out'));
                connection.once('error', reject);
                connection.once('connect', () => {
                    clearTimeout(ref);
                    connection.removeListener('error', reject);
                    resolve(connection);
                });
            }),
        ]).then(async ([redis, sub]) => {
            this._redis = redis;
            this._sub = sub;
            delete this._ready;
            sub.on('message', (channel, message) => this._handles.get(channel)(message));
            // Attach default handlers
            await this.attachMulti(['request', (message) => this.handleRequest(message)], ['response', (message) => this.handleResponse(message)], ['advertise', (message) => this.handleAdvertisement(message)], ['stats', (message) => this.handleStats(message)], ['implemented', (message) => this.handleImplemented(message)]);
        });
        this.maxsize = maxsize;
        this.timeout = timeout;
        this.onError = new micro_signals_1.Signal();
        this._queue = [];
        this._ready = promise;
        this._pause = !flowing;
        this._active = new Set();
        this._responses = new Map();
        this._implements = new Map();
    }
    /**
     * Currently proccessing request queues
     */
    get active() {
        return Boolean(this._active.size);
    }
    /**
     * Currently proccessing x request queues
     */
    get activeSize() {
        return this._active.size;
    }
    /**
     * Check if flowing mode is enabled.
     */
    get flowing() {
        return !this._pause;
    }
    register(name, handler, ...handlers) {
        if (!name || name.length === 0) {
            throw new TypeError('Name cannot be empty');
        }
        if (isIterable(handler)) {
            handlers = Array.from(handler);
        }
        else {
            handlers.splice(0, 0, handler);
        }
        if (handlers.length) {
            handlers.name = name;
            handlers.active = false;
            handlers.created = Date.now();
            handlers.received = 0;
            this._implements.set(name, handlers);
        }
        return this;
    }
    /**
     * Unregister middleware for queue.
     *
     * @param name Queue name
     * @returns Unregister successful
     */
    unregister(name) {
        return this._implements.delete(name);
    }
    async push(name, pack, timeout) {
        if (!name) {
            throw new Error('Name cannot be empty');
        }
        const date = Date.now();
        const id = bson_objectid_1.generate(date);
        let message = id;
        // Only append message if not undefined
        if (pack !== undefined) {
            try {
                message += `;${JSON.stringify(pack)}`;
            }
            catch (e) {
                const err = new Error('Cannot pack message');
                err.inner = e;
                throw err;
            }
        }
        if (message.length > this.maxsize) {
            throw new Error('Packed message exceeds max size');
        }
        // Wait for connection if not ready
        if (this._ready) {
            await this._ready;
        }
        if ('number' !== typeof timeout) {
            timeout = this.timeout > 0 ? this.timeout : 1000;
        }
        await this._redis.pipeline()
            .lpush(this.prefixQueue(name), message)
            .publish('request', name)
            .exec();
        // Will return undefined if timeout finished first
        const value = await Promise.race([
            new Promise((resolve, reject) => this._responses.set(id, (err, val) => err ? reject(err) : resolve(val))),
            // Timeout after x milliseconds
            wait(timeout),
        ]);
        // Delete callback
        this._responses.delete(id);
        // We timed out
        if (!value && Date.now() >= date + timeout) {
            throw new Error('Request callback timed out');
        }
        return value;
    }
    /**
     * Pops queue if flow is paused.
     *
     * @param name Queue name
     * @returns Pop successful
     */
    async pop(name) {
        if (this.flowing) {
            return false;
        }
        if (this._implements.has(name)) {
            return false;
        }
        // Wait for connection if not ready
        if (this._ready) {
            await this._ready;
        }
        const data = await this._redis.rpop(this.prefixQueue(name));
        if (data) {
            const queue = this._implements.get(name);
            await this.parseQueueData(queue, data);
        }
        return Boolean(data);
    }
    async availableQueues(timeout) {
        // Wait for connection if not ready
        if (this._ready) {
            await this._ready;
        }
        if ('number' !== typeof timeout) {
            timeout = this.timeout > 0 ? this.timeout : 10;
        }
        // Generate id for timeout period
        const id = bson_objectid_1.generate(Date.now() + timeout);
        await Promise.all([
            this._redis.publish('advertise', id),
            wait(timeout),
        ]);
        const prefixed = this.prefixId(id);
        const [list] = await this._redis.multi()
            .smembers(prefixed)
            .del(prefixed)
            .exec();
        return list;
    }
    async has(queue, timeout) {
        // Wait for connection if not ready
        if (this._ready) {
            await this._ready;
        }
        if (this._implements.has(queue)) {
            return true;
        }
        if ('number' !== typeof timeout) {
            timeout = this.timeout > 0 ? this.timeout : 1000;
        }
        // Generate id for timeout period
        const id = bson_objectid_1.generate(Date.now() + timeout);
        const [value = false] = await Promise.all([
            Promise.race([
                new Promise((resolve, reject) => this._responses.set(id, () => resolve(true))),
                wait(timeout),
            ]),
            this._redis.publish('implemented', `${queue};${id}`),
        ]);
        return value;
    }
    async stats(queue, timeout) {
        // Wait for connection if not ready
        if (this._ready) {
            await this._ready;
        }
        if ('number' !== typeof timeout) {
            timeout = this.timeout > 0 ? this.timeout : 1000;
        }
        await Promise.all([
            this._redis.publish('report', `${queue};${Date.now() + timeout}`),
            wait(timeout),
        ]);
        const [workers = 0, total_received = 0, created = -1, in_queue = 0,] = await this._redis.multi()
            .get(this.prefixQueue(queue, 'coun'))
            .get(this.prefixQueue(queue, 'recv'))
            .get(this.prefixQueue(queue, 'crea'))
            .llen(this.prefixQueue(queue))
            .del(this.prefixQueue(queue, 'coun'))
            .del(this.prefixQueue(queue, 'recv'))
            .del(this.prefixQueue(queue, 'crea'))
            .exec();
        // Return stats if we found workers or queue is not empty
        if (workers || in_queue) {
            return {
                delay: this.timeout,
                first_created: created >= 0 ? new Date(created) : undefined,
                in_queue,
                maxsize: this.maxsize,
                queue_name: queue,
                total_received,
                total_sent: total_received + in_queue,
                workers,
            };
        }
    }
    /**
     * Wait till instance is connected and ready for use.
     *
     * @returns Resolves when ready
     */
    ready() {
        if (!this._ready) {
            return Promise.resolve();
        }
        return this._ready;
    }
    /**
     * Pauses flow. Resolves when flow is fully paused.
     */
    async pause() {
        if (this._pause) {
            return;
        }
        if (this._ready || !this._active.size) {
            this._pause = true;
            return;
        }
        // Wait till all queues are stopped
        return new Promise((resolve) => this._pause = () => {
            resolve();
            this._pause = true;
        });
    }
    /**
     * Resumes flow.
     */
    async resume() {
        if (!this._pause) {
            return;
        }
        this._pause = false;
        // Resume if stack is not empty
        if (this._queue.length) {
            this.handleRequest(false);
        }
    }
    /**
     * Gracefully exit.
     */
    async quit() {
        await Promise.all([
            this._redis.quit(),
            this._sub.quit(),
        ]);
    }
    /**
     * Publish message on redis channel.
     * @param channel Channel name
     * @param message Message
     * @returns Message received by client(-s).
     */
    async publish(channel, message) {
        // Wait for connection if not ready
        if (this._ready) {
            await this._ready;
        }
        return 0 < await this._redis.publish(channel, message);
    }
    /**
     * Subscribes to an extra channel on redis. All messages goes to handler.
     * @param channel Channel name
     * @param handler Handles messages for channel
     * @returns Subscribed to channel
     */
    async subscribe(channel, handler) {
        return this.attachMulti([channel, handler]);
    }
    async attachMulti(...entries) {
        const handles = new Map(entries);
        if (!this._handles) {
            this._handles = handles;
        }
        else {
            handles.forEach((v, k) => this._handles.set(k, v));
        }
        // Wait for connection if not ready
        if (this._ready) {
            await this._ready;
        }
        return this._handles.size <= await this._sub.subscribe(...handles.keys());
    }
    prefixQueue(...strings) {
        return `queue:${strings.join(':')}`;
    }
    prefixId(...strings) {
        return `id:${strings.join(':')}`;
    }
    /**
     * Process requests in an async series till stack is empty
     */
    async handleRequest(handle) {
        if (handle !== false) {
            if (!this._implements.has(handle)) {
                return;
            }
            if (this._active.has(handle)) {
                return;
            }
            if (!this._queue.includes(handle)) {
                this._queue.push(handle);
            }
        }
        if (this._pause) {
            return;
        }
        if (this._active.size === this.concurrency) {
            return;
        }
        let name = this._queue.shift();
        while (name) {
            this._active.add(name);
            let data;
            const queue = this._implements.get(name);
            do {
                data = await this._redis.rpop(this.prefixQueue(name));
                if (data) {
                    await this.parseQueueData(queue, data);
                }
                // We did some async work, so re-check pause state
                if (this._pause) {
                    break;
                }
            } while (data);
            this._active.delete(name);
            if (this._pause) {
                break;
            }
            name = this._queue.shift();
        }
        if (!this._active.size && this._pause && 'function' === typeof this._pause) {
            this._pause();
        }
    }
    async parseQueueData(queue, data) {
        let count = 0;
        const length = queue.length;
        const received = new Date();
        const [id, body] = data.split(';');
        let payload = id;
        const sent = bson_objectid_1.createFromHexString(id).getTimestamp();
        const request = body ? JSON.parse(body) : undefined;
        const context = {
            queue: queue.name,
            received,
            request,
            response: undefined,
            sent,
        };
        queue.received++;
        const multi = this._redis.multi();
        try {
            await dispatch(0);
            if (context.response === undefined) {
                payload += ';';
            }
            else {
                const message = JSON.stringify(context.response);
                multi.set(this.prefixId(id), message);
            }
        }
        catch (err) {
            const error = new Error(`Could not pack response for queue '${queue.name}' (id: ${id})`);
            error.inner = err;
            this.onError.dispatch(error);
            payload += `;${err && err.message || 'No message'}`;
        }
        await multi.publish('response', payload).exec();
        async function dispatch(index) {
            if (index < count) {
                throw new Error('next() called multiple times in registered flow handle');
            }
            count = index + 1;
            if (index >= length) {
                return;
            }
            await queue[index](context, () => dispatch(index + 1));
        }
    }
    async handleResponse(payload) {
        // T is not a hexadecimal value, so we (can) use it as a boolean.
        const [id, error] = payload.split(';');
        if (!this._responses.has(id)) {
            return;
        }
        // Retrive handler
        const handler = this._responses.get(id);
        this._responses.delete(id);
        // Try parse message
        if (error !== undefined) {
            if (!error.length) {
                handler();
            }
            else {
                const err = new Error(error);
                handler(err);
            }
        }
        else {
            try {
                const [[, message]] = await this._redis.multi()
                    .get(this.prefixId(id))
                    .del(this.prefixId(id))
                    .exec();
                const data = JSON.parse(message);
                handler(null, data);
            }
            catch (err) {
                handler(err);
            }
        }
    }
    async handleAdvertisement(id) {
        const timeout = bson_objectid_1.createFromHexString(id).getTimestamp().getTime();
        // Don't advertise after timeout
        if (Date.now() > timeout) {
            return;
        }
        await this._redis.sadd(id, ...this._implements.keys());
    }
    async handleStats(payload = '') {
        const [name, timeout_string] = payload.split(';');
        const timeout = safeParseInt(timeout_string, 0);
        if (Date.now() >= timeout) {
            return;
        }
        const queue = this._implements.get(name);
        this._redis.multi()
            .setlt(this.prefixQueue(name, 'crea'), queue.created)
            .incrby(this.prefixQueue(name, 'recv'), queue.received)
            .incr(this.prefixQueue(name, 'coun'))
            .exec();
    }
    async handleImplemented(payload = '') {
        const [name, id] = payload.split(";");
        const timeout = bson_objectid_1.createFromHexString(id).getTimestamp().getTime();
        if (Date.now() >= timeout) {
            return;
        }
        if (this._implements.has(name)) {
            this._redis.publish("response", `${id};`);
        }
    }
}
exports.Flow = Flow;
function safeParseInt(source, default_value) {
    const value = parseInt(source, 10);
    return Number.isNaN(value) ? default_value : value;
}
function wait(timeout) {
    return new Promise((resolve) => setTimeout(() => resolve(void 0), timeout));
}
//# sourceMappingURL=index.js.map