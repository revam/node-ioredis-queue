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
    constructor({ concurrency = 1, flowing = false, maxsize = 2000, prefix: keyPrefix = 'rqm:', uri = 'redis://localhost:6379/0', timeout = 1000, } = {}) {
        Object.defineProperties(this, {
            hasActiveQueues: {
                get() {
                    return this.__active.size;
                }
            },
            hasConcurrency: {
                get() {
                    return this.__concurrency;
                }
            },
            hasMaxPacketSize: {
                value: maxsize,
                writable: false,
            },
            hasTimeout: {
                value: timeout > 0 ? timeout : 1000,
                writable: false,
            },
            isActive: {
                get() {
                    return Boolean(this.__active.size);
                },
            },
            isPaused: {
                get() {
                    return this.__pause;
                },
            },
            onError: {
                value: new micro_signals_1.Signal(),
                writable: false,
            },
            onPause: {
                value: new micro_signals_1.Signal(),
                writable: false,
            },
            onReady: {
                value: new micro_signals_1.Signal(),
                writable: false,
            },
            onResume: {
                value: new micro_signals_1.Signal(),
                writable: false,
            },
        });
        this.__concurrency = concurrency;
        this.__pause = !flowing;
        this.__queue = [];
        this.__active = new Set();
        this.__responses = new Map();
        this.__registered = new Map();
        this.__readyPromise = Promise.all([
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
            this.__redis = redis;
            this.__sub = sub;
            delete this.__readyPromise;
            setImmediate(() => {
                try {
                    this.onReady.dispatch(void 0);
                }
                catch (err) {
                    this.onError.dispatch(err);
                }
            });
            sub.on('message', (channel, message) => this.__handles.get(channel)(message));
            // Attach default handlers
            await this.attachMulti(['request', (message) => this.handleRequest(message)], ['response', (message) => this.handleResponse(message)], ['advertise', (message) => this.handleAdvertisement(message)], ['stats', (message) => this.handleStats(message)], ['implemented', (message) => this.handleImplemented(message)]);
        });
    }
    /**
     * Change max concurring queue processing.
     * @param value New value
     */
    concurrency(value) {
        if (typeof value === "number" && value >= 0) {
            this.__concurrency = value;
        }
        return this;
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
            this.__registered.set(name, handlers);
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
        return this.__registered.delete(name);
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
        if (message.length > this.hasMaxPacketSize) {
            throw new Error('Packed message exceeds max size');
        }
        // Wait for connection if not ready
        if (this.__readyPromise) {
            await this.__readyPromise;
        }
        if ('number' !== typeof timeout) {
            timeout = this.hasTimeout;
        }
        await this.__redis.pipeline()
            .lpush(this.prefixQueue(name), message)
            .publish('request', name)
            .exec();
        // Will return undefined if timeout finished first
        const value = await Promise.race([
            new Promise((resolve, reject) => this.__responses.set(id, (err, val) => err ? reject(err) : resolve(val))),
            // Timeout after x milliseconds
            wait(timeout),
        ]);
        // Delete callback
        this.__responses.delete(id);
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
        if (!this.__pause) {
            return false;
        }
        if (this.__registered.has(name)) {
            return false;
        }
        // Wait for connection if not ready
        if (this.__readyPromise) {
            await this.__readyPromise;
        }
        const data = await this.__redis.rpop(this.prefixQueue(name));
        if (data) {
            const queue = this.__registered.get(name);
            await this.parseQueueData(queue, data);
        }
        return Boolean(data);
    }
    async availableQueues(timeout) {
        // Wait for connection if not ready
        if (this.__readyPromise) {
            await this.__readyPromise;
        }
        if ('number' !== typeof timeout) {
            timeout = this.hasTimeout;
        }
        // Generate id for timeout period
        const id = bson_objectid_1.generate(Date.now() + timeout);
        await Promise.all([
            this.__redis.publish('advertise', id),
            wait(timeout),
        ]);
        const prefixed = this.prefixId(id);
        const [[_, list]] = await this.__redis.multi()
            .smembers(prefixed)
            .del(prefixed)
            .exec();
        return list;
    }
    async has(queue, timeout) {
        // Wait for connection if not ready
        if (this.__readyPromise) {
            await this.__readyPromise;
        }
        if (this.__registered.has(queue)) {
            return true;
        }
        if ('number' !== typeof timeout) {
            timeout = this.hasTimeout;
        }
        // Generate id for timeout period
        const id = bson_objectid_1.generate(Date.now() + timeout);
        const [value = false] = await Promise.all([
            Promise.race([
                new Promise((resolve, reject) => this.__responses.set(id, () => resolve(true))),
                wait(timeout),
            ]),
            this.__redis.publish('implemented', `${queue};${id}`),
        ]);
        return value;
    }
    async stats(queue, timeout) {
        // Wait for connection if not ready
        if (this.__readyPromise) {
            await this.__readyPromise;
        }
        if ('number' !== typeof timeout) {
            timeout = this.hasTimeout;
        }
        await Promise.all([
            this.__redis.publish('report', `${queue};${Date.now() + timeout}`),
            wait(timeout),
        ]);
        const [[_1, workers = 0], [_2, total_received = 0], [_3, created = -1], [_4, in_queue = 0],] = await this.__redis.multi()
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
                delay: this.hasTimeout,
                first_created: created >= 0 ? new Date(created) : undefined,
                in_queue,
                maxsize: this.hasMaxPacketSize,
                queue_name: queue,
                total_received,
                total_sent: total_received + in_queue,
                workers,
            };
        }
    }
    createQueue(name) {
        return new Queue(name, this);
    }
    /**
     * Wait till instance is connected and ready for use.
     *
     * @returns Resolves when ready
     */
    async ready() {
        if (this.__ready) {
            return;
        }
        return this.__readyPromise;
    }
    /**
     * Pauses flow. Resolves when flow is fully paused.
     */
    async pause() {
        if (this.__pausePromise) {
            return this.__pausePromise;
        }
        if (this.__pause) {
            return;
        }
        this.__pause = true;
        if (!this.__active.size) {
            return;
        }
        // Wait till all queues are stopped
        return this.__pausePromise = new Promise((resolve) => this.onPause.addOnce(() => {
            delete this.__pausePromise;
            resolve();
        }));
    }
    /**
     * Resumes flow.
     */
    async resume() {
        if (!this.__pause) {
            return;
        }
        this.__pause = false;
        // Resume if stack is not empty
        if (this.__queue.length) {
            this.handleRequest();
        }
        setImmediate(() => {
            try {
                this.onResume.dispatch(void 0);
            }
            catch (err) {
                this.onError.dispatch(err);
            }
        });
    }
    /**
     * Gracefully exit.
     */
    async quit() {
        // Wait till requests are finished
        await this.pause();
        // Then close redis connections
        await Promise.all([
            this.__redis.quit(),
            this.__sub.quit(),
        ]);
        // Inform third-parties through signal.
        try {
            this.onQuit.dispatch(void 0);
        }
        catch (err) {
            this.onError.dispatch(void 0);
        }
    }
    /**
     * Publish message on redis channel.
     * @param channel Channel name
     * @param message Message
     * @returns Message received by client(-s).
     */
    async publish(channel, message) {
        // Wait for connection if not ready
        if (this.__readyPromise) {
            await this.__readyPromise;
        }
        return 0 < await this.__redis.publish(channel, message);
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
        if (!this.__handles) {
            this.__handles = handles;
        }
        else {
            handles.forEach((v, k) => this.__handles.set(k, v));
        }
        // Wait for connection if not ready
        if (this.__readyPromise) {
            await this.__readyPromise;
        }
        return this.__handles.size <= await this.__sub.subscribe(...handles.keys());
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
        if (handle !== undefined) {
            if (!this.__registered.has(handle)) {
                return;
            }
            if (this.__active.has(handle)) {
                return;
            }
            if (!this.__queue.includes(handle)) {
                this.__queue.push(handle);
            }
        }
        if (this.__pause) {
            return;
        }
        if (this.__active.size >= this.__concurrency) {
            return;
        }
        let name = this.__queue.shift();
        while (name) {
            // We may have done some async work, so recheck value
            if (this.__active.size >= this.__concurrency) {
                this.__queue.unshift(name);
                break;
            }
            this.__active.add(name);
            let data;
            const queue = this.__registered.get(name);
            do {
                data = await this.__redis.rpop(this.prefixQueue(name));
                if (data) {
                    await this.parseQueueData(queue, data);
                }
                // We did some async work, so recheck pause state
                if (this.__pause) {
                    this.__queue.unshift(name);
                    break;
                }
            } while (data);
            this.__active.delete(name);
            if (this.__pause) {
                break;
            }
            name = this.__queue.shift();
        }
        if (!this.__active.size) {
            setImmediate(() => {
                try {
                    this.onPause.dispatch(void 0);
                }
                catch (err) {
                    this.onError.dispatch(err);
                }
            });
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
        const multi = this.__redis.multi();
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
    handleResponse(payload) {
        // T is not a hexadecimal value, so we (can) use it as a boolean.
        const [id, error] = payload.split(';');
        if (!this.__responses.has(id)) {
            return;
        }
        // Retrive handler
        const handler = this.__responses.get(id);
        this.__responses.delete(id);
        // Try parse message
        if (error !== undefined) {
            if (!error.length) {
                setImmediate(handler);
            }
            else {
                const err = new Error(error);
                setImmediate(() => handler(err));
            }
        }
        else {
            setImmediate(async () => {
                try {
                    const [[, message]] = await this.__redis.multi()
                        .get(this.prefixId(id))
                        .del(this.prefixId(id))
                        .exec();
                    const data = JSON.parse(message);
                    handler(null, data);
                }
                catch (err) {
                    handler(err);
                }
            });
        }
    }
    async handleAdvertisement(id) {
        const timeout = bson_objectid_1.createFromHexString(id).getTimestamp().getTime();
        // Don't advertise after timeout
        if (Date.now() > timeout) {
            return;
        }
        await this.__redis.sadd(id, ...this.__registered.keys());
    }
    async handleStats(payload = '') {
        const [name, timeout_string] = payload.split(';');
        const timeout = safeParseInt(timeout_string, 0);
        if (Date.now() >= timeout) {
            return;
        }
        const queue = this.__registered.get(name);
        this.__redis.multi()
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
        if (this.__registered.has(name)) {
            this.__redis.publish("response", `${id};`);
        }
    }
}
exports.Flow = Flow;
class Queue {
    constructor(name, parent) {
        Object.defineProperties(this, {
            name: {
                value: name,
                writable: false,
            },
            parent: {
                value: parent,
                writable: false,
            },
        });
    }
    async pop() {
        return this.parent.pop(this.name);
    }
    async push(value) {
        return this.parent.push(this.name, value);
    }
}
exports.Queue = Queue;
function safeParseInt(source, default_value) {
    const value = parseInt(source, 10);
    return Number.isNaN(value) ? default_value : value;
}
function wait(timeout) {
    return new Promise((resolve) => setTimeout(() => resolve(void 0), timeout));
}
//# sourceMappingURL=index.js.map