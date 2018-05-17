import { createFromHexString, generate } from "bson-objectid";
import * as IORedis from "ioredis";
import * as isIterable from "is-iterable";
import { Signal } from "micro-signals";

const SETIFLESS = `local c = tonumber(redis.call('get', KEYS[1])); if c then if tonumber(ARGV[1]) < c then
redis.call('set', KEYS[1], ARGV[1]) return c - tonumber(ARGV[1]) else return 0 end else
redis.call('set', KEYS[1], ARGV[1]) return -1 end`;

export class Flow {
  /**
   * Default timeout for async methods
   */
  public readonly timeout: number;
  /**
   * Max size for packed requests
   */
  public readonly maxsize: number;

  /**
   * Max concurring queues. May be reconfigurred
   */
  public concurrency: number;

  /**
   * Silenced errors land here.
   */
  public onError: Signal<Error>;

  private _implements: Map<string, MiddlewareArray<any, any>>;
  private _responses: Map<string, (err?: any, value?: any) => void>;
  private _handles: Map<string, (message?: string) => void>;

  private _active: Set<string>;
  private _queue: string[];

  private _redis: ExtendedRedis;
  private _sub: IORedis.Redis;

  private _ready?: Promise<void>;
  private _pause?: boolean | (() => void);

  constructor(options?: Options);
  constructor({
    concurrency = 3,
    flowing = false,
    maxsize = 2000,
    prefix: keyPrefix = 'rqm:',
    uri = 'redis://localhost:6379/0',
    timeout = 5000,
  }: Options = {}) {
    const promise = Promise.all([
      new Promise<ExtendedRedis>((resolve, reject) => {
        const connection = new IORedis(uri, {keyPrefix});
        const ref = setTimeout(reject, timeout, new Error('Connection to '));

        connection.defineCommand('setlt', {
          lua: SETIFLESS,
          numberOfKeys: 1,
        });

        connection.once('error', reject);
        connection.once('connect', () => {
          clearTimeout(ref);
          connection.removeListener('error', reject);
          resolve(connection as ExtendedRedis);
        });
      }),
      new Promise<IORedis.Redis>((resolve, reject) => {
        const connection = new IORedis(uri, {keyPrefix});
        const ref = setTimeout(reject, timeout, new Error('Connection timed out'));

        connection.once('error', reject);
        connection.once('connect', () => {
          clearTimeout(ref);
          connection.removeListener('error', reject);
          resolve(connection);
        });
      }),
    ]).then(async([redis, sub]) => {
      this._redis = redis;
      this._sub = sub;
      delete this._ready;

      sub.on('message', (channel, message) => this._handles.get(channel)(message));

      // Attach default handlers
      await this.attachMulti(
        ['request', (message) => this.handleRequest(message)],
        ['response', (message) => this.handleResponse(message)],
        ['advertise', (message) => this.handleAdvertisement(message)],
        ['stats', (message) => this.handleStats(message)],
        ['implemented', (message) => this.handleImplemented(message)],
      );
    });

    this.maxsize = maxsize;
    this.timeout = timeout;
    this.onError = new Signal();

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
  get active(): boolean {
    return Boolean(this._active.size);
  }

  /**
   * Currently proccessing x request queues
   */
  get activeSize(): number {
    return this._active.size;
  }

  /**
   * Check if flowing mode is enabled.
   */
  get flowing(): boolean {
    return !this._pause;
  }

  /**
   * Register a single queue middleware.
   *
   * @template T Request type
   * @template U Response type
   * @param name Queue name
   * @param handler Middleware
   */
  public register<T = any, U = any>(
    name: string,
    handler: Middleware<T, U>,
  ): this;
  /**
   * Register multiple queue middleware supplied as an array.
   *
   * @template T Request type
   * @template U Response type
   * @param name Queue name
   * @param handlers Midddleware
   */
  public register<T = any, U = any>(
    name: string,
    handlers: Iterable<Middleware<T, U>> | IterableIterator<Middleware<T, U>>,
  ): this;
  /**
   * Register multiple queue middleware supplied as rest parameters.
   *
   * @template T Request type
   * @template U Response type
   * @param name Queue name
   * @param handlers Middleware
   */
  public register<T = any, U = any>(
    name: string,
    ...handlers: Array<Middleware<T, U>>
  ): this;
  public register<T = any, U = any>(
    name: string,
    handler: Middleware<T, U> | Iterable<Middleware<T, U>> | IterableIterator<Middleware<T, U>>,
    ...handlers: Array<Middleware<T, U>>
  ): this {
    if (!name || name.length === 0) {
      throw new TypeError('Name cannot be empty');
    }

    if (isIterable(handler)) {
      handlers = Array.from(handler);
    } else {
      handlers.splice(0, 0, handler);
    }

    if (handlers.length) {
      (handlers as MiddlewareArray<T, U>).name = name;
      (handlers as MiddlewareArray<T, U>).active = false;
      (handlers as MiddlewareArray<T, U>).created = Date.now();
      (handlers as MiddlewareArray<T, U>).received = 0;

      this._implements.set(name, handlers as MiddlewareArray<T, U>);
    }

    return this;
  }

  /**
   * Unregister middleware for queue.
   *
   * @param name Queue name
   * @returns Unregister successful
   */
  public unregister(name: string): boolean {
    return this._implements.delete(name);
  }

  public push<T = void>(name: string): Promise<T>;
  public push<T = any, U = void>(name: string, pack: T): Promise<U>;
  public push<T = any, U = void>(name: string, pack: T, timeout: number): Promise<U>;
  public async push<T, U>(name: string, pack?: T, timeout?: number): Promise<U> {
    if (!name) {
      throw new Error('Name cannot be empty');
    }

    const date = Date.now();
    const id = generate(date);
    let message = id;

    // Only append message if not undefined
    if (pack !== undefined) {
      try {
        message += `;${JSON.stringify(pack)}`;
      } catch (e) {
        const err: Partial<Error & {inner: Error}> = new Error('Cannot pack message');
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
    const value = await Promise.race<U>([
      new Promise<U>((resolve, reject) => this._responses.set(id, (err: any, val?: U) =>
        err ? reject(err) : resolve(val))),
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
  public async pop(name: string): Promise<boolean> {
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

  /**
   * Lists names of available queues in redis.
   * @param timeout Advetisement timeout in milliseconds
   */
  public availableQueues(): Promise<string[]>;
  public availableQueues(timeout: number): Promise<string[]>;
  public async availableQueues(timeout?: number): Promise<string[]> {
    // Wait for connection if not ready
    if (this._ready) {
      await this._ready;
    }

    if ('number' !== typeof timeout) {
      timeout = this.timeout > 0 ? this.timeout : 10;
    }

    // Generate id for timeout period
    const id = generate(Date.now() + timeout);

    await Promise.all([
      this._redis.publish('advertise', id),
      wait(timeout),
    ]);

    const prefixed = this.prefixId(id);

    const [list]: [string[]] = await this._redis.multi()
      .smembers(prefixed)
      .del(prefixed)
      .exec()
    ;

    return list;
  }

  public has(name: string): Promise<boolean>;
  public has(name: string, timeout: number): Promise<boolean>;
  public async has(queue: string, timeout?: number): Promise<boolean> {
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
    const id = generate(Date.now() + timeout);

    const [value = false] = await Promise.all([
      Promise.race([
        new Promise<boolean>((resolve, reject) => this._responses.set(id, () =>
          resolve(true))),
        wait(timeout),
      ]),
      this._redis.publish('implemented', `${queue};${id}`),
    ]);

    return value;
  }

  /**
   * Get combined stats for current running instances
   *
   *
   * @param name
   */
  public stats(name: string): Promise<Stats>;
  public stats(name: string, timeout: number): Promise<Stats>;
  public async stats(queue: string, timeout?: number): Promise<Stats> {
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

    const [
      workers = 0,
      total_received = 0,
      created = -1,
      in_queue = 0,
    ]: [number, number, number, number, number] = await this._redis.multi()
      .get(this.prefixQueue(queue, 'coun'))
      .get(this.prefixQueue(queue, 'recv'))
      .get(this.prefixQueue(queue, 'crea'))
      .llen(this.prefixQueue(queue))
      .del(this.prefixQueue(queue, 'coun'))
      .del(this.prefixQueue(queue, 'recv'))
      .del(this.prefixQueue(queue, 'crea'))
      .exec()
    ;

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
  public ready(): Promise<void> {
    if (!this._ready) {
      return Promise.resolve();
    }

    return this._ready;
  }

  /**
   * Pauses flow. Resolves when flow is fully paused.
   */
  public async pause(): Promise<void> {
    if (this._pause) {
      return;
    }

    if (this._ready || !this._active.size) {
      this._pause = true;

      return;
    }

    // Wait till all queues are stopped
    return new Promise<void>((resolve) => this._pause = () => {
      resolve();
      this._pause = true;
    });
  }

  /**
   * Resumes flow.
   */
  public async resume(): Promise<void> {
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
  public async quit(): Promise<void> {
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
  public async publish(channel: string, message: string): Promise<boolean> {
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
  public async subscribe(channel: string, handler: (message?: string) => any): Promise<boolean> {
    return this.attachMulti([channel, handler]);
  }

  private async attachMulti(...entries: Array<[string, (message: string) => any]>): Promise<boolean> {
    const handles = new Map(entries);

    if (!this._handles) {
      this._handles = handles;
    } else {
      handles.forEach((v, k) => this._handles.set(k, v));
    }

    // Wait for connection if not ready
    if (this._ready) {
      await this._ready;
    }

    return this._handles.size <= await this._sub.subscribe(...handles.keys());
  }

  private prefixQueue(queue: string): string;
  private prefixQueue(queue: string, ...strings: string[]): string;
  private prefixQueue(...strings: string[]): string {
    return `queue:${strings.join(':')}`;
  }

  private prefixId(id: string): string;
  private prefixId(id: string, ...strings: string[]): string;
  private prefixId(...strings: string[]): string {
    return `id:${strings.join(':')}`;
  }

  /**
   * Process requests in an async series till stack is empty
   */
  private async handleRequest<T, U>(handle: string | false): Promise<void> {
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
          await this.parseQueueData<T, U>(queue, data);
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

  private async parseQueueData<T, U>(queue: MiddlewareArray<T, U>, data: string): Promise<void> {
    let count = 0;
    const length = queue.length;

    const received = new Date();
    const [id, body] = data.split(';');
    let payload = id;
    const sent = createFromHexString(id).getTimestamp();
    const request = body ? JSON.parse(body) : undefined;
    const context: Context<T, U> = {
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
      } else {
        const message = JSON.stringify(context.response);

        multi.set(this.prefixId(id), message);
      }
    } catch (err) {
      const error: Partial<Error & {inner: Error}> =
        new Error(`Could not pack response for queue '${queue.name}' (id: ${id})`);
      error.inner = err;

      this.onError.dispatch(error as Error);
      payload += `;${err && err.message || 'No message'}`;
    }

    await multi.publish('response', payload).exec();

    async function dispatch(index: number) {
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

  private async handleResponse(payload: string) {
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
      } else {
        const err = new Error(error);

        handler(err);
      }
    } else {
      try {
        const [[, message]]: [[null, string]] = await this._redis.multi()
          .get(this.prefixId(id))
          .del(this.prefixId(id))
          .exec()
        ;
        const data = JSON.parse(message);

        handler(null, data);
      } catch (err) {
        handler(err);
      }
    }
  }

  private async handleAdvertisement(id: string) {
    const timeout = createFromHexString(id).getTimestamp().getTime();

    // Don't advertise after timeout
    if (Date.now() > timeout) {
      return;
    }

    await this._redis.sadd(id, ...this._implements.keys());
  }

  private async handleStats(payload: string = '') {
    const [name, timeout_string] = payload.split(';');
    const timeout = safeParseInt(timeout_string, 0);

    if (Date.now() >= timeout) {
      return;
    }

    const queue = this._implements.get(name);

    (this._redis.multi() as ExtendedPipeline)
      .setlt(this.prefixQueue(name, 'crea'), queue.created)
      .incrby(this.prefixQueue(name, 'recv'), queue.received)
      .incr(this.prefixQueue(name, 'coun'))
      .exec()
    ;
  }

  private async handleImplemented(payload: string = '') {
    const [name, id] = payload.split(";");
    const timeout = createFromHexString(id).getTimestamp().getTime();
    if (Date.now() >= timeout) {
      return;
    }

    if (this._implements.has(name)) {
      this._redis.publish("response", `${id};`);
    }
  }
}

export interface Options {
  uri?: string;
  prefix?: string;
  maxsize?: number;
  timeout?: number;
  concurrency?: number;
  flowing?: boolean;
}

export interface Stats {
  delay: number;
  maxsize: number;
  queue_name: string;
  first_created: Date;
  in_queue: number;
  total_sent: number;
  total_received: number;
  workers: number;
}

export interface Context<T, U = any> {
  // Response body
  response?: U;
  // Request body
  request: T;
  // Time sent
  sent: Date;
  // Time received
  received: Date;
  // Queue name
  queue: string;
}

export type Middleware<T, U> = (context: Context<T, U>, next: () => Promise<any>) => any;

interface MiddlewareArray<T, U> extends Array<Middleware<T, U>> {
  name: string;
  active: boolean;
  received: number;
  created: number;
}

interface ExtendedRedis extends IORedis.Redis {
  setlt(key: string, value: number): Promise<number>;
  rpopBuffer(key: string): Promise<Buffer | null>;
}

interface ExtendedPipeline extends IORedis.Pipeline {
  setlt(key: string, value: number): this;
}

function safeParseInt(source: string, default_value: number) {
  const value = parseInt(source, 10);
  return Number.isNaN(value) ? default_value : value;
}

function wait(timeout: number): Promise<never> {
  return new Promise((resolve) => setTimeout(() => resolve(void 0), timeout));
}
