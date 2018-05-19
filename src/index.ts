import { createFromHexString, generate } from "bson-objectid";
import * as IORedis from "ioredis";
import * as isIterable from "is-iterable";
import { Signal } from "micro-signals";

const SETIFLESS = `local c = tonumber(redis.call('get', KEYS[1])); if c then if tonumber(ARGV[1]) < c then
redis.call('set', KEYS[1], ARGV[1]) return c - tonumber(ARGV[1]) else return 0 end else
redis.call('set', KEYS[1], ARGV[1]) return -1 end`;

export class Flow {
  /**
   * Currently processing [number] queues.
   */
  public readonly hasActiveQueues: number;
  /**
   * Default timeout for async methods
   */
  public readonly hasTimeout: number;
  /**
   * Max size for packed requests.
   */
  public readonly hasMaxPacketSize: number;
  /**
   * Max concurring queues.
   */
  public readonly hasConcurrency: number;

  /**
   * Dispatched if any errors occurr.
   */
  public readonly onError: Signal<any>;
  /**
   * Dispatched when flow is paused.
   */
  public readonly onPause: Signal<void>;
  /**
   * Dispatched when redis is connected.
   */
  public readonly onReady: Signal<void>;
  /**
   * Dispatched when flow is resumed.
   */
  public readonly onResume: Signal<void>;
  /**
   * Dispatched when redis connection is closing.
   */
  public readonly onQuit: Signal<void>;

  /**
   * Indicates that requests are currently being processed.
   */
  public readonly isActive: boolean;
  /**
   * Inidates that the instance is in a paused state.
   */
  public readonly isPaused: boolean;
  /**
   * Indicates that the instance is ready for use.
   */
  public readonly isReady: boolean;

  private __registered: Map<string, MiddlewareArray<any, any>>;
  private __responses: Map<string, (err?: any, value?: any) => void>;
  private __handles: Map<string, (message?: string) => any>;

  private __active: Set<string>;
  private __queue: string[];

  private __redis: IRedis;
  private __sub: IORedis.Redis;

  private __concurrency: number;
  private __ready: boolean;
  private __readyPromise?: Promise<void>;
  private __pause: boolean;
  private __pausePromise?: Promise<void>;

  constructor(options?: IFlowOptions);
  constructor({
    concurrency = 1,
    flowing = false,
    maxsize = 2000,
    prefix: keyPrefix = 'rqm:',
    uri = 'redis://localhost:6379/0',
    timeout = 1000,
  }: IFlowOptions = {}) {
    Object.defineProperties(this, {
      hasActiveQueues: {
        get(this: Flow) {
          return this.__active.size;
        }
      },
      hasConcurrency: {
        get(this: Flow) {
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
        get(this: Flow) {
          return Boolean(this.__active.size);
        },
      },
      isPaused: {
        get(this: Flow) {
          return this.__pause;
        },
      },
      onError: {
        value: new Signal(),
        writable: false,
      },
      onPause: {
        value: new Signal(),
        writable: false,
      },
      onReady: {
        value: new Signal(),
        writable: false,
      },
      onResume: {
        value: new Signal(),
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
      new Promise<IRedis>((resolve, reject) => {
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
          resolve(connection as IRedis);
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
      this.__redis = redis;
      this.__sub = sub;
      delete this.__readyPromise;
      setImmediate(() => {
        try {
          this.onReady.dispatch(void 0);
        } catch (err) {
          this.onError.dispatch(err);
        }
      });
      sub.on('message', (channel, message) => this.__handles.get(channel)(message));

      // Attach default handlers
      await this.attachMulti(
        ['request', (message) => this.handleRequest(message)],
        ['response', (message) => this.handleResponse(message)],
        ['advertise', (message) => this.handleAdvertisement(message)],
        ['stats', (message) => this.handleStats(message)],
        ['implemented', (message) => this.handleImplemented(message)],
      );
    });
  }

  /**
   * Change max concurring queue processing.
   * @param value New value
   */
  public concurrency(value: number): this {
    if (typeof value === "number" && value >= 0) {
      this.__concurrency = value;
    }
    return this;
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

      this.__registered.set(name, handlers as MiddlewareArray<T, U>);
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
    return this.__registered.delete(name);
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
    const value = await Promise.race<U>([
      new Promise<U>((resolve, reject) => this.__responses.set(id, (err: any, val?: U) =>
        err ? reject(err) : resolve(val))),
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
  public async pop(name: string): Promise<boolean> {
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

  /**
   * Lists names of available queues in redis.
   * @param timeout Advetisement timeout in milliseconds
   */
  public availableQueues(): Promise<string[]>;
  public availableQueues(timeout: number): Promise<string[]>;
  public async availableQueues(timeout?: number): Promise<string[]> {
    // Wait for connection if not ready
    if (this.__readyPromise) {
      await this.__readyPromise;
    }

    if ('number' !== typeof timeout) {
      timeout = this.hasTimeout;
    }

    // Generate id for timeout period
    const id = generate(Date.now() + timeout);

    await Promise.all([
      this.__redis.publish('advertise', id),
      wait(timeout),
    ]);

    const prefixed = this.prefixId(id);

    const [[_, list]]: [[any, string[]]] = await this.__redis.multi()
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
    const id = generate(Date.now() + timeout);

    const [value = false] = await Promise.all([
      Promise.race([
        new Promise<boolean>((resolve, reject) => this.__responses.set(id, () =>
          resolve(true))),
        wait(timeout),
      ]),
      this.__redis.publish('implemented', `${queue};${id}`),
    ]);

    return value;
  }

  /**
   * Get combined stats for current running instances
   *
   *
   * @param name
   */
  public stats(name: string): Promise<IFlowStats>;
  public stats(name: string, timeout: number): Promise<IFlowStats>;
  public async stats(queue: string, timeout?: number): Promise<IFlowStats> {
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

    const [
      [_1, workers = 0],
      [_2, total_received = 0],
      [_3, created = -1],
      [_4, in_queue = 0],
    ]: [[any, number], [any, number], [any, number], [any, number], [any, number]] = await this.__redis.multi()
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

  public createQueue<T, U>(name: string) {
    return new Queue(name, this);
  }

  /**
   * Wait till instance is connected and ready for use.
   *
   * @returns Resolves when ready
   */
  public async ready(): Promise<void> {
    if (this.__ready) {
      return;
    }

    return this.__readyPromise;
  }

  /**
   * Pauses flow. Resolves when flow is fully paused.
   */
  public async pause(): Promise<void> {
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
    return this.__pausePromise = new Promise<void>((resolve) => this.onPause.addOnce(() => {
      delete this.__pausePromise;
      resolve();
    }));
  }

  /**
   * Resumes flow.
   */
  public async resume(): Promise<void> {
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
      } catch (err) {
        this.onError.dispatch(err);
      }
    });
  }

  /**
   * Gracefully exit.
   */
  public async quit(): Promise<void> {
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
    } catch (err) {
      this.onError.dispatch(void 0);
    }
  }

  /**
   * Publish message on redis channel.
   * @param channel Channel name
   * @param message Message
   * @returns Message received by client(-s).
   */
  public async publish(channel: string, message: string): Promise<boolean> {
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
  public async subscribe(channel: string, handler: (message?: string) => any): Promise<boolean> {
    return this.attachMulti([channel, handler]);
  }

  private async attachMulti(...entries: Array<[string, (message: string) => any]>): Promise<boolean> {
    const handles = new Map(entries);

    if (!this.__handles) {
      this.__handles = handles;
    } else {
      handles.forEach((v, k) => this.__handles.set(k, v));
    }

    // Wait for connection if not ready
    if (this.__readyPromise) {
      await this.__readyPromise;
    }

    return this.__handles.size <= await this.__sub.subscribe(...handles.keys());
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
  private async handleRequest<T, U>(handle?: string): Promise<void> {
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
          await this.parseQueueData<T, U>(queue, data);
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
        } catch (err) {
          this.onError.dispatch(err);
        }
      });
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
    const context: IContext<T, U> = {
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

  private handleResponse(payload: string) {
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
      } else {
        const err = new Error(error);
        setImmediate(() => handler(err));
      }
    } else {
      setImmediate(async() => {
        try {
          const [[, message]]: [[null, string]] = await this.__redis.multi()
            .get(this.prefixId(id))
            .del(this.prefixId(id))
            .exec()
          ;
          const data = JSON.parse(message);

          handler(null, data);
        } catch (err) {
          handler(err);
        }
      });
    }
  }

  private async handleAdvertisement(id: string) {
    const timeout = createFromHexString(id).getTimestamp().getTime();

    // Don't advertise after timeout
    if (Date.now() > timeout) {
      return;
    }

    await this.__redis.sadd(id, ...this.__registered.keys());
  }

  private async handleStats(payload: string = '') {
    const [name, timeout_string] = payload.split(';');
    const timeout = safeParseInt(timeout_string, 0);

    if (Date.now() >= timeout) {
      return;
    }

    const queue = this.__registered.get(name);

    (this.__redis.multi() as IPipeline)
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

    if (this.__registered.has(name)) {
      this.__redis.publish("response", `${id};`);
    }
  }
}

export class Queue<T, U> {
  public readonly name: string;
  public readonly parent: Flow;

  constructor(name: string, parent: Flow) {
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

  public async pop(): Promise<boolean> {
    return this.parent.pop(this.name);
  }

  public async push(value?: T): Promise<U> {
    return this.parent.push<T, U>(this.name, value);
  }
}

export interface IFlowOptions {
  uri?: string;
  prefix?: string;
  maxsize?: number;
  timeout?: number;
  concurrency?: number;
  flowing?: boolean;
}

export interface IFlowStats {
  delay: number;
  maxsize: number;
  queue_name: string;
  first_created: Date;
  in_queue: number;
  total_sent: number;
  total_received: number;
  workers: number;
}

export interface IContext<T, U = any> {
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

export type Middleware<T, U> = (context: IContext<T, U>, next: () => Promise<any>) => any;

interface MiddlewareArray<T, U> extends Array<Middleware<T, U>> {
  name: string;
  active: boolean;
  received: number;
  created: number;
}

interface IRedis extends IORedis.Redis {
  setlt(key: string, value: number): Promise<number>;
  rpopBuffer(key: string): Promise<Buffer | null>;
}

interface IPipeline extends IORedis.Pipeline {
  setlt(key: string, value: number): this;
}

function safeParseInt(source: string, default_value: number) {
  const value = parseInt(source, 10);
  return Number.isNaN(value) ? default_value : value;
}

function wait(timeout: number): Promise<never> {
  return new Promise((resolve) => setTimeout(() => resolve(void 0), timeout));
}
