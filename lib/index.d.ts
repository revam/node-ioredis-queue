import { Signal } from "micro-signals";
export declare class Flow {
    /**
     * Default timeout for async methods
     */
    readonly timeout: number;
    /**
     * Max size for packed requests
     */
    readonly maxsize: number;
    /**
     * Max concurring queues. May be reconfigurred
     */
    concurrency: number;
    /**
     * Silenced errors land here.
     */
    onError: Signal<Error>;
    private _implements;
    private _responses;
    private _handles;
    private _active;
    private _queue;
    private _redis;
    private _sub;
    private _ready?;
    private _pause?;
    constructor(options?: Options);
    /**
     * Currently proccessing request queues
     */
    readonly active: boolean;
    /**
     * Currently proccessing x request queues
     */
    readonly activeSize: number;
    /**
     * Check if flowing mode is enabled.
     */
    readonly flowing: boolean;
    /**
     * Register a single queue middleware.
     *
     * @template T Request type
     * @template U Response type
     * @param name Queue name
     * @param handler Middleware
     */
    register<T = any, U = any>(name: string, handler: Middleware<T, U>): this;
    /**
     * Register multiple queue middleware supplied as an array.
     *
     * @template T Request type
     * @template U Response type
     * @param name Queue name
     * @param handlers Midddleware
     */
    register<T = any, U = any>(name: string, handlers: Iterable<Middleware<T, U>> | IterableIterator<Middleware<T, U>>): this;
    /**
     * Register multiple queue middleware supplied as rest parameters.
     *
     * @template T Request type
     * @template U Response type
     * @param name Queue name
     * @param handlers Middleware
     */
    register<T = any, U = any>(name: string, ...handlers: Array<Middleware<T, U>>): this;
    /**
     * Unregister middleware for queue.
     *
     * @param name Queue name
     * @returns Unregister successful
     */
    unregister(name: string): boolean;
    push<T = void>(name: string): Promise<T>;
    push<T = any, U = void>(name: string, pack: T): Promise<U>;
    push<T = any, U = void>(name: string, pack: T, timeout: number): Promise<U>;
    /**
     * Pops queue if flow is paused.
     *
     * @param name Queue name
     * @returns Pop successful
     */
    pop(name: string): Promise<boolean>;
    /**
     * Lists names of available queues in redis.
     * @param timeout Advetisement timeout in milliseconds
     */
    availableQueues(): Promise<string[]>;
    availableQueues(timeout: number): Promise<string[]>;
    has(name: string): Promise<boolean>;
    has(name: string, timeout: number): Promise<boolean>;
    /**
     * Get combined stats for current running instances
     *
     *
     * @param name
     */
    stats(name: string): Promise<Stats>;
    stats(name: string, timeout: number): Promise<Stats>;
    /**
     * Wait till instance is connected and ready for use.
     *
     * @returns Resolves when ready
     */
    ready(): Promise<void>;
    /**
     * Pauses flow. Resolves when flow is fully paused.
     */
    pause(): Promise<void>;
    /**
     * Resumes flow.
     */
    resume(): Promise<void>;
    /**
     * Gracefully exit.
     */
    quit(): Promise<void>;
    /**
     * Publish message on redis channel.
     * @param channel Channel name
     * @param message Message
     * @returns Message received by client(-s).
     */
    publish(channel: string, message: string): Promise<boolean>;
    /**
     * Subscribes to an extra channel on redis. All messages goes to handler.
     * @param channel Channel name
     * @param handler Handles messages for channel
     * @returns Subscribed to channel
     */
    subscribe(channel: string, handler: (message?: string) => any): Promise<boolean>;
    private attachMulti(...entries);
    private prefixQueue(queue);
    private prefixQueue(queue, ...strings);
    private prefixId(id);
    private prefixId(id, ...strings);
    /**
     * Process requests in an async series till stack is empty
     */
    private handleRequest<T, U>(handle);
    private parseQueueData<T, U>(queue, data);
    private handleResponse(payload);
    private handleAdvertisement(id);
    private handleStats(payload?);
    private handleImplemented(payload?);
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
    response?: U;
    request: T;
    sent: Date;
    received: Date;
    queue: string;
}
export declare type Middleware<T, U> = (context: Context<T, U>, next: () => Promise<any>) => any;
