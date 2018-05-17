
// Polyfill module
declare module "bson-objectid" {
  export interface ObjectID {
    toHexString(): string;
    getTimestamp(): Date;
    equals<T = ObjectID>(any: T): boolean;
  }
  export function generate(time?: number): string;
  export function createFromHexString(string: string): ObjectID;
}

declare module "is-iterable" {
  function isIterable<T>(iterable: any): iterable is Iterable<T> | IterableIterator<T>;
  namespace isIterable {

  }
  export = isIterable;
}
