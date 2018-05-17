
// Polyfill module
declare module "bson-objectid" {
  export interface ObjectID {
    toHexString(): string;
    getTimestamp(): Date;
    equals(otherOID: ObjectID | string): boolean;
  }
  export function generate(time?: number): string;
  export function createFromHexString<T>(oid: string): ObjectID;
}

declare module "is-iterable" {
  function isIterable<T>(iterable: any): iterable is Iterable<T> | IterableIterator<T>;
  namespace isIterable {

  }
  export = isIterable;
}
