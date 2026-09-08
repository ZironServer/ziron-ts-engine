/*
Author: Ing. Luca Gian Scaringella
GitHub: LucaCode
Copyright(c) Ing. Luca Gian Scaringella
 */

import {Package} from "./Package";

export const EMPTY_FUNCTION = () => {};

export type Writable<T> = { -readonly [P in keyof T]: T[P] };
export const RESOLVED_PROMISE = Object.freeze(Promise.resolve()) as Promise<void>;

export const MAX_UINT_32 = 4294967295;
export const MAX_SUPPORTED_ARRAY_BUFFER_SIZE = MAX_UINT_32 - 1;

export type CorkFunction = (callback: () => void) => void;
export type GroupCorkFunction = (group: string,callback: () => void) => void;

export type GroupSendFunction = (group: string,msg: ArrayBuffer | string, binary?: boolean, batch?: boolean) => void;
export type SendFunction = (msg: ArrayBuffer | string, binary?: boolean, batch?: boolean) => void;
export type MultiSendFunction = (messages: (ArrayBuffer | string)[],batches: boolean) => void;

/**
 * @description
 * Sets the not specified options by loading them from the default options.
 * Notice that no copy will be made, and the returned value is the options object.
 * @param options
 * @param defaults
 */
export function loadDefaults<T>(options: Partial<T>,defaults: T): T {
    for (const key in defaults) {
        //explicitly undefined options must not override the defaults
        if(options[key] === undefined) options[key] = defaults[key];
    }
    return options as T;
}

/**
 * Estimates the max UTF-8 string size.
 * @param str
 */
export function estimateMaxUTF8Size(str: string) {
    return str.length * 4;
}

/**
 * @description
 * Escape a string for building JSON content.
 * It is used to escape quotes from receiver/procedure names.
 * @param str
 */
export function escapeJSONString(str: string): string {
    if(str.indexOf('"') === -1) return str;
    return JSON.stringify(str).slice(1,-1);
}

/**
 * @description
 * Escapes reserved placeholder char sequence for streams and binary content.
 * @param key
 */
export function escapePlaceholderSequence(key: string): string {
    if(key.indexOf("_") === -1) return key;
    return key.replace('_b','__b')
            .replace("_s","__s");
}

/**
 * @description
 * Unescapes reserved placeholder char sequence for streams and binary content.
 * @param key
 */
export function unescapePlaceholderSequence(key: string): string {
    if(key.indexOf("_") === -1) return key;
    return key.replace('__b','_b')
        .replace("__s","_s");
}

/**
 * @description
 * Utility function to send a package.
 * @param pack
 * @param send
 */
export function sendPackage(pack: Package, send: SendFunction) {
    send(pack[0]);
    if(pack.length > 1) send(pack[1]!,true);
}