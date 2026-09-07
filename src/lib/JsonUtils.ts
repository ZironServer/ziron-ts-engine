/*
Author: Ing. Luca Gian Scaringella
GitHub: LucaCode
Copyright(c) Ing. Luca Gian Scaringella
 */

export function createSafeJsonReplacer(): (key: string, value: any) => any {
    const seen = new WeakSet<object>();
    return (_, value) => {
        if(typeof value === "object" && value !== null) {
            if(seen.has(value)) return '[Circular]';
            seen.add(value);
        }
        return value;
    };
}
export function safeJsonStringify(value: any): string {
    return JSON.stringify(value,createSafeJsonReplacer());
}

export class JSONString extends String {}
const validJSONStartRegex = /^[ \n\r\t]*[{\[]/;

/**
 * Decodes the JSON data which was transmitted over
 * the wire to a JavaScript value.
 * @param str
 */
export function decodeJson(str: string): any {
    // Performance optimization to detect invalid JSON packet sooner.
    if (!validJSONStartRegex.test(str)) throw Error('Invalid JSON.');
    return JSON.parse(str);
}

/**
 * Encodes a raw JavaScript value into JSON for transferring it over the wire.
 * Supports also objects with circular dependencies.
 * @param value
 */
export function encodeJson(value: any): string {
    if(value instanceof JSONString) return value.toString();
    try {return JSON.stringify(value);}
    catch (_) {return safeJsonStringify(value);}
}