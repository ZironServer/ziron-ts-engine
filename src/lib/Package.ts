/*
Author: Ing. Luca Gian Scaringella
GitHub: LucaCode
Copyright(c) Ing. Luca Gian Scaringella
 */

/**
 * A package contains a string header packet
 * followed by an optional binary content packet.
 * Also, it has some internal used properties.
 */
export type Package = [string,ArrayBuffer?] & {
    /**
     * @description
     * Used to set the ack timeout.
     * Do not override or use this property.
     * @internal
     */
    _afterSend?: () => void;
    /**
     * @description
     * Called when the package is dropped from the buffer without being sent.
     * Do not override or use this property.
     * @internal
     */
    _onDrop?: () => void;
    /**
     * @description
     * The size of the package (used internally).
     * Do not override or use this property.
     * @internal
     */
    _size?: number;
};

export type InvokePackage<T = any> = Package & {
    promise: Promise<T>
}