/*
Author: Ing. Luca Gian Scaringella
GitHub: LucaCode
Copyright(c) Ing. Luca Gian Scaringella
 */

import {NEXT_BINARIES_PACKET_TOKEN, PacketType} from "./Protocol";
import {Package} from "./Package";
import {estimateMaxUTF8Size, loadDefaults, MultiSendFunction} from "./Utils";
import {InsufficientBufferSizeError} from "./Errors";

/**
 * Runs a callback at the end of the current event loop turn:
 * setImmediate in Node.js (after the I/O callbacks of the turn), a zero timeout elsewhere.
 */
const scheduleTurnEnd: (callback: () => void) => any = typeof setImmediate === 'function' ?
    setImmediate : (callback) => setTimeout(callback,0);
const cancelTurnEnd: (handle: any) => void = typeof clearImmediate === 'function' ?
    clearImmediate : (handle) => clearTimeout(handle);

export interface PackageBufferOptions {
    /**
     * @description
     * The maximum buffer size in bytes.
     * The maximum size will limit the available space to buffer packages in the buffer.
     * When a new package would exceed the buffer size,
     * the buffer will be flushed automatically.
     * When it is not possible to flush the buffer duo to a not open state,
     * an InsufficientBufferSizeError will be thrown.
     * Notice that the stringSizeDeterminer is used to determine the
     * UTF-8 string encoded byte size of string packets.
     * By default, estimateMaxUTF8Size is used for performance reasons but
     * can be replaced with a function that determines the byte size specific.
     * @default Number.POSITIVE_INFINITY
     */
    maxBufferSize: number;
    /**
     * @description
     * The maximum buffer chunk length.
     * When flushing a quite full buffer, the packages will be grouped in chunks.
     * The maximum count of packages in a chunk is specified with this option.
     * For each chunk, a batch will be created and sent.
     * @default 200
     */
    maxBufferChunkLength: number;
    /**
     * @description
     * The limit length of a string (text packets with JSON content) batch.
     * When the buffer creates a text packet batch,
     * it stops adding string packets when the limit gets exceeded.
     * @default 310000
     */
    limitBatchStringLength: number;
    /**
     * @description
     * The limit size of a binary batch (in bytes).
     * When the buffer creates a binary batch,
     * it stops adding packets when the limit gets exceeded.
     * @default 3145728 (3 MB)
     */
    limitBatchBinarySize: number;
    /**
     * @description
     * Used to find out the UTF-8 byte size of a string to detect if the buffer space is enough.
     * Defaults to estimateMaxUTF8Size for performance reasons but can be
     * replaced with a function that determines the byte size specific.
     * @default estimateMaxUTF8Size
     */
    stringSizeDeterminer: (str: string) => number;
}

/**
 * Class for buffering packages and send them batched together.
 * When the underlying source has a temporarily closed state,
 * the buffer should be flushed on a reopening.
 */
export default class PackageBuffer {

    /**
     * @internal
     */
    public afterFlush?: () => void;

    constructor(
        public send: MultiSendFunction,
        /**
         * @description
         * This function indicates if the underlying source is open.
         * Notice that when this function returns false,
         * it is important to flush the buffer when the source opens.
         */
        public isOpen: () => boolean = () => true,
        /**
         * Notice that the provided options will not be cloned to save memory and performance.
         */
        public options: PackageBufferOptions = {...PackageBuffer.DEFAULT_OPTIONS}
    ) {}

    private _buffer: Package[] = [];
    private _bufferSize: number = 0;

    private _bufferTimeoutDelay: number | undefined;
    private _bufferTimeoutTicker: NodeJS.Timeout | undefined;
    private _bufferTimeoutTimestamp: number | undefined;
    private _bufferTurnTicker: any;

    public static readonly DEFAULT_OPTIONS: Readonly<PackageBufferOptions> = {
        maxBufferSize: Number.POSITIVE_INFINITY,
        maxBufferChunkLength: 200,
        limitBatchStringLength: 310000,
        limitBatchBinarySize: 3145728,
        stringSizeDeterminer: estimateMaxUTF8Size
    }

    public static buildOptions(options: Partial<PackageBufferOptions>): PackageBufferOptions {
        return loadDefaults(options,PackageBuffer.DEFAULT_OPTIONS);
    }

    /**
     * @description
     * Returns the count of packages in the buffer.
     */
    public getBufferLength(): number {
        return this._buffer.length;
    }

    /**
     * @description
     * Returns the buffer size.
     */
    public getBufferSize(): number {
        return this._bufferSize;
    }

    /**
     * @description
     * Clears the buffer.
     */
    public clearBuffer() {
        const dropped = this._buffer;
        this._buffer = [];
        this._bufferSize = 0;
        this._clearBufferTimeout();
        for(let i = 0; i < dropped.length; i++) {
            if(dropped[i]._onDrop) {
                try {dropped[i]._onDrop!();}
                catch (_) {}
            }
        }
    }

    private _onBatchTimeout = () => {
        this._bufferTimeoutTicker = undefined;
        if(this.isOpen()) this._flushBuffer();
    }

    private _onBatchTurnEnd = () => {
        this._bufferTurnTicker = undefined;
        if(this.isOpen()) this._flushBuffer();
    }

    /**
     * Adds the size of a package as meta information.
     * @param pack
     * @private
     */
    private _addPackageSize(pack: Package) {
        pack._size = this.options.stringSizeDeterminer(pack[0]);
        for(let i = pack.length - 1; i > 0; i--)
            pack._size += (pack[i] as ArrayBuffer).byteLength;
    }

    /**
     * @description
     * Adds a new package to the buffer.
     * Optionally a batch parameter can be provided to specify
     * the maximum time a package should wait in the buffer.
     * Whenever the lowest time of all packages in the buffer is reached,
     * the buffer gets flushed.
     * A batch of 0 flushes the buffer at the end of the current event loop turn,
     * so all packages added within the same turn are sent together without waiting for a timer.
     * When the parameter is not provided or has a true value,
     * flushing depends on other packages or manually flushing the buffer.
     * Notice when the underlying source is not open,
     * the buffer will not be flushed and should be flushed on reopening.
     * When the new package would exceed the buffer size,
     * the buffer will be flushed automatically.
     * When it is not possible to flush the buffer in such a state,
     * an InsufficientBufferSizeError will be thrown.
     * @param pack
     * @param batch
     */
    public add(pack: Package, batch?: number | true) {
        this._addPackageSize(pack);
        if(this._bufferSize + pack._size! > this.options.maxBufferSize) {
            //would max out buffer..
            if(this.isOpen()) {
                //Flush with new package directly.
                this._pushToBuffer(pack);
                this._internalFlushBuffer();
            }
            else throw new InsufficientBufferSizeError("PackageBuffer");
        }
        else {
            this._pushToBuffer(pack);
            if(typeof batch === "number") this._setBatchTime(batch);
        }
    }

    /**
     * @description
     * Tries to remove a package from the buffer if it is not already sent.
     * The returned boolean indicates if it was successfully removed.
     * @param pack
     */
    public tryRemove(pack: Package): boolean {
        const index = this._buffer.indexOf(pack);
        if(index !== -1) {
            this._buffer.splice(index,1);
            this._bufferSize -= pack._size!;
            if(this._buffer.length === 0) this._clearBufferTimeout();
            return true;
        }
        return false;
    }

    private _pushToBuffer(pack: Package) {
        this._buffer.push(pack);
        this._bufferSize += pack._size!;
    }

    private _setBatchTime(time: number) {
        if(time <= 0) {
            if(this._bufferTurnTicker === undefined)
                this._bufferTurnTicker = scheduleTurnEnd(this._onBatchTurnEnd);
            return;
        }
        if(this._bufferTurnTicker !== undefined) return;
        if(this._bufferTimeoutTicker) {
            if(((this._bufferTimeoutDelay! - Date.now()) + this._bufferTimeoutTimestamp!) > time){
                clearTimeout(this._bufferTimeoutTicker);
                this._setBufferTimeout(time);
            }
        }
        else this._setBufferTimeout(time);
    }

    private _clearBufferTimeout() {
        if(this._bufferTimeoutTicker) {
            clearTimeout(this._bufferTimeoutTicker);
            this._bufferTimeoutTicker = undefined;
        }
        if(this._bufferTurnTicker !== undefined) {
            cancelTurnEnd(this._bufferTurnTicker);
            this._bufferTurnTicker = undefined;
        }
    }

    private _setBufferTimeout(ms: number) {
        this._bufferTimeoutTicker = setTimeout(this._onBatchTimeout,ms);
        this._bufferTimeoutTimestamp = Date.now();
        this._bufferTimeoutDelay = ms;
    }

    /**
     * Clears the current buffer batch time ticker.
     * It can be used when the underlying source gets closed.
     */
    public clearBatchTime() {
        this._clearBufferTimeout();
    }

    /**
     * @description
     * Flushes the buffer when the underlying source is open;
     * otherwise, it returns false.
     */
    public flushBuffer(): boolean {
        if(!this.isOpen()) return false;
        this._flushBuffer();
        return true;
    }

    private _flushBuffer() {
        this._clearBufferTimeout();
        this._internalFlushBuffer();
    }

    private _internalFlushBuffer() {
        if(this._buffer.length < 1) return;
        const packages = this._buffer;
        this._buffer = [];
        this._bufferSize = 0;
        if(packages.length <= this.options.maxBufferChunkLength) this._sendBufferChunk(packages);
        else {
            const chunkLength = this.options.maxBufferChunkLength;
            for (let i = 0,len = packages.length; i < len; i += chunkLength)
                this._sendBufferChunk(packages.slice(i, i + chunkLength))
        }
        if(this.afterFlush) this.afterFlush();
    }

    private _sendBufferChunk(packages: Package[]) {
        const batchPackage = this._batchCompress(packages), listLength = packages.length;
        this.send(batchPackage,batchPackage.length < packages.length);
        for(let i = 0; i < listLength; i++) if(packages[i]._afterSend) packages[i]._afterSend!();
    }

    // noinspection JSMethodCanBeStatic
    /**
     * @description
     * The created batch will keep the order of packages.
     * @param packages
     * @private
     */
    private _batchCompress(packages: Package[]): (string|ArrayBuffer)[] {
        if(packages.length < 1) return [];
        else if(packages.length === 1) return packages[0] as (string|ArrayBuffer)[];

        const batchPackets: (string|ArrayBuffer)[] = [], len = packages.length;
        let tmpStringPacket: string = "",tmpBinaryPackets: ArrayBuffer[] = [],
            tmpPackage: Package, bundleHasBinary: boolean = false;

        for(let i = 0; i < len; i++) {
            tmpPackage = packages[i];
            if((bundleHasBinary && tmpPackage.length === 1) ||
                (tmpStringPacket.length > 0 &&
                    (tmpStringPacket.length + tmpPackage[0].length) > this.options.limitBatchStringLength))
            {
                batchPackets.push(PacketType.Bundle +
                    ',[' + tmpStringPacket.substring(0, tmpStringPacket.length - 1) + ']');
                batchPackets.push(...this._createBinaryContentBatches(tmpBinaryPackets));
                tmpBinaryPackets = [];
                tmpStringPacket = '';
                bundleHasBinary = false;
            }
            tmpStringPacket += ('[' + tmpPackage[0] + '],');
            if(tmpPackage.length > 1) {
                tmpBinaryPackets.push(tmpPackage[1]!);
                bundleHasBinary = true;
            }
        }
        batchPackets.push(PacketType.Bundle +
            ',[' + tmpStringPacket.substring(0, tmpStringPacket.length - 1) + ']');
        batchPackets.push(...this._createBinaryContentBatches(tmpBinaryPackets));
        return batchPackets;
    }

    private _createBinaryContentBatches(binaries: ArrayBuffer[]): ArrayBuffer[] {
        if(binaries.length === 0) return [];
        const packets: ArrayBuffer[] = [], len = binaries.length;
        let size = 1, packetsBatch: ArrayBuffer[] = [], binary: ArrayBuffer;
        for(let i = 0; i < len; i++) {
            binary = binaries[i];
            size += binary.byteLength - 1;
            packetsBatch.push(binary);
            if(size > this.options.limitBatchBinarySize && packetsBatch.length > 0) {
                packets.push(PackageBuffer._createBinaryContentBatch(packetsBatch));
                size = 1;
                packetsBatch = [];
            }
            else size += 4;
        }
        if(packetsBatch.length > 0) packets.push(PackageBuffer._createBinaryContentBatch(packetsBatch));
        return packets;
    }

    private static _createBinaryContentBatch(binaries: ArrayBuffer[]): ArrayBuffer {
        const length = binaries.length;
        if(length < 2) {
            if(length === 1) return binaries[0];
            else throw new Error("Can not batch empty binary content packets array.");
        }
        let size = 1 + (length * 4 - 4), i: number, bi = 1;
        for(i = 0; i < length; i++) size += binaries[i].byteLength - 1;
        const dataView = new DataView(new ArrayBuffer(size));
        dataView.setInt8(0,PacketType.BinaryContent);
        const uint8PacketView = new Uint8Array(dataView.buffer);
        for(i = 0; i < length; i++) {
            uint8PacketView.set(new Uint8Array(binaries[i],1),bi);
            bi += binaries[i].byteLength - 1;
            if(i + 1 < length) {
                dataView.setUint32(bi,NEXT_BINARIES_PACKET_TOKEN);
                bi += 4;
            }
        }
        return dataView.buffer;
    }
}