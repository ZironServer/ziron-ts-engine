/*
Author: Ing. Luca Gian Scaringella
GitHub: LucaCode
Copyright(c) Ing. Luca Gian Scaringella
 */

import {EMPTY_FUNCTION, Writable} from "../Utils";
import {StreamCloseCode} from "./StreamCloseCode";
import {StreamState} from "./StreamState";
import Transport from "../Transport";
import {DataType} from "../DataType";
import LinkedBuffer from "./LinkedBuffer";
import StreamCloseError from "./StreamCloseError";

interface ReadStreamOptions {
    /**
     * @description
     * The size of the buffer to buffer incoming chunks.
     * Minimum value is 1.
     * @default object mode: 8 or binary: 16384 (16KB)
     */
    bufferSize?: number;
    /**
     * @description
     * Whenever the stream is open, the buffer is empty, the WriteStream has permission,
     * and the ReadStream doesn't receive any new chunks in the given timeout,
     * the stream will be closed in an error state.
     * It helps to detect and close dead streams.
     * You can disable this timeout with null when you can trust the other side.
     * @default 5000
     */
    chunkTimeout?: number | null;
    /**
     * @description
     * Sets a size limit for the stream.
     * If the limit is exceeded, the stream will be closed with an error.
     * In binary mode, the limit is specified in bytes and in object mode in the count of objects.
     * When setting this option to null unlimited data
     * can be transmitted through the stream.
     * @default null
     */
    sizeLimit?: number | null;
}

export type ChunkMiddleware<T = any> = (chunk: any, updateChunk: (chunk: T) => void, type: DataType) => boolean | Promise<boolean>;

export default class ReadStream<T = any> {

    /**
     * @description
     * The current state of the stream.
     */
    public readonly state: StreamState = StreamState.Pending;

    /**
     * @description
     * Indicates if the ReadStream is accepted.
     */
    public readonly accepted: boolean;

    private readPromise: Promise<T> | null;
    private readResolve: ((chunk: any) => void) | null;

    /**
     * @description
     * Sets a size limit for the stream.
     * If the limit is exceeded, the stream will be closed with an error.
     * In binary mode, the limit is specified in bytes and in object mode in the count of objects.
     * When setting this option to null unlimited data
     * can be transmitted through the stream.
     * @default null
     */
    public set sizeLimit(sizeLimit: number | null) {
        this._sizeLimit = sizeLimit;
    }
    private _sizeLimit?: number | null;

    private _buffer: LinkedBuffer
    private _bufferSizeLimit: number;

    private _allowedSize: number;
    /**
     * @description
     * The size that the stream has processed.
     * This size contains chunks that are already pushed to the buffer or directly to the reader.
     * @private
     */
    private _processedSize: number;
    /**
     * @description
     * The size that the stream has received.
     * It can contain unprocessed chunks,
     * that potentially gets pushed to the buffer or directly to the reader.
     * This size indicator helps to detect size limit violations faster.
     * @private
     */
    private _receivedSize: number;

    private _chainEnd: boolean;
    private _chainCancel: boolean;
    private _chain: Promise<any>;
    private _chainChunkSize: number;

    private readonly _createdBadConnectionStamp: number;

    /*
    Timeout to close unused streams.
    The timeout needs to run when the buffer has no data,
    and no chunk is in the chain and when the chain is open.
    The timeout can be cleared when the buffer is not empty,
    or chunks are in the chain or the chain is closed.
     */
    private _chunkTimeoutActive: boolean;
    private _chunkTimeout: number;
    private _chunkTimeoutTick: NodeJS.Timeout;

    private _allowMoreMinTimeout: NodeJS.Timeout;

    /**
     * @description
     * The chunk middleware can be used to validate incoming stream chunks.
     * When false is returned, the stream will be closed immediately.
     * The middleware also allows changing incoming chunks.
     * It doesn't matter if you increase the size of the chunk in the middleware.
     * Only the size when the chunk was received is used.
     */
    public set chunkMiddleware(middleware: ChunkMiddleware<T> | undefined) {
        this._chunkMiddleware = middleware;
    }
    private _chunkMiddleware?: ChunkMiddleware<T>;

    private _closedPromiseResolve: (closeCode: StreamCloseCode | number) => void;

    /**
     * @description
     * A promise that gets resolved on stream closure with the close code.
     */
    public readonly closed: Promise<StreamCloseCode | number> = new Promise(resolve => this._closedPromiseResolve = resolve);

    private _openedPromiseResolve: () => void;

    /**
     * @description
     * A promise that gets resolved when the stream opens.
     * Notice the promise is still resolved when the stream has opened and closed.
     */
    public readonly opened: Promise<void> = new Promise(resolve => this._openedPromiseResolve = resolve);

    /**
     * @description
     * This property can be used to access the close code when the stream has been closed.
     */
    public readonly closeCode?: StreamCloseCode | number;

    private readonly _transport: Transport;

    constructor(public readonly id: number, transport: Transport) {
        Object.defineProperty(this, "_transport", {
            enumerable: false,
            writable: true
        });
        Object.defineProperty(this, "_createdBadConnectionStamp", {
            enumerable: false,
            writable: true
        });
        this._transport = transport;
        this._createdBadConnectionStamp = transport.badConnectionStamp;
    }

    /**
     * @description
     * Indicates if this is a binary stream.
     */
    get binary(): boolean {
        //negative id indicates binary stream
        return this.id < 0;
    }

    /**
     * @description
     * Indicates if this is a object stream.
     */
    public get object(): boolean {
        return !this.binary;
    }

    /**
     * @description
     * Returns if the ReadStream has been closed successfully (end close code).
     */
    get closedSuccessfully(): boolean {
        return this.state === StreamState.Closed && this.closeCode === StreamCloseCode.End;
    }

    /**
     * Accepts the stream.
     * @param options
     */
    accept({sizeLimit,bufferSize,chunkTimeout = 5000}: ReadStreamOptions = {}) {
        if(this.state !== StreamState.Pending)
            throw new Error("Cannot accept a not pending ReadStream.");

        if(this._createdBadConnectionStamp !== this._transport.badConnectionStamp) {
            //The connection was lost in-between time.
            //When the stream is not registered in the transporter,
            // it is needed because then the stream will not be notified of a connection lost.
            return this._emitBadConnection();
        }

        //init
        if(sizeLimit !== undefined) this.sizeLimit = sizeLimit;
        this._bufferSizeLimit = bufferSize ?
            Math.max(1,bufferSize) : (this.binary ? 16384 : 8);
        this._buffer = new LinkedBuffer();

        this._allowedSize = this._bufferSizeLimit;
        this._processedSize = 0;
        this._receivedSize = 0;

        this._chain = Promise.resolve();
        this._chainEnd = false;
        this._chainCancel = false;
        this._chainChunkSize = 0;

        this._transport._addReadStream(this.id,this);
        (this as Writable<ReadStream>).state = StreamState.Open;
        (this as Writable<ReadStream>).accepted = true;
        this._transport._sendStreamAccept(this.id,this._bufferSizeLimit);

        if(chunkTimeout != null) {
            this._chunkTimeout = chunkTimeout;
            this._chunkTimeoutActive = true;
            this._setChunkTimeout();
        }

        this._openedPromiseResolve();
    }

    /**
     * @description
     * Use this method to close the Write- and Read-Stream immediately with a close code.
     * The method is helpful to deny or abort a stream.
     * It can also be used to close an unlimited stream successfully.
     * Notice that the method can not be used when the ReadStream is already closed.
     * When a read chunk is invalid, you should not use this method to close the stream in an error state.
     * Instead, you should use the chunk middleware.
     * Because when the ReadStream has processed the last chunks successfully and
     * pushed them in the buffer, it closes without an error.
     * You can still read the chunks, but when you determine that a chunk is incompatible,
     * it is not possible to close the ReadStream with a code because it is already closed successfully.
     * @param closeCode
     */
    close(closeCode: StreamCloseCode | number) {
        if(this.state === StreamState.Closed) return;
        if(this._createdBadConnectionStamp !== this._transport.badConnectionStamp) {
            //The connection was lost in-between time.
            //When the stream is not registered in the transporter,
            // it is needed because then the stream will not be notified of a connection lost.
            return this._emitBadConnection();
        }
        this._transport._sendReadStreamClose(this.id,closeCode);
        this._close(closeCode);
    }

    private _checkAllowMore(applyMinimum?: boolean) {
        if(this._chainEnd || this.state !== StreamState.Open) return;
        /*
         Calculate the potential buffer size by adding the
         currently used buffer size and the size that can still be added.
         Subtract the potential buffer size from the limit to get the safe available free space.
         If the free space is greater than 20% of the buffer limit, allow the client to send more.
         After a specific time when the 20% mark is still not reached,
         the minor permission possible for safe free space is sent.

         Example:

         bufferLimit: 16
         currentBufferSize: 5

         Allowed: 40
         Processed: 38
         It is crucial to use the processedSize, not the received size,
         because it could contain data that still needs to be pushed to the buffer.
         But we only want the already processed size that will not be potentially added to the buffer size.

         potential buffer size: 5 + (40 - 38) = 7
         free space: 16 - 7 = 9

         free space >= (0.2 * 16 = 3.2): yes
         */
        const freeBufferSize = this._bufferSizeLimit - (this._buffer.size + (this._allowedSize - this._processedSize));
        if(freeBufferSize >= (applyMinimum ? 1 : (0.2 * this._bufferSizeLimit))) {
            this._allowedSize += freeBufferSize;
            this._transport._sendStreamAllowMore(this.id,freeBufferSize);
            clearTimeout(this._allowMoreMinTimeout);
        }
        else {
            clearTimeout(this._allowMoreMinTimeout);
            this._allowMoreMinTimeout = setTimeout(() => this._checkAllowMore(true),3000);
        }
    }

    async *[Symbol.asyncIterator] () {
        let chunk: T | null;
        while((chunk = await this.read()) !== null) yield chunk;
    }

    /**
     * @description
     * Reads a chunk from the stream.
     * The chunk is eventually returned from the buffer.
     * Otherwise, it waits for a new chunk from the WriteStream.
     * It returns null when the stream is closed and no more chunks are available.
     * Notice it can happen that the stream is already closed,
     * but the read method can still return chunks from the buffer.
     * So use the return value as an indicator and not the stream state.
     */
    read(): Promise<T | null> {
        if(!this.accepted) {
            if(this.state === StreamState.Pending) throw new Error("Accept ReadStream before start reading.");
            else {
                // ReadStream is closed before accepted
                return Promise.resolve(null);
            }
        }

        if(this.readPromise) return this.readPromise;
        else if(this._buffer.length > 0) {
            const chunk = this._buffer.shift();
            this._emptyChunksCheck();
            this._checkAllowMore();
            return Promise.resolve(chunk);
        }
        else if(this._chainChunkSize > 0 || !this._chainEnd)
            return this.readPromise = new Promise(r => this.readResolve = r);
        else return Promise.resolve(null);
    }

    /**
     * @description
     * Reads all chunks until no more chunks are available and the stream is closed.
     * Only use this method when the whole context is needed because
     * the complete data is buffered in the memory.
     * It is highly recommended to specify a sizeLimit in the accept method when using this method.
     * Internally it uses the read method,
     * so only use this method or either the read method.
     * The method throws a StreamCloseError when the stream has been closed with an abnormal close code.
     * @throws StreamCloseError
     */
    async readAll(): Promise<T[]> {
        const buffer: T[] = [];
        let chunk = await this.read();
        while (chunk != null) {
            buffer.push(chunk);
            chunk = await this.read();
        }
        if(this.closeCode !== StreamCloseCode.End)
            throw new StreamCloseError(this.closeCode!);
        return buffer;
    }

    private _setChunkTimeout() {
        if(!this._chunkTimeoutActive) return;
        this._clearChunkTimeout(); //for safety to not overwrite an already existing timeout
        this._chunkTimeoutTick =
            setTimeout(() => this.close(StreamCloseCode.ChunkTimeout), this._chunkTimeout);
    }

    private _emptyChunksCheck() {
        if(!this._chainEnd && this._buffer.length <= 0 && this._chainChunkSize <= 0)
            this._setChunkTimeout();
    }

    /**
     * @private
     */
    private _clearChunkTimeout() {
        clearTimeout(this._chunkTimeoutTick);
    }

    /**
     * @internal
     */
    _pushChunk(chunk: Promise<T> | ArrayBuffer, type: DataType) {
        //the chunk may be skipped, avoid an unhandled rejection
        if(!(chunk instanceof ArrayBuffer)) chunk.catch(EMPTY_FUNCTION);
        if(this.state === StreamState.Open && !this._chainEnd) {

            // The chunks in a binary stream are sent via binary
            // packets to resolving the data async is not needed.
            if(this.binary && !(chunk instanceof ArrayBuffer)) {
                this.close(StreamCloseCode.InvalidChunk);
                return this._transport.onInvalidMessage(new Error('Invalid stream chunk.'));
            }

            const size = this.binary ? (chunk as ArrayBuffer).byteLength : 1;

            //Skip empty binary chunks
            if(size <= 0) return;

            if(
                (this._sizeLimit != null && (this._receivedSize + size) > this._sizeLimit) ||
                (size + this._receivedSize > this._allowedSize)
            ) return this.close(StreamCloseCode.SizeLimitExceeded);

            this._receivedSize += size;

            this._chainChunkSize++;
            this._clearChunkTimeout();
            this._chain = this._chain.then(() => this._chainNextChunk(chunk, type, size));
        }
    }

    private async _chainNextChunk(chunk: Promise<T> | ArrayBuffer, type: DataType, size: number) {
        if(this._chainCancel) return;
        await this._processChunk(chunk,type,size);
        this._chainChunkSize--;
        this._emptyChunksCheck();
    }

    private async _processChunk(chunk: Promise<any> | ArrayBuffer, type: DataType, size: number) {
        try {
            chunk = await chunk;

            if(this._chainCancel) return;

            if(this._chunkMiddleware && !await this._chunkMiddleware(chunk, c => chunk = c as any,type)) {
                this.close(StreamCloseCode.InvalidChunk);
                return this._transport.onInvalidMessage(new Error('Invalid stream chunk.'));
            }

            if(this._chainCancel) return;

            this._processedSize += size;
            if(this.readResolve) {
                const resolve = this.readResolve;
                this.readResolve = null;
                this.readPromise = null;
                this._checkAllowMore();
                resolve(chunk);
            }
            else {
                this._buffer.push(chunk,size);
                this._clearChunkTimeout();
            }
        }
        catch (e) {
            this.close(StreamCloseCode.InvalidChunk);
            this._transport.onInvalidMessage(e);
        }
    }

    /**
     * @internal
     */
    _end() {
        if(this.state === StreamState.Open && !this._chainEnd) {
            this._chainEnd = true;
            this._clearChunkTimeout();
            this._chain = this._chain.then(() => this._processChainEnd());
        }
    }

    private _processChainEnd() {
        if(this._chainCancel || this.state !== StreamState.Open) return;
        this._transport._sendReadStreamClose(this.id);
        this._close(StreamCloseCode.End,true);
    }

    // noinspection JSUnusedGlobalSymbols
    /**
     * @internal
     */
    _emitBadConnection() {
        this._close(StreamCloseCode.BadConnection,false);
    }

    private _cancelChunkChain() {
        this._chainCancel = true;
        this._chainEnd = true;
        this._chainChunkSize = 0;
    }

    /**
     * @internal
     */
    _writeStreamClose(closeCode: Exclude<StreamCloseCode,StreamCloseCode.End> | number) {
        this._close(closeCode !== StreamCloseCode.End ?
            closeCode : StreamCloseCode.InvalidAction,true);
    }

    /**
     * @internal
     */
    _close(closeCode: StreamCloseCode | number, rmFromTransport: boolean = true) {
        if(this.state === StreamState.Closed) return;
        clearTimeout(this._allowMoreMinTimeout);
        (this as Writable<ReadStream>).state = StreamState.Closed;
        (this as Writable<ReadStream>).closeCode = closeCode;
        this._cancelChunkChain();
        this._clearChunkTimeout();
        if(this.readResolve) {
            const resolve = this.readResolve;
            this.readResolve = null;
            this.readPromise = null;
            resolve(null);
        }
        if(rmFromTransport) this._transport._removeReadStream(this.id);
        this._closedPromiseResolve(closeCode);
    }

    /**
     * @internal
     */
    public toJSON() {
        return '[ReadStream]';
    }
}