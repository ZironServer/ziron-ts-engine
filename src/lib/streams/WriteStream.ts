/*
Author: Ing. Luca Gian Scaringella
GitHub: LucaCode
Copyright(c) Ing. Luca Gian Scaringella
 */

import Transport from "../Transport";
import {StreamCloseCode} from "./StreamCloseCode";
import {StreamState} from "./StreamState";
import {Writable} from "../Utils";

type WriterCallback<B extends boolean> = (write: (B extends true ? ((data: ArrayBuffer) => void) :
    ((data: any, processComplexTypes?: boolean) => void)),end: (B extends true ? ((data?: ArrayBuffer) => void) :
    ((data?: any, processComplexTypes?: boolean) => void))) => void | Promise<void>

interface TimeoutOption {
    /**
     * @description
     * The ReadStream must accept the WriteStream to continue.
     * The timeout is used to close the stream with an error when the ReadStream never answers.
     * @default 5000
     */
    acceptTimeout?: number,
    /**
     * @description
     * Whenever the WriteStream reaches the allowed size, it waits for permission to write more data.
     * The timeout helps to close the stream in an error state when the
     * ReadStream never sends permission for more data.
     * Be careful and give the ReadStream side some time to read data from the buffer.
     * Only when space is available in the buffer, the ReadStream normally sends permission for more data.
     * When you can trust the other side to send permissions, you can disable this timeout with null.
     * @default 20000
     */
    sizePermissionTimeout?: number | null,
    /**
     * @description
     * When the WriteStream sends the end indicator,
     * the ReadStream will close the WriteStream successfully when all chunks
     * have been processed without an error.
     * It does not mean that all chunks on the ReadStream side need to be read;
     * there can still be chunks in the buffer.
     * You can disable this timeout with null when you can trust the
     * other side to close the stream after processing successfully.
     * @default 10000
     */
    endClosureTimeout?: number | null;
}

export default class WriteStream<B extends boolean = false> {

    private readonly acceptTimeout: number = 5000;
    private readonly sizePermissionTimeout: number | null = 20000
    private readonly endClosureTimeout: number | null = 10000;

    /**
     * @description
     * Indicates if the WriteStream was sent to the other side.
     */
    public readonly transmitted: boolean = false;

    /**
     * @description
     * Indicates if this is a binary stream.
     */
    public readonly binary: boolean;

    /**
     * @description
     * Indicates if this is a object stream.
     */
    public get object(): boolean {
        return !this.binary;
    }

    /**
     * @description
     * The current state of the stream.
     */
    public readonly state: StreamState = StreamState.Unused;

    private _allowedSize: number = 0;
    private _sentSize: number = 0;

    private _resolveSizePermissionWait: ((err?: Error) => void) | null;
    private _cancelLowSendBackpressureWait: ((err: Error) => void) | null;

    private _writeLock: boolean = false;

    private _closePromiseResolve: (closeCode: StreamCloseCode | number) => void;

    /**
     * @description
     * A promise that gets resolved on stream closure with the close code.
     */
    public readonly closed: Promise<StreamCloseCode | number> = new Promise(resolve => this._closePromiseResolve = resolve);

    private _openedPromiseResolve: () => void;

    /**
     * @description
     * A promise that gets resolved when the stream opens.
     * Notice the promise is still resolved when the stream has opened and closed.
     * Keep in mind that in some cases, when the stream gets closed directly without being opened, the promise will never resolve.
     */
    public readonly opened: Promise<void> = new Promise(resolve => this._openedPromiseResolve = resolve);

    /**
     * @description
     * This property can be used to access the close code when the stream has been closed.
     */
    public readonly closeCode?: StreamCloseCode | number;

    private _id: number;
    private _transport: Transport;

    private _acceptTimeoutTicker: NodeJS.Timeout;
    private _endClosureTimeoutTicker: NodeJS.Timeout;

    private _eofSent: boolean;

    constructor(binary?: B,timeouts?: TimeoutOption) {
        this.binary = binary || false;
        this.write = (binary ? this.binaryWrite.bind(this) : this.objectWrite.bind(this)) as any;
        this.end = (binary ? this.binaryEnd.bind(this) :
            this.objectEnd.bind(this)) as any;
        if(timeouts) Object.assign(this,timeouts);
    }

    /**
     * @description
     * An alternative to manually calling write or end and checking the return value.
     * The provided callback will be called every time when the previous write was successful
     * and was not an end call.
     * @param callback
     */
    useWriter(callback: WriterCallback<B>) {
        const write = (...args: any) => {
            (this.write as any)(...args).then(result => {
                if(result) callback(write as any,this.end);
            }).catch(() => {});
        };
        callback(write as any,this.end);
    }

    private get remainingSizeAllowed(): number {
        return this._allowedSize - this._sentSize;
    }

    /**
     * @internal
     * @private
     * @param transport
     * @param id
     * The id must be negative when the stream is a
     * binary stream or positive when the stream is an object stream.
     */
    _init(transport: Transport, id: number) {
        if((this.binary && id > 0) || (!this.binary && id < 0))
            throw new Error("Id does not match with the stream mode.")
        if(this.state !== StreamState.Unused) throw new Error('Write-stream already used.');
        this._transport = transport;
        this._id = id;
        (this as Writable<WriteStream>).state = StreamState.Pending;
        this._transport._addWriteStream(id,this);
    }

    /**
     * @internal
     */
    _onTransmitted() {
        if(this.transmitted || this.state !== StreamState.Pending) return;
        (this as Writable<WriteStream>).transmitted = true;
        this._acceptTimeoutTicker = setTimeout(() => this.close(StreamCloseCode.AcceptTimeout),
            this.acceptTimeout);
    }

    /**
     * @internal
     * @private
     */
    _open(bufferSize: number) {
        if(this.state === StreamState.Closed) {
            //Stream is already locally closed (Inform the ReadStream again...)
            //It can happen that this stream was closed in the pending state and the
            //ReadStream could not receive the close package.
            return this._transport._sendWriteStreamClose(this._id,this.closeCode ?? StreamCloseCode.Abort);
        }
        else if(this.state !== StreamState.Pending) {
            //Some strange behaviour close the stream and inform ReadStream...
            return this.close(StreamCloseCode.Abort);
        }

        clearTimeout(this._acceptTimeoutTicker);
        (this as Writable<WriteStream>).state = StreamState.Open;
        this._openedPromiseResolve();
        this._allowSize(bufferSize);
    }

    /**
     * @internal
     * @private
     * @param size
     */
    _addDataPermission(size: number) {
        this._allowSize(size);
    }

    private waitForMoreSizePermission(): Promise<void> {
        if(this._resolveSizePermissionWait) throw new Error("Already waiting for size permission");
        else return new Promise((res,rej) => {
            let timeoutTicker;
            if(this.sizePermissionTimeout != null) {
                timeoutTicker = setTimeout(() =>
                    this.close(StreamCloseCode.SizePermissionTimeout),this.sizePermissionTimeout)
            }
            this._resolveSizePermissionWait = (err?: Error) => {
                clearTimeout(timeoutTicker);
                this._resolveSizePermissionWait = null;
                err ? rej(err) : res();
            }
        });
    }

    private waitForLowSendBackpressure(): Promise<void> | void {
        if(this._transport.hasLowSendBackpressure()) return;
        if(this._cancelLowSendBackpressureWait) throw new Error("Already waiting for low send backpressure");
        else return new Promise((res,rej) => {
            const resolver = (err?: Error) => {
                this._cancelLowSendBackpressureWait = null;
                if(err) {
                    this._transport._cancelLowSendBackpressureWaiter(resolver);
                    rej(err);
                }
                else res();
            }
            this._transport._addLowSendBackpressureWaiter(resolver);
            this._cancelLowSendBackpressureWait = resolver;
        });
    }

    private _allowSize(size: number) {
        if(this.state !== StreamState.Open || size <= 0) return;
        this._allowedSize += size;
        if(this._resolveSizePermissionWait)
            this._resolveSizePermissionWait();
    }

    private async binaryWrite(data: ArrayBuffer | null): Promise<boolean> {
        if(data === null) return this.end();
        if(this.state === StreamState.Closed) return false;
        if(this._writeLock) throw new Error("The previous write is still being processed.");
        if(this._eofSent) throw new Error("Can not write when end was already called.");
        if(this.state !== StreamState.Open) {
            await Promise.race([this.opened,this.closed]);
            if((this.state as StreamState) !== StreamState.Open) return false;
        }
        const availableSize = this.remainingSizeAllowed;
        try {
            this._writeLock = true;
            if(data.byteLength <= availableSize) await this._sendBinaryChunk(new Uint8Array(data));
            else await this._internalSendBinaryChunked(data,availableSize);
        }
        catch (_) {return false;}
        finally {this._writeLock = false;}
        return true;
    }

    /**
     * Used to send binary data chunked to not
     * break the allowed remaining size.
     * @param data
     * @param availableSize
     * @private
     */
    private async _internalSendBinaryChunked(data: ArrayBuffer, availableSize = this.remainingSizeAllowed) {
        for(let i = 0; i < data.byteLength;) {
            if(availableSize <= 0) {
                await this.waitForMoreSizePermission();
                availableSize = this.remainingSizeAllowed;
            }
            const byteLength = Math.min(data.byteLength - i,availableSize);
            await this._sendBinaryChunk(new Uint8Array(data,i,byteLength))
            i+= byteLength;
            availableSize = this.remainingSizeAllowed;
        }
    }

    private async objectWrite(data: any, processComplexTypes?: boolean): Promise<boolean> {
        if(data === null) return this.end();
        if(this.state === StreamState.Closed) return false;
        if(this._writeLock) throw new Error("The previous write is still being processed.");
        if(this._eofSent) throw new Error("Can not write when end was already called.");
        if(this.state !== StreamState.Open) {
            await Promise.race([this.opened,this.closed]);
            if((this.state as StreamState) !== StreamState.Open) return false;
        }
        try {
            this._writeLock = true;
            if(this.remainingSizeAllowed > 0) await this._sendObjectChunk(data,processComplexTypes);
            else {
                await this.waitForMoreSizePermission();
                await this._sendObjectChunk(data,processComplexTypes);
            }
        }
        catch (_) {return false;}
        finally {this._writeLock = false;}
        return true;
    }

    /**
     * @description
     * Writes a chunk or EOF indication (with null).
     * Don't call this method when the previous write promise is not resolved yet.
     * The returned promise resolves to true when the write has been transmitted successfully and
     * to false when the write failed because of closure; you then should also stop further writing.
     * In case of an EOF indication, the WriteStream will close with a successful state
     * when the ReadStream has informed the WriteStream that all chunks have been processed successfully.
     */
    readonly write: (B extends true ? ((data: ArrayBuffer | null) => Promise<boolean>) :
        ((data: any | null, processComplexTypes?: boolean) => Promise<boolean>));

    private async binaryEnd(data?: ArrayBuffer): Promise<boolean> {
        if(this.state === StreamState.Closed) return false;
        if(this._writeLock) throw new Error("The previous write is still being processed.");
        if(this._eofSent) throw new Error("End was already called.");
        if(this.state !== StreamState.Open) {
            await Promise.race([this.opened,this.closed]);
            if((this.state as StreamState) !== StreamState.Open) return false;
        }

        try {
            this._writeLock = true;
            if(data == null) await this._sendStreamEnd();
            else if(data.byteLength <= this.remainingSizeAllowed)
                await this._sendBinaryChunk(new Uint8Array(data),true);
            else {
                await this._internalSendBinaryChunked(data);
                await this._sendStreamEnd();
            }
            this._onEOFSend();
        }
        catch (_) {return false;}
        finally {this._writeLock = false;}
        return true;
    }

    private async objectEnd(data?: any, processComplexTypes?: boolean): Promise<boolean>
    {
        if(this.state === StreamState.Closed) return false;
        if(this._writeLock) throw new Error("The previous write is still being processed.");
        if(this._eofSent) throw new Error("End was already called.");
        if(this.state !== StreamState.Open) {
            await Promise.race([this.opened,this.closed]);
            if((this.state as StreamState) !== StreamState.Open) return false;
        }

        try {
            this._writeLock = true;
            if(data === undefined) await this._sendStreamEnd();
            else if(this.remainingSizeAllowed > 0) await this._sendObjectChunk(data,processComplexTypes,true);
            else {
                await this.waitForMoreSizePermission();
                await this._sendObjectChunk(data,processComplexTypes,true);
            }
            this._onEOFSend();
        }
        catch (_) {return false;}
        finally {this._writeLock = false;}
        return true;
    }

    /**
     * @description
     * Ends the stream.
     * You optionally can write a last chunk.
     * Don't call this method when the previous write promise is not resolved yet.
     * The returned promise resolves to true when the package/s has been transmitted successfully and
     * to false when the operation failed because of closure.
     * The WriteStream will close with an end close code when the ReadStream has
     * informed the WriteStream that all chunks have been processed successfully.
     * In optimal cases, the method packages the chunk and EOF indication in a single package.
     */
    readonly end: (B extends true ? ((data?: ArrayBuffer) => Promise<boolean>) :
        ((data?: any, processComplexTypes?: boolean) => Promise<boolean>));

    private async _sendBinaryChunk(data: Uint8Array,end?: boolean) {
        await this.waitForLowSendBackpressure();
        this._sentSize += data.byteLength;
        this._transport._sendBinaryStreamChunk(this._id,data,end);
    }

    private async _sendObjectChunk(data: any, processComplexTypes?: boolean, end?: boolean) {
        await this.waitForLowSendBackpressure();
        this._sentSize += 1;
        this._transport._sendStreamChunk(this._id,data,processComplexTypes,end);
    }

    private async _sendStreamEnd() {
        await this.waitForLowSendBackpressure();
        this._transport._sendStreamEnd(this._id);
    }

    private _onEOFSend() {
        this._eofSent = true;
        if(this.endClosureTimeout != null && this._endClosureTimeoutTicker == null) {
            this._endClosureTimeoutTicker = setTimeout(() => {
                this.close(StreamCloseCode.EndClosureTimeout);
            },this.endClosureTimeout);
        }
    }

    /**
     * @internal
     */
    _emitBadConnection() {
        this._close(StreamCloseCode.BadConnection,false)
    }

    /**
     * @internal
     */
    _readStreamClose(closeCode: StreamCloseCode | number) {
        this._close(closeCode,true);
    }

    /**
     * @description
     * Returns if the WriteStream has been closed successfully (end close code).
     */
    get closedSuccessfully(): boolean {
        return this.state === StreamState.Closed && this.closeCode === StreamCloseCode.End;
    }

    /**
     * Use this method to close the Write- and Read-Stream immediately with a close code in abnormal cases.
     * To indicate that all chunks have been sent you should use the end method and not the close method.
     * @param closeCode
     */
    close(closeCode: Exclude<StreamCloseCode,StreamCloseCode.End> | number) {
        if(closeCode === StreamCloseCode.End) throw new Error("A write stream can not immediately close the Write- and Read-Stream successfully. Use the end method to indicate the stream data end!")
        if(this.state === StreamState.Closed) return;
        if(this.state !== StreamState.Unused) this._transport._sendWriteStreamClose(this._id,closeCode);
        this._close(closeCode,true);
    }

    private _close(closeCode: StreamCloseCode | number, rmFromTransport: boolean = true) {
        if(this.state === StreamState.Closed) return;
        (this as Writable<WriteStream>).state = StreamState.Closed;
        (this as Writable<WriteStream>).closeCode = closeCode;
        clearTimeout(this._acceptTimeoutTicker);
        clearTimeout(this._endClosureTimeoutTicker);
        if(rmFromTransport) this._transport._removeWriteStream(this._id);
        if(this._resolveSizePermissionWait)
            this._resolveSizePermissionWait(new Error("Stream is closed."));
        if(this._cancelLowSendBackpressureWait)
            this._cancelLowSendBackpressureWait(new Error("Stream is closed."));
        this._closePromiseResolve(closeCode);
    }

    /**
     * @internal
     */
    public toJSON() {
        return '[WriteStream]';
    }
}