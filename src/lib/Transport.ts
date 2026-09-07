/*
Author: Ing. Luca Gian Scaringella
GitHub: LucaCode
Copyright(c) Ing. Luca Gian Scaringella
 */

import {
    ActionPacket,
    BundlePacket,
    NEXT_BINARIES_PACKET_TOKEN,
    PacketType, PING,
    PING_UINT8,
    PONG,
    PONG_UINT8,
} from "./Protocol";
import {containsStreams, DataType, isMixedJSONDataType, parseJSONDataType} from "./DataType";
import {dehydrateError, hydrateError} from "./ErrorUtils";
import {decodeJson, encodeJson, JSONString} from "./JsonUtils";
import ReadStream from "./streams/ReadStream";
import WriteStream from "./streams/WriteStream";
import {StreamCloseCode} from "./streams/StreamCloseCode";
import {
    CorkFunction,
    escapeJSONString,
    escapePlaceholderSequence, loadDefaults,
    MAX_SUPPORTED_ARRAY_BUFFER_SIZE,
    RESOLVED_PROMISE,
    SendFunction, unescapePlaceholderSequence,
    Writable
} from "./Utils";
import {
    BadConnectionError,
    BadConnectionType,
    InvalidActionError,
    MaxSupportedArrayBufferSizeExceededError,
    TimeoutError,
    TimeoutType
} from "./Errors";
import {InvokePackage, Package} from "./Package";
import PackageBuffer, {PackageBufferOptions} from "./PackageBuffer";
import {
    BatchOption,
    BatchOptionsValue,
    ComplexTypesOption,
    ResponseTimeoutOption,
    ReturnDataTypeOption
} from "./Options";

export type TransmitListener = (receiver: string, data: any, type: DataType) => void | Promise<void>;
export type InvokeListener = (procedure: string, data: any, end: (data?: any, processComplexTypes?: boolean) => void,
    reject: (err?: any) => void, type: DataType) => void | Promise<void>

type BinaryContentResolver = {
    callback: (err?: Error | null,binaries?: ArrayBuffer[]) => void,
    timeout: NodeJS.Timeout
};

export interface TransportOptions extends PackageBufferOptions {
    /**
     * @description
     * Defines the default timeout in milliseconds for
     * receiving the response of an invoke.
     * The timeout only starts when the data of the invoke is completely transmitted,
     * and all containing streams are closed.
     * Notice that an individual response timeout can be specified for
     * an invoke that overrides this option value.
     * @default 10000
     */
    responseTimeout: number;
    /**
     * @description
     * Defines the timeout in milliseconds for receiving
     * the referenced binary content packet of a text packet.
     * @default 10000
     */
    binaryContentPacketTimeout: number;
    /**
     * @description
     * This option defines how many
     * streams are allowed in a package.
     * @default 20
     */
    streamsPerPackageLimit: number;
    /**
     * @description
     * This option enables or disables streams.
     * @default true
     */
    streamsEnabled: boolean;
    /**
     * @description
     * This option species if chunks
     * of streams can contain streams.
     * @default false
     */
    chunksCanContainStreams: boolean;
    /**
     * @description
     * The read stream class that is used.
     * Must inherit from the Ziron ReadStream.
     * @default ReadStream
     */
    readStream: typeof ReadStream
}

export default class Transport {

    public static readonly DEFAULT_OPTIONS: Readonly<TransportOptions> = {
        readStream: ReadStream,
        responseTimeout: 10000,
        binaryContentPacketTimeout: 10000,
        streamsPerPackageLimit: 20,
        streamsEnabled: true,
        chunksCanContainStreams: false,
        ...PackageBuffer.DEFAULT_OPTIONS
    }

    public static buildOptions(options: Partial<TransportOptions>): TransportOptions {
        return loadDefaults(options,Transport.DEFAULT_OPTIONS);
    }

    public readonly buffer: PackageBuffer;

    public onInvalidMessage: (err: Error) => void;
    /**
     * @description
     * Is called whenever one of the listeners
     * (onTransmit, onInvoke, onPing, onPong) have thrown an error.
     */
    public onListenerError: (err: Error) => void;
    public onTransmit: TransmitListener;
    public onInvoke: InvokeListener;
    public onPing: () => void;
    public onPong: () => void;
    public send: SendFunction;
    public cork: CorkFunction;

    public hasLowSendBackpressure: () => boolean;

    /**
     * @description
     * A new bad connection stamp is generated whenever a bad connection is emitted.
     * The stamp can be used to check if the connection was lost in-between times.
     */
    public readonly badConnectionStamp: number = Number.MIN_SAFE_INTEGER;

    constructor(
        connector: {
            onInvalidMessage?: (err: Error) => void;
            onListenerError?: (err: Error) => void;
            onTransmit?: TransmitListener;
            onInvoke?: InvokeListener;
            onPing?: () => void;
            onPong?: () => void;
            send?: SendFunction;
            cork?: CorkFunction;
            /**
             * @description
             * The write streams will pause when the socket send backpressure is
             * not low and are waiting for low pressure.
             * When this method is used, backpressure draining
             * must be emitted with the emitSendBackpressureDrain method.
             */
            hasLowSendBackpressure?: () => boolean;
        } = {},
        /**
         * Notice that the provided options will not be cloned to save memory and performance.
         */
        public options: TransportOptions = {...Transport.DEFAULT_OPTIONS},
        connected: boolean = true)
    {
        this.onInvalidMessage = connector.onInvalidMessage || (() => {});
        this.onListenerError = connector.onListenerError || (() => {});
        this.onTransmit = connector.onTransmit || (() => {});
        this.onInvoke = connector.onInvoke || (() => {});
        this.onPing = connector.onPing || (() => {});
        this.onPong = connector.onPong || (() => {});
        this.send = connector.send || (() => {});
        this.cork = connector.cork || (cb => cb());
        this.hasLowSendBackpressure = connector.hasLowSendBackpressure || (() => true);
        this.open = connected;
        this.buffer = new PackageBuffer(this._multiSend.bind(this),
            () => this.open,options);
    }

    /**
     * Can not be reset on connection lost
     * because packages with old ids can exist.
     */
    private _binaryContentPacketId: number = 0;
    private _binaryContentResolver: Record<number,BinaryContentResolver> = {};

    /**
     * Can not be reset on connection lost
     * because packages with old ids can exist.
     */
    private _objectStreamId: number = 1;
    private _binaryStreamId: number = -1;
    private _activeReadStreams: Record<string, ReadStream> = {};
    private _activeWriteStreams: Record<string, WriteStream> = {};

    /**
     * Can not be reset on connection lost
     * because packages with old ids can exist.
     */
    private _cid: number = 0;
    private _invokeResponsePromises: Record<number,
        {
            resolve: (data: any) => void,
            reject: (err: any) => void,
            timeout?: NodeJS.Timeout,
            returnDataType?: boolean
        }> = {};

    public readonly open: boolean = true;

    private readonly _lowSendBackpressureWaiters: (() => void)[] = [];

    private _multiSend(messages: (string | ArrayBuffer)[],batches: boolean) {
        const len = messages.length;
        if(len > 1) this.cork(() => {
            for(let i = 0; i < len; i++)
                this.send(messages[i],typeof messages[i] === 'object',batches);
        });
        else if(len === 1) this.send(messages[0],typeof messages[0] === 'object',batches);
    }

    /**
     * @internal
     * @private
     */
    _addLowSendBackpressureWaiter(waiter: () => void) {
        this._lowSendBackpressureWaiters.push(waiter);
    }

    /**
     * @internal
     * @private
     */
    _cancelLowSendBackpressureWaiter(waiter: () => void) {
        const index = this._lowSendBackpressureWaiters.indexOf(waiter);
        if(index !== -1) this._lowSendBackpressureWaiters.splice(index, 1);
    }

    emitMessage(rawMsg: string | ArrayBuffer) {
        try {
            if(typeof rawMsg !== "string"){
                if(rawMsg.byteLength === 1) {
                    if((new Uint8Array(rawMsg))[0] === PING_UINT8) {
                        try {this.onPing();}
                        catch (err) {this.onListenerError(err)}
                    }
                    else if((new Uint8Array(rawMsg))[0] === PONG_UINT8) {
                        try {this.onPong();}
                        catch (err) {this.onListenerError(err)}
                    }
                    else this._processBinaryPacket(rawMsg);
                }
                else this._processBinaryPacket(rawMsg);
            }
            else {
                let packet: BundlePacket | ActionPacket;
                try {packet = decodeJson('[' + rawMsg + ']')}
                catch (err) {return this.onInvalidMessage(err)}
                if(packet) {
                    if(packet['0'] === PacketType.Bundle) {
                        const packets = packet['1'];
                        if(Array.isArray(packets)) {
                            const len = (packets as any[]).length;
                            for(let i = 0; i < len; i++) {
                                this._processJsonActionPacket(packets[i]);
                            }
                        }
                    }
                    else this._processJsonActionPacket(packet);
                }
            }
        }
        catch(e){this.onInvalidMessage(e);}
    }

    emitSendBackpressureDrain() {
        while(this._lowSendBackpressureWaiters.length && this.hasLowSendBackpressure())
            this._lowSendBackpressureWaiters.shift()!();
    }

    emitBadConnection(type: BadConnectionType,msg?: string) {
        (this as Writable<Transport>).open = false;
        this.buffer.clearBatchTime();
        const err = new BadConnectionError(type,msg);
        (this as Writable<Transport>).badConnectionStamp = this._generateNewBadConnectionStamp();
        this._rejectBinaryContentResolver(err);
        this._rejectInvokeRespPromises(err);
        this._emitBadConnectionToStreams();
        this._activeReadStreams = {};
        this._activeWriteStreams = {};
    }

    emitConnection() {
        (this as Writable<Transport>).open = true;
        this.buffer.flushBuffer();
    }

    private _onListenerError(err: Error) {
        try {this.onListenerError(err);}
        catch(_) {}
    }

    private _rejectInvokeRespPromises(err: Error) {
        const tmpPromises = this._invokeResponsePromises;
        this._invokeResponsePromises = {};
        for(const k in tmpPromises) {
            if(tmpPromises.hasOwnProperty(k)){
                clearTimeout(tmpPromises[k].timeout!);
                tmpPromises[k].reject(err);
            }
        }
    }

    private _getNewBinaryContentPacketId() {
        if(this._binaryContentPacketId > Number.MAX_SAFE_INTEGER) this._binaryContentPacketId = 0;
        return this._binaryContentPacketId++;
    }

    private _generateNewBadConnectionStamp() {
        return this.badConnectionStamp > Number.MAX_SAFE_INTEGER ?
            Number.MIN_SAFE_INTEGER : this.badConnectionStamp + 1;
    }

    private _getNewCid(): number {
        if(this._cid > Number.MAX_SAFE_INTEGER) this._cid = 0;
        return this._cid++;
    }

    /**
     * @param binaryStream
     * @private
     */
    private _getNewStreamId(binaryStream: boolean): number {
        if(binaryStream) {
            if(this._binaryStreamId < Number.MIN_SAFE_INTEGER) this._binaryStreamId = -1;
            return this._binaryStreamId--;
        }
        else {
            if(this._objectStreamId > Number.MAX_SAFE_INTEGER) this._objectStreamId = 1;
            return this._objectStreamId++;
        }
    }

    private _processBinaryPacket(buffer: ArrayBuffer) {
        const header = (new Uint8Array(buffer,0,1))[0];
        if(header === PacketType.BinaryContent)
            this._processBinaryContentPacket(new DataView(buffer),1)
        else if(header === PacketType.StreamChunk)
            this._processBinaryStreamChunk((new Float64Array(buffer.slice(1,9)))[0],buffer.slice(9));
        else if(header === PacketType.StreamEnd)
            this._processBinaryStreamEnd((new Float64Array(buffer.slice(1,9)))[0],buffer.slice(9));
        else this.onInvalidMessage(new Error('Unknown binary package header type.'))
    }

    private _processBinaryContentPacket(view: DataView, offset: number) {
        const id = view.getFloat64(offset)
        const resolver = this._binaryContentResolver[id],
            byteLength = view.byteLength,
            binaries: ArrayBuffer[] = [];
        let binaryLen: number | undefined = undefined;
        offset += 8;

        while(offset < byteLength) {
            binaryLen = view.getUint32(offset);
            offset += 4;
            if(binaryLen === NEXT_BINARIES_PACKET_TOKEN) break;
            if(resolver) binaries.push(view.buffer.slice(offset,offset + binaryLen));
            offset += binaryLen;
        }
        if(resolver){
            delete this._binaryContentResolver[id];
            clearTimeout(resolver.timeout);
            resolver.callback(null,binaries);
        }
        if(binaryLen === NEXT_BINARIES_PACKET_TOKEN) this._processBinaryContentPacket(view,offset);
    }

    private _processTransmit(receiver: string,data: any,dataType: DataType) {
        try {
            const res = this.onTransmit(receiver,data,dataType);
            if(res instanceof Promise) res.catch(err => this._onListenerError(err));
        }
        catch(err) {this._onListenerError(err)}
    }

    private _processJsonActionPacket(packet: ActionPacket) {
        switch (packet['0']) {
            case PacketType.Transmit:
                if(typeof packet['1'] !== 'string') return this.onInvalidMessage(new Error('Receiver is not a string.'));
                return this._processData(packet['2'],packet['3'],packet['4'],(err,data) => {
                    if(!err) this._processTransmit(packet['1'],data,packet['2']);
                    else if(!(err instanceof BadConnectionError)) this.onInvalidMessage(err);
                });
            case PacketType.Invoke:
                if(typeof packet['1'] !== 'string') return this.onInvalidMessage(new Error('Receiver is not a string.'));
                if(typeof packet['2'] !== 'number') return this.onInvalidMessage(new Error('CallId is not a number.'));
                return this._processData(packet['3'],packet['4'],packet['5'],(err,data) => {
                    if(!err) this._processInvoke(packet['1'],packet['2'],data,packet['3']);
                    else if(!(err instanceof BadConnectionError)) this.onInvalidMessage(err);
                });
            case PacketType.InvokeDataResp:
                const resp = this._invokeResponsePromises[packet['1']];
                if (resp) {
                    clearTimeout(resp.timeout!);
                    delete this._invokeResponsePromises[packet['1']];
                    return this._processData(packet['2'],packet['3'],packet['4'], (err,data) => {
                        if(!err) resp.resolve(resp.returnDataType ? [data,packet['2']] : data);
                        else if(err instanceof BadConnectionError) resp.reject(err);
                        else this.onInvalidMessage(err);
                    })
                }
                return;
            case PacketType.StreamChunk: return this._processJsonStreamChunk(packet['1'],packet['2'],packet['3'],packet['4']);
            case PacketType.StreamDataPermission: return this._processStreamDataPermission(packet['1'],packet['2']);
            case PacketType.StreamEnd: return this._processJsonStreamEnd(packet['1'],packet['2'],packet['3'],packet['4']);
            case PacketType.InvokeErrResp: return this._rejectInvoke(packet['1'],packet['2']);
            case PacketType.ReadStreamClose: return this._processReadStreamClose(packet['1'], packet['2']);
            case PacketType.StreamAccept: return this._processStreamAccept(packet['1'],packet['2']);
            case PacketType.WriteStreamClose: return this._processJsonWriteStreamClose(packet['1'], packet['2']);
            default: return this.onInvalidMessage(new Error('Unknown packet type.'));
        }
    }

    private _rejectInvoke(callId: number, rawErr: any) {
        const resp = this._invokeResponsePromises[callId];
        if (resp) {
            clearTimeout(resp.timeout!);
            delete this._invokeResponsePromises[callId];
            resp.reject(hydrateError(rawErr));
        }
    }

    private _processStreamAccept(streamId: number,bufferSize: number | any) {
        if(typeof bufferSize !== 'number') throw new Error('Invalid buffer size data type to accept a stream.');
        const stream = this._activeWriteStreams[streamId];
        if(stream) stream._open(bufferSize);
    }

    private _processStreamDataPermission(streamId: number,size: number | any) {
        if(typeof size !== 'number') throw new Error('Invalid stream data permission size data type.');
        const stream = this._activeWriteStreams[streamId];
        if(stream) stream._addDataPermission(size);
    }

    private _processReadStreamClose(streamId: number, closeCode?: StreamCloseCode | number) {
        if(typeof closeCode !== 'number' && typeof closeCode !== 'undefined')
            throw new Error('Invalid close code data type to close a stream.');
        const stream = this._activeWriteStreams[streamId];
        if(stream) stream._readStreamClose(closeCode ?? StreamCloseCode.End);
    }

    private _processJsonWriteStreamClose(streamId: number, closeCode: StreamCloseCode | number) {
        if(typeof closeCode !== 'number') throw new Error('Invalid close code data type to close a stream.');
        const stream = this._activeReadStreams[streamId];
        if(stream) stream._writeStreamClose(closeCode);
    }

    private _processJsonStreamChunk(streamId: number, dataType: DataType, data: any, binariesPacketId?: number) {
        const stream = this._activeReadStreams[streamId];
        if(stream) {
            stream._pushChunk(new Promise((res,rej) => {
                if(containsStreams(dataType) && !this.options.chunksCanContainStreams)
                    return rej(new Error('Streams in chunks are not allowed.'));
                this._processData(dataType,data,binariesPacketId,
                    (err,data) => err ? rej(err) : res(data));
            }),dataType);
        }
    }

    private _processJsonStreamEnd(streamId: number, dataType?: DataType, data?: any, binariesPacketId?: number) {
        const stream = this._activeReadStreams[streamId];
        if(stream) {
            if(typeof dataType === 'number') {
                stream._pushChunk(new Promise((res,rej) => {
                    if(containsStreams(dataType) && !this.options.chunksCanContainStreams)
                        return rej(new Error('Streams in chunks are not allowed.'));
                    this._processData(dataType,data,binariesPacketId,
                        (err,data) => err ? rej(err) : res(data));
                }),dataType);
            }
            stream._end();
        }
    }

    private _processBinaryStreamChunk(streamId: number, binary: ArrayBuffer) {
        const stream = this._activeReadStreams[streamId];
        if(stream) stream._pushChunk(binary,DataType.Binary);
    }

    private _processBinaryStreamEnd(streamId: number, binary: ArrayBuffer) {
        const stream = this._activeReadStreams[streamId];
        if(stream) {
            //Binary stream end package chunk is required.
            stream._pushChunk(binary,DataType.Binary);
            stream._end();
        }
    }

    private _processInvoke(procedure: string, callId: number, data: any, dataType: DataType) {
        let called;
        try {
            const badConnectionTimestamp = this.badConnectionStamp;
            const res = this.onInvoke(procedure, data,(data, processComplexTypes) => {
                if(called) throw new InvalidActionError('Response ' + callId + ' has already been sent');
                called = true;
                if(badConnectionTimestamp !== this.badConnectionStamp) return;
                try {this._sendInvokeDataResp(callId, data, processComplexTypes);}
                catch (err) {
                    //allow to reject the invoke when the response could not be sent
                    called = false;
                    throw err;
                }
            }, (err) => {
                if(called) throw new InvalidActionError('Response ' + callId + ' has already been sent');
                called = true;
                if(badConnectionTimestamp !== this.badConnectionStamp) return;
                this.send(PacketType.InvokeErrResp + ',' +
                    callId + ',' + (err instanceof JSONString ? JSONString.toString() : encodeJson(dehydrateError(err))));
            },dataType);
            if(res instanceof Promise) res.catch(err => this._onListenerError(err));
        }
        catch(err) {this._onListenerError(err);}
    }

    /**
     * Only use when the connection was not lost in-between time.
     * @param callId
     * @param data
     * @param processComplexTypes
     * @private
     */
    private _sendInvokeDataResp(callId: number, data: any, processComplexTypes?: boolean) {
        if(!processComplexTypes) {
            this.send(PacketType.InvokeDataResp + ',' + callId + ',' +
                DataType.JSON + (data !== undefined ? (',' + encodeJson(data)) : ''));
        }
        else if(data instanceof WriteStream && this.options.streamsEnabled){
            const streamId = this._getNewStreamId(data.binary);
            this.send(PacketType.InvokeDataResp + ',' + callId + ',' +
                DataType.Stream + ',' + streamId);
            data._init(this,streamId);
            data._onTransmitted();
        }
        else if(data instanceof ArrayBuffer) {
            const binaryContentPacketId = this._getNewBinaryContentPacketId();
            this.cork(() => {
                this.send(PacketType.InvokeDataResp + ',' + callId + ',' +
                    DataType.Binary + ',' + binaryContentPacketId);
                this.send(Transport._createBinaryContentPacket(binaryContentPacketId,data),true);
            })
        }
        else {
            const binaries: ArrayBuffer[] = [], streams: WriteStream<any>[] = [];
            data = this._processMixedJSONDeep(data,binaries,streams);

            const pack: Package = [
                PacketType.InvokeDataResp + ',' + callId + ',' +
                parseJSONDataType(binaries.length > 0, streams.length > 0) +
                    (data !== undefined ? (',' + encodeJson(data)) : '')
            ];

            if(binaries.length > 0) {
                const binaryContentPacketId = this._getNewBinaryContentPacketId();
                pack[0] += "," + binaryContentPacketId;
                pack[1] = Transport._createBinaryContentPacket(binaryContentPacketId,binaries);
            }

            this._silentDirectSendPackage(pack);
            if(streams.length > 0) for(let i = 0; i < streams.length; i++) streams[i]._onTransmitted();
        }
    }

    private _processData(type: DataType, data: any, dataMetaInfo: any, callback: (err?: Error | null,data?: any) => void) {
        if (type === DataType.JSON) return callback(null,data);
        else if (type === DataType.Binary) {
            if(typeof data !== 'number') return callback(new Error('Invalid binary packet id.'));
            this._resolveBinaryContent(data,(err,binaries) => {
                err ? callback(err) : callback(null,binaries![0]);
            });
        } else if (isMixedJSONDataType(type)) {
            const resolveOptions = {
                parseStreams: this.options.streamsEnabled &&
                    (type === DataType.JSONWithStreams || type === DataType.JSONWithStreamsAndBinaries),
                parseBinaries: type === DataType.JSONWithBinaries || type === DataType.JSONWithStreamsAndBinaries
            };
            if(typeof dataMetaInfo === 'number') return this._resolveBinaryContent(dataMetaInfo,
                (err, binaries) => {
                err ? callback(err) : callback(null,this._resolveMixedJSONDeep(data,resolveOptions,binaries!));
            })
            else return callback(null,this._resolveMixedJSONDeep(data,resolveOptions));
        } else if(type === DataType.Stream && this.options.streamsEnabled) {
            if(typeof data !== 'number') return callback(new Error('Invalid stream id.'));
            return callback(null,new this.options.readStream(data,this));
        }
        else callback(new Error('Invalid data type.'));
    }

    private _resolveBinaryContent(binariesPacketId: number, callback: (error?: any,binaries?: ArrayBuffer[]) => void) {
        if(this._binaryContentResolver[binariesPacketId]) throw new Error('Binaries resolver already exists.');
        this._binaryContentResolver[binariesPacketId] = {
            callback,
            timeout: setTimeout(() => {
                delete this._binaryContentResolver[binariesPacketId];
                callback(new TimeoutError(`Binaries resolver: ${binariesPacketId} not resolved in time.`,TimeoutType.BinaryResolve));
            }, this.options.binaryContentPacketTimeout)
        };
    }

    private _resolveMixedJSONDeep(data: any,
                                  options: {parseStreams: boolean, parseBinaries: boolean},
                                  binaries?: ArrayBuffer[]): Promise<any>
    {
        const wrapper = [data];
        this._internalResolveMixedJSONDeep(wrapper, 0, options, {binaries, streamCount: 0});
        return wrapper[0];
    }

    private _internalResolveMixedJSONDeep(obj: any, key: any,
                                  options: {parseStreams: boolean, parseBinaries: boolean},
                                  meta: {
                                    binaries?: ArrayBuffer[],
                                    streamCount: number
                                  }): any
    {
        const value = obj[key];
        if(typeof value === 'object' && value) {
            if(Array.isArray(value)) {
                const len = value.length;
                for (let i = 0; i < len; i++)
                    this._internalResolveMixedJSONDeep(value,i,options,meta);
            }
            else  {
                if(options.parseBinaries && typeof value['_b'] === 'number') {
                    if(!meta.binaries) throw new Error('Can not resolve binary data without binary content packet.');
                    obj[key] = meta.binaries[value['_b']];
                }
                else if(options.parseStreams && typeof value['_s'] === 'number'){
                    if(meta.streamCount >= this.options.streamsPerPackageLimit) throw new Error('Max stream limit reached.')
                    meta.streamCount++;
                    obj[key] = new this.options.readStream(value['_s'],this);
                }
                else {
                    const clone = {};
                    let unescapedKey: string;
                    for(const key in value) {
                        unescapedKey = unescapePlaceholderSequence(key);
                        clone[unescapedKey] = value[key];
                        this._internalResolveMixedJSONDeep(clone,unescapedKey,options,meta);
                    }
                    obj[key] = clone;
                }
            }
        }
    }

    private static _createBinaryContentPacket(refId: number, content: ArrayBuffer | ArrayBuffer[]): ArrayBuffer {
        if(content instanceof ArrayBuffer) {
            if(content.byteLength > MAX_SUPPORTED_ARRAY_BUFFER_SIZE)
                throw new MaxSupportedArrayBufferSizeExceededError(content);

            const packetBuffer = new DataView(new ArrayBuffer(13 + content.byteLength));
            const uint8PacketView = new Uint8Array(packetBuffer.buffer);
            packetBuffer.setInt8(0,PacketType.BinaryContent);
            packetBuffer.setFloat64(1,refId);
            packetBuffer.setUint32(9,content.byteLength);
            uint8PacketView.set(new Uint8Array(content),13);
            return packetBuffer.buffer;
        }
        else {
            const len = content.length;

            //Calculate size
            let size = 9 + len * 4, i, bi, item: ArrayBuffer;
            for(i = 0; i < len; i++) size += content[i].byteLength;

            const packetBuffer = new DataView(new ArrayBuffer(size));
            const uint8PacketView = new Uint8Array(packetBuffer.buffer);
            packetBuffer.setInt8(0,PacketType.BinaryContent);
            packetBuffer.setFloat64(1,refId);
            for(i = 0, bi = 9; i < len; i++) {
                item = content[i];
                if(item.byteLength > MAX_SUPPORTED_ARRAY_BUFFER_SIZE)
                    throw new MaxSupportedArrayBufferSizeExceededError(item);
                packetBuffer.setUint32(bi,item.byteLength);
                uint8PacketView.set(new Uint8Array(item),bi+=4);
                bi += item.byteLength;
            }
            return packetBuffer.buffer;
        }
    }

    private _processMixedJSONDeep(data: any, binaries: ArrayBuffer[], streams: WriteStream<any>[]) {
        if(typeof data === 'object' && data){
            if(data instanceof ArrayBuffer) return {_b: binaries.push(data) - 1};
            else if(data instanceof WriteStream){
                if(this.options.streamsEnabled){
                    const streamId = this._getNewStreamId(data.binary);
                    data._init(this,streamId);
                    streams.push(data);
                    return {_s: streamId}
                }
                else return data.toJSON();
            }
            else if(Array.isArray(data)) {
                const newArray: any[] = [], len = data.length;
                for (let i = 0; i < len; i++)
                    newArray[i] = this._processMixedJSONDeep(data[i], binaries, streams);
                return newArray;
            }
            else if(!(data instanceof Date)) {
                const clone = {};
                for(const key in data) {
                    // noinspection JSUnfilteredForInLoop
                    clone[escapePlaceholderSequence(key)] = this._processMixedJSONDeep(data[key], binaries, streams);
                }
                return clone;
            }
        }
        return data;
    }

    private _rejectBinaryContentResolver(err: Error) {
        let resolver: BinaryContentResolver;
        for(const k in this._binaryContentResolver) {
            if(this._binaryContentResolver.hasOwnProperty(k)){
                resolver = this._binaryContentResolver[k];
                clearTimeout(resolver.timeout);
                resolver.callback(err);
            }
        }
        this._binaryContentResolver = {};
    }

    private _emitBadConnectionToStreams() {
        for (const k in this._activeReadStreams) {
            if (this._activeReadStreams.hasOwnProperty(k))
                this._activeReadStreams[k]._emitBadConnection();
        }
        for (const k in this._activeWriteStreams) {
            if (this._activeWriteStreams.hasOwnProperty(k))
                this._activeWriteStreams[k]._emitBadConnection();
        }
    }

    /**
     * @internal
     * @param id
     * @param stream
     * @private
     */
    _addReadStream(id: number, stream: ReadStream) {
        this._activeReadStreams[id] = stream;
    }

    /**
     * @internal
     * @param id
     * @param stream
     * @private
     */
    _addWriteStream(id: number, stream: WriteStream<any>) {
        this._activeWriteStreams[id] = stream;
    }

    /**
     * @internal
     * @private
     * @param id
     */
    _removeWriteStream(id: number) {
        delete this._activeWriteStreams[id];
    }

    /**
     * @internal
     * @param id
     * @private
     */
    _removeReadStream(id: number) {
        delete this._activeReadStreams[id];
    }

    //Send
    /**
     * Notice that the package can not send multiple times.
     * If you need this you can check out the static method prepareMultiTransmit.
     * Also after preparing you should not send millions of other
     * packages before sending the created package.
     * It is perfect to prepare packages when the connection
     * is lost and send them when the socket is connected again.
     * @param receiver
     * It should not contain double-quotes.
     * To be sure, you can use the escapeJSONString function.
     * @param data
     * @param processComplexTypes
     */
    prepareTransmit(receiver: string, data?: any, {processComplexTypes}: ComplexTypesOption = {}): Package {
        receiver = escapeJSONString(receiver);
        if(!processComplexTypes) {
            return [PacketType.Transmit + ',"' + receiver + '",' +
            DataType.JSON + (data !== undefined ? (',' + encodeJson(data)) : '')];
        }
        else if(data instanceof WriteStream && this.options.streamsEnabled){
            const streamId = this._getNewStreamId(data.binary);
            const packet: Package = [PacketType.Transmit + ',"' + receiver + '",' +
                DataType.Stream + ',' + streamId];
            data._init(this,streamId);
            packet._afterSend = () => data._onTransmitted();
            return packet;
        }
        else if(data instanceof ArrayBuffer) {
            const binaryContentPacketId = this._getNewBinaryContentPacketId();
            return [PacketType.Transmit + ',"' + receiver + '",' +
                DataType.Binary + ',' + binaryContentPacketId,
                Transport._createBinaryContentPacket(binaryContentPacketId,data)];
        }
        else {
            const binaries: ArrayBuffer[] = [], streams: WriteStream<any>[] = [];
            data = this._processMixedJSONDeep(data,binaries,streams);

            const pack: Package = [
                PacketType.Transmit + ',"' + receiver + '",' +
                parseJSONDataType(binaries.length > 0,streams.length > 0) +
                (data !== undefined ? (',' + encodeJson(data)) : '')
            ];

            if(binaries.length > 0) {
                const binaryContentPacketId = this._getNewBinaryContentPacketId();
                pack[0] += "," + binaryContentPacketId;
                pack[1] = Transport._createBinaryContentPacket(binaryContentPacketId,binaries);
            }
            if(streams.length > 0)
                pack._afterSend = () => {for(let i = 0; i < streams.length; i++) streams[i]._onTransmitted();}
            return pack;
        }
    }

    /**
     * Notice that the package can not send multiple times.
     * Also after preparing you should not send millions of other
     * packages before sending the created package.
     * It is perfect to prepare packages when the connection
     * is lost and send them when the socket is connected again.
     * @param procedure
     * It should not contain double-quotes.
     * To be sure, you can use the escapeJSONString function.
     * @param data
     * @param responseTimeout
     * @param processComplexTypes
     * @param returnDataType
     */
    prepareInvoke<RDT extends true | false | undefined>(
        procedure: string,
        data?: any,
        {responseTimeout,processComplexTypes,returnDataType}: ResponseTimeoutOption & ComplexTypesOption & ReturnDataTypeOption<RDT> = {}
        ): InvokePackage<RDT extends true ? [any,DataType] : any>
    {
        procedure = escapeJSONString(procedure);
        const callId = this._getNewCid();
        const pack: InvokePackage = [] as any;

        if(!processComplexTypes) {
            pack.promise = new Promise<any>((resolve, reject) => {
                pack._afterSend = () => {
                    this._invokeResponsePromises[callId] = returnDataType ? {resolve, reject, returnDataType} : {resolve, reject};
                    this._invokeResponsePromises[callId].timeout = setTimeout(() => {
                        delete this._invokeResponsePromises[callId];
                        reject(new TimeoutError(`Response for call id: "${callId}" timed out`,TimeoutType.InvokeResponse));
                    }, responseTimeout || this.options.responseTimeout);
                }
            });
            pack[0] = PacketType.Invoke + ',"' + procedure + '",' + callId + ',' +
                DataType.JSON + (data !== undefined ? (',' + encodeJson(data)) : '');
            return pack;
        }
        else {
            let setResponse: (() => void) | undefined = undefined;
            let setResponseTimeout: (() => void) | undefined = undefined;
            pack.promise = new Promise<any>((resolve, reject) => {
                setResponse = () => {
                    this._invokeResponsePromises[callId] = returnDataType ? {resolve, reject, returnDataType} : {resolve, reject};
                }
                setResponseTimeout = () => {
                    if(this._invokeResponsePromises[callId] && this._invokeResponsePromises[callId].timeout === undefined)
                        this._invokeResponsePromises[callId].timeout = setTimeout(() => {
                            delete this._invokeResponsePromises[callId];
                            reject(new TimeoutError(`Response for call id: "${callId}" timed out`,TimeoutType.InvokeResponse));
                        }, responseTimeout || this.options.responseTimeout);
                }
            });

            if(data instanceof WriteStream && this.options.streamsEnabled){
                const sent = new Promise(res => pack._afterSend = () => {
                    setResponse!();
                    res();
                    (data as WriteStream<any>)._onTransmitted();
                });
                const streamId = this._getNewStreamId(data.binary);
                pack[0] = PacketType.Invoke + ',"' + procedure + '",' + callId + ',' +
                    DataType.Stream + ',' + streamId;
                Promise.all([sent,data.closed]).then(setResponseTimeout);
                data._init(this,streamId);
                return pack;
            }
            else if(data instanceof ArrayBuffer) {
                pack._afterSend = () => {
                    setResponse!();
                    setResponseTimeout!();
                }
                const binaryContentPacketId = this._getNewBinaryContentPacketId();
                pack[0] = PacketType.Invoke + ',"' + procedure + '",' + callId + ',' +
                    DataType.Binary + ',' + binaryContentPacketId;
                pack[1] = Transport._createBinaryContentPacket(binaryContentPacketId,data);
                return pack;
            }
            else {
                const binaries: ArrayBuffer[] = [], streams: WriteStream<any>[] = [];
                data = this._processMixedJSONDeep(data,binaries,streams);

                if(streams.length > 0) {
                    const sent = new Promise(res => pack._afterSend = () => {
                        setResponse!();
                        res();
                        for(let i = 0; i < streams.length; i++) streams[i]._onTransmitted();
                    });
                    Promise.all([sent,...streams.map(stream => stream.closed)]).then(setResponseTimeout);
                }
                else pack._afterSend = () => {
                    setResponse!();
                    setResponseTimeout!();
                }

                pack[0] = PacketType.Invoke + ',"' + procedure + '",' + callId + ',' +
                    parseJSONDataType(binaries.length > 0,streams.length > 0) +
                    (data !== undefined ? (',' + encodeJson(data)) : '');

                if(binaries.length > 0) {
                    const binaryContentPacketId = this._getNewBinaryContentPacketId();
                    pack[0] += "," + binaryContentPacketId;
                    pack[1] = Transport._createBinaryContentPacket(binaryContentPacketId,binaries);
                }

                return pack;
            }
        }
    }

    // noinspection JSUnusedGlobalSymbols
    sendPackage(pack: Package, batch?: BatchOptionsValue): void {
        if(!this.open) this.buffer.add(pack);
        else if(batch) this.buffer.add(pack,batch);
        else this._directSendPackage(pack);
    }

    // noinspection JSUnusedGlobalSymbols
    sendPackageWithPromise(pack: Package, batch?: BatchOptionsValue): Promise<void> {
        if(batch) {
            return new Promise((resolve) => {
                const tmpAfterSend = pack._afterSend;
                pack._afterSend = () => {
                    if(tmpAfterSend) tmpAfterSend();
                    resolve();
                }
                this.buffer.add(pack,batch);
            })
        }
        else if(this.open) return this._directSendPackage(pack), RESOLVED_PROMISE;
        else return new Promise((resolve) => {
            const tmpAfterSend = pack._afterSend;
            pack._afterSend = () => {
                if(tmpAfterSend) tmpAfterSend();
                resolve();
            }
            this.buffer.add(pack);
        })
    }

    // noinspection JSUnusedGlobalSymbols
    invoke<RDT extends true | false | undefined>(procedure: string, data?: any, options: ResponseTimeoutOption &
        ComplexTypesOption & BatchOption & ReturnDataTypeOption<RDT> = {}):
        Promise<RDT extends true ? [any,DataType] : any>
    {
        const prePackage = this.prepareInvoke(procedure,data,options);
        this.sendPackage(prePackage,options.batch);
        return prePackage.promise;
    }

    // noinspection JSUnusedGlobalSymbols
    transmit(receiver: string, data?: any, options: BatchOption & ComplexTypesOption = {}) {
        this.sendPackage(this.prepareTransmit(receiver,data,options),options.batch);
    }

    // noinspection JSUnusedGlobalSymbols
    sendPing() {
        try {this.send(PING,true);}
        catch (_) {}
    }

    // noinspection JSUnusedGlobalSymbols
    sendPong() {
        try {this.send(PONG,true);}
        catch (_) {}
    }

    private _silentDirectSendPackage(pack: Package) {
        if(pack.length > 1) this.cork(() => {
            this.send(pack[0]);
            this.send(pack[1]!, true);
        })
        else this.send(pack[0]);
    }

    private _directSendPackage(pack: Package) {
        if(pack.length > 1) this.cork(() => {
            this.send(pack[0]);
            this.send(pack[1]!, true);
        })
        else this.send(pack[0]);
        if(pack._afterSend) pack._afterSend();
    }

    /**
     * Only use when the connection was not lost in-between time.
     * It sends the chunk directly.
     * @internal
     * @param streamId
     * @param data
     * @param processComplexTypes
     * @param end
     * @private
     */
    _sendStreamChunk(streamId: number, data: any, processComplexTypes?: boolean, end?: boolean) {
        if(!processComplexTypes) {
            this.send((end ? PacketType.StreamEnd : PacketType.StreamChunk) +
                ',' + streamId + ',' + DataType.JSON + (data !== undefined ? (',' + encodeJson(data)) : ''));
        }
        else if(this.options.chunksCanContainStreams && data instanceof WriteStream){
            const nestedStreamId = this._getNewStreamId(data.binary);
            this.send((end ? PacketType.StreamEnd : PacketType.StreamChunk) +
                ',' + streamId + ',' + DataType.Stream + ',' + streamId);
            data._init(this,streamId);
            data._onTransmitted();
        }
        else if(data instanceof ArrayBuffer)
            this.send(Transport._createBinaryStreamChunkPacket(streamId,new Uint8Array(data),end),true);
        else {
            const binaries: ArrayBuffer[] = [], streams: WriteStream<any>[] = [];
            data = this._processMixedJSONDeep(data,binaries,streams);

            const pack: Package = [
                (end ? PacketType.StreamEnd : PacketType.StreamChunk) +
                ',' + streamId + ',' + parseJSONDataType(binaries.length > 0 || streams.length > 0) +
                (data !== undefined ? (',' + encodeJson(data)) : '')
            ];

            if(binaries.length > 0) {
                const binaryContentPacketId = this._getNewBinaryContentPacketId();
                pack[0] += "," + binaryContentPacketId;
                pack[1] = Transport._createBinaryContentPacket(binaryContentPacketId,binaries);
            }

            this._silentDirectSendPackage(pack);
            if(streams.length > 0) for(let i = 0; i < streams.length; i++) streams[i]._onTransmitted();
        }
    }

    /**
     * @internal
     * @description
     * Useful to send a binary stream chunk
     * packet directly (faster than using _sendStreamChunk).
     */
    _sendBinaryStreamChunk(streamId: number, binaryPart: Uint8Array, end?: boolean) {
        this.send(Transport._createBinaryStreamChunkPacket(streamId,binaryPart,end),true)
    }

    /**
     * @internal
     * @description
     * Sends a stream end without any data.
     * @param streamId
     */
    _sendStreamEnd(streamId: number) {
        this.send(PacketType.StreamEnd + ',' + streamId);
    }

    private static _createBinaryStreamChunkPacket(streamId: number, binary: Uint8Array, end?: boolean): ArrayBuffer {
        const packetBuffer = new Uint8Array(9 + binary.byteLength);
        packetBuffer[0] = end ? PacketType.StreamEnd : PacketType.StreamChunk;
        packetBuffer.set(new Uint8Array((new Float64Array([streamId])).buffer),1);
        packetBuffer.set(binary,9);
        return packetBuffer.buffer;
    }

    /**
     * Only use when the connection was not lost in-between time.
     * @internal
     * @param streamId
     * @param allowedSize
     * @private
     */
    _sendStreamAccept(streamId: number,allowedSize: number) {
        this.send(PacketType.StreamAccept + ',' + streamId + ',' + allowedSize);
    }

    /**
     * Only use when the connection was not lost in-between time.
     * @internal
     * @param streamId
     * @param allowedSize
     * @private
     */
    _sendStreamAllowMore(streamId: number,allowedSize: number) {
        this.send(PacketType.StreamDataPermission + ',' + streamId + ',' + allowedSize);
    }

    /**
     * Only use when the connection was not lost in-between time.
     * @internal
     * @param streamId
     * @param closeCode
     * @private
     */
    _sendReadStreamClose(streamId: number, closeCode?: number) {
        this.send(PacketType.ReadStreamClose + ',' + streamId +
            (closeCode != null ? (',' + closeCode) : ''));
    }

    /**
     * Only use when the connection was not lost in-between time.
     * @internal
     * @param streamId
     * @param closeCode
     * @private
     */
    _sendWriteStreamClose(streamId: number, closeCode: number) {
        this.send(PacketType.WriteStreamClose + ',' + streamId + ',' + closeCode);
    }

    /**
     * @description
     * Tries to cancel a package sent if it is not already sent.
     * The returned boolean indicates if it was successfully cancelled.
     * @param pack
     */
    public tryCancelPackage(pack: Package): boolean {
        return this.buffer.tryRemove(pack);
    }

    /**
     * @internal
     */
    public toJSON() {
        return '[Transport]';
    }

    //Multi transport

    private static _multiBinaryContentPacketId: number = -1;

    private static _getNewMultiBinaryContentPacketId() {
        if(Transport._multiBinaryContentPacketId < Number.MIN_SAFE_INTEGER) Transport._multiBinaryContentPacketId = -1;
        return Transport._multiBinaryContentPacketId--;
    }

    /**
     * @description
     * Creates a transmit package that can be sent to multiple transporters
     * but not multiple times to the same transport (except there is no binary data in the package).
     * This is extremely efficient when sending to a lot of transporters.
     * Notice that streams are not supported but binaries are supported.
     * After preparing you should not wait a long time to send the package to the targets.
     * @param receiver
     * It should not contain double-quotes.
     * To be sure, you can use the escapeJSONString function.
     * @param data
     * @param processComplexTypes
     */
    public static prepareMultiTransmit(receiver: string, data?: any, {processComplexTypes}: ComplexTypesOption = {}): Package {
        receiver = escapeJSONString(receiver);
        if(!processComplexTypes) {
            return [PacketType.Transmit + ',"' + receiver + '",' +
                DataType.JSON + (data !== undefined ? (',' + encodeJson(data)) : '')];
        }
        else if(data instanceof ArrayBuffer) {
            const binaryContentPacketId = Transport._getNewMultiBinaryContentPacketId();
            return [PacketType.Transmit + ',"' + receiver + '",' +
                DataType.Binary + ',' + binaryContentPacketId,
                Transport._createBinaryContentPacket(binaryContentPacketId,data)];
        }
        else {
            const binaries: ArrayBuffer[] = [];
            data = Transport._processMultiMixedJSONDeep(data,binaries);
            const pack: Package = [
                PacketType.Transmit + ',"' + receiver + '",' +
                parseJSONDataType(binaries.length > 0,false) +
                (data !== undefined ? (',' + encodeJson(data)) : '')
            ];
            if(binaries.length > 0) {
                const binaryContentPacketId = Transport._getNewMultiBinaryContentPacketId();
                pack[0] += "," + binaryContentPacketId;
                pack[1] = Transport._createBinaryContentPacket(binaryContentPacketId,binaries);
            }
            return pack;
        }
    }

    private static _processMultiMixedJSONDeep(data: any, binaries: ArrayBuffer[]) {
        if(typeof data === 'object' && data){
            if(data instanceof ArrayBuffer) return {_b: binaries.push(data) - 1};
            else if(Array.isArray(data)) {
                const newArray: any[] = [], len = data.length;
                for (let i = 0; i < len; i++)
                    newArray[i] = Transport._processMultiMixedJSONDeep(data[i],binaries);
                return newArray;
            }
            else if(!(data instanceof Date)) {
                const clone = {};
                for(const key in data) {
                    // noinspection JSUnfilteredForInLoop
                    clone[escapePlaceholderSequence(key)] = Transport._processMultiMixedJSONDeep(data[key],binaries);
                }
                return clone;
            }
        }
        return data;
    }

    connect(transport: Transport) {
        let outgoingChain = Promise.resolve();
        this.send = msg => outgoingChain = outgoingChain.then(() => transport.emitMessage(msg));
        let incomingChain = Promise.resolve();
        transport.send = msg => incomingChain = incomingChain.then(() => this.emitMessage(msg));
    }

}