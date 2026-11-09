/*
Author: Ing. Luca Gian Scaringella
GitHub: LucaCode
Copyright(c) Ing. Luca Gian Scaringella
 */

import Transport from "./Transport";
import {EMPTY_FUNCTION, GroupCorkFunction, GroupSendFunction, loadDefaults} from "./Utils";
import PackageBuffer, {PackageBufferOptions} from "./PackageBuffer";
import {Package} from "./Package";
import {BatchOption, ComplexTypesOption, isBatched} from "./Options";

export interface DynamicGroupTransportOptions extends PackageBufferOptions {
    /**
     * @description
     * Defines how many buffers can be stored in a
     * pool when they're currently not needed anymore.
     * @default 100
     */
    freeBufferMaxPoolSize: number;
}

const FREE_BUFFER_SEND_PLACEHOLDER = () => {throw new Error("Can not send from an unused buffer.");};
const ALWAYS_OPEN = () => true;

/**
 * @description
 * This util class helps to build a transport for
 * multiple groups with multiple end sources.
 * Instead of using the transport prepareMultiTransmit
 * method and sending the package with each transporter,
 * the batching is not individual for each transport and shared for a group.
 * This class dynamically manages multiple groups with buffers instead of
 * the GroupTransport class with only represents a single group.
 * The group buffer should be tried to flush on reconnection of
 * the underlying group source when the source has a temporarily disconnected state.
 */
export default class DynamicGroupTransport {

    public send: GroupSendFunction;
    public cork: GroupCorkFunction;
    public isConnected: (group: string) => boolean;

    /**
     * @param connector
     * @param options
     */
    constructor(
        connector: {
            /**
             * @description
             * The send function should send the message to every socket of the specific group once.
             */
            send?: GroupSendFunction
            cork?: GroupCorkFunction
            /**
             * @description
             * Should return a boolean that indicates if the underlying group source is completely connected.
             * When the underlying source does not have a disconnected state,
             * the function can always return true.
             * Notice that you should try to flush the group buffer on reconnection of
             * the group underlying source when the source has a temporarily disconnected state.
             */
            isConnected?: (group: string) => boolean
        } = {},
        /**
         * Notice that the provided options will not be cloned to save memory and performance.
         */
        public options: DynamicGroupTransportOptions = {...DynamicGroupTransport.DEFAULT_OPTIONS}
    ) {
        this.send = connector.send || (() => {});
        this.cork = connector.cork || ((group,cb) => cb());
        this.isConnected = connector.isConnected || (() => true);
    }

    public static readonly DEFAULT_OPTIONS: Readonly<DynamicGroupTransportOptions> = {
        freeBufferMaxPoolSize: 100,
        ...PackageBuffer.DEFAULT_OPTIONS
    };

    public static buildOptions(options: Partial<DynamicGroupTransportOptions>): DynamicGroupTransportOptions {
        return loadDefaults(options,DynamicGroupTransport.DEFAULT_OPTIONS);
    }

    private _freeBuffers: PackageBuffer[] = [];
    private _usedBuffers: Map<string,PackageBuffer> = new Map<string, PackageBuffer>();

    private getGroupBuffer(group: string): PackageBuffer {
        let buffer = this._usedBuffers.get(group);
        if(!buffer) {
            if(this._freeBuffers.length > 0)
                buffer = this._freeBuffers.pop();
            else buffer = new PackageBuffer(FREE_BUFFER_SEND_PLACEHOLDER,ALWAYS_OPEN,this.options);
            buffer!.afterFlush = () => this._freeGroupBuffer(group);
            buffer!.send = (messages,batches) =>
                this._multiSend(group,messages,batches);
            buffer!.isOpen = () => this.isConnected(group);
            this._usedBuffers.set(group,buffer!);
        }
        return buffer!;
    }

    private _freeGroupBuffer(group: string) {
        const buffer = this._usedBuffers.get(group);
        if(!buffer || buffer.getBufferLength() > 0) return false;

        buffer.send = FREE_BUFFER_SEND_PLACEHOLDER;
        buffer.isOpen = ALWAYS_OPEN;
        this._usedBuffers.delete(group);

        if(this._freeBuffers.length < this.options.freeBufferMaxPoolSize)
            this._freeBuffers.push(buffer);
        else buffer.afterFlush = EMPTY_FUNCTION;
        return true;
    }

    private _multiSend(group: string,messages: (string | ArrayBuffer)[],batches: boolean) {
        const len = messages.length;
        if(len > 1) this.cork(group,() => {
            for(let i = 0; i < len; i++)
                this.send(group,messages[i],typeof messages[i] === 'object',batches);
        });
        else if(len === 1) this.send(group,messages[0],typeof messages[0] === 'object',batches);
    }

    private _directSendMultiTransmit(group: string,pack: Package) {
        if(pack.length > 1) this.cork(group,() => {
            this.send(group,pack[0]);
            this.send(group,pack[1]!, true);
        })
        else this.send(group,pack[0]);
        if(pack._afterSend) pack._afterSend();
    }

    /**
     * Sends a transmit.
     * Notice that internally the prepareMultiTransmit method is used.
     * This method does not support streams, but binaries are supported.
     * @param group
     * @param receiver
     * @param data
     * @param options
     */
    transmit(group: string,receiver: string, data?: any, options: BatchOption & ComplexTypesOption = {}) {
        const pack = Transport.prepareMultiTransmit(receiver,data,options);
        if(!this.isConnected(group)) this.getGroupBuffer(group).add(pack);
        else if(isBatched(options.batch)) this.getGroupBuffer(group).add(pack,options.batch);
        else this._directSendMultiTransmit(group,pack);
    }

    /**
     * @description
     * Flushes all buffers.
     * @return If at least one buffer is flushed.
     */
    flushBuffers(): boolean {
        let flushedOne = false;
        for(const buffer of this._usedBuffers.values())
            flushedOne = buffer.flushBuffer() || flushedOne;
        return flushedOne;
    }

    /**
     * @description
     * Flushes a buffer of a specific group.
     * @param group
     * @return If the buffer is flushed.
     */
    flushBuffer(group: string): boolean {
        const buffer = this._usedBuffers.get(group);
        if(!buffer) return false;
        return buffer.flushBuffer();
    }
}