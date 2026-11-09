/*
Author: Ing. Luca Gian Scaringella
GitHub: LucaCode
Copyright(c) Ing. Luca Gian Scaringella
 */

import PackageBuffer, {PackageBufferOptions} from "./PackageBuffer";
import {Package} from "./Package";
import Transport from "./Transport";
import {CorkFunction, loadDefaults, SendFunction} from "./Utils";
import {BatchOption, ComplexTypesOption, isBatched} from "./Options";

export interface GroupTransportOptions extends PackageBufferOptions {}

/**
 * @description
 * This util class helps to build a transport for multiple end sources.
 * Instead of using the transport prepareMultiTransmit method and sending the
 * package with each transporter, the batching is not individual for
 * each transport and shared for the group.
 * If you want to manage multiple groups dynamically,
 * you should look at the DynamicGroupTransport class.
 * When the underlying source has a temporarily disconnected state,
 * the buffer should be flushed on a reconnection.
 */
export default class GroupTransport {

    public readonly buffer: PackageBuffer;

    public send: SendFunction;
    public cork: CorkFunction;
    public isConnected: () => boolean;

    /**
     * @param connector
     * @param options
     */
    constructor(
        connector: {
            /**
             * @description
             * The send function should send the message to every socket of the group once.
             */
            send?: SendFunction
            cork?: CorkFunction
            /**
             * @description
             * Should return a boolean that indicates if the underlying source is completely connected.
             * When the underlying source does not have a disconnected state,
             * the function can always return true.
             * Notice that you should flush the buffer on a reconnection
             * when the underlying source has a temporarily disconnected state.
             */
            isConnected?: () => boolean
        } = {},
        /**
         * Notice that the provided options will not be cloned to save memory and performance.
         */
        public options: GroupTransportOptions = {...GroupTransport.DEFAULT_OPTIONS}
    ) {
        this.send = connector.send || (() => {});
        this.cork = connector.cork || (cb => cb());
        this.isConnected = connector.isConnected || (() => true);
        this.buffer = new PackageBuffer(this._multiSend.bind(this),
            () => this.isConnected(),options);
    }

    public static readonly DEFAULT_OPTIONS: Readonly<GroupTransportOptions> = PackageBuffer.DEFAULT_OPTIONS;

    public static buildOptions(options: Partial<GroupTransportOptions>): GroupTransportOptions {
        return loadDefaults(options,GroupTransport.DEFAULT_OPTIONS);
    }

    private _multiSend(messages: (string | ArrayBuffer)[],batches: boolean) {
        const len = messages.length;
        if(len > 1) this.cork(() => {
            for(let i = 0; i < len; i++)
                this.send(messages[i],typeof messages[i] === 'object',batches);
        });
        else if(len === 1) this.send(messages[0],typeof messages[0] === 'object',batches);
    }

    private _directSendMultiTransmit(pack: Package) {
        if(pack.length > 1) this.cork(() => {
            this.send(pack[0]);
            this.send(pack[1]!, true);
        })
        else this.send(pack[0]);
        if(pack._afterSend) pack._afterSend();
    }

    /**
     * Sends a transmit.
     * Notice that internally the prepareMultiTransmit method is used.
     * This method does not support streams, but binaries are supported.
     * @param receiver
     * @param data
     * @param options
     */
    transmit(receiver: string, data?: any, options: BatchOption & ComplexTypesOption = {}) {
        const pack = Transport.prepareMultiTransmit(receiver,data,options);
        if(!this.isConnected()) this.buffer.add(pack);
        else if(isBatched(options.batch)) this.buffer.add(pack,options.batch);
        else this._directSendMultiTransmit(pack);
    }
}