/*
Author: Ing. Luca Gian Scaringella
GitHub: LucaCode
Copyright(c) Ing. Luca Gian Scaringella
 */

import {JSONString} from "./lib/JsonUtils";
import WriteStream from "./lib/streams/WriteStream";
import Transport, {
    TransmitListener,
    InvokeListener,
    TransportOptions
} from "./lib/Transport";
import ReadStream, {ChunkMiddleware} from "./lib/streams/ReadStream";
import {StreamCloseCode} from "./lib/streams/StreamCloseCode";
import {StreamState} from "./lib/streams/StreamState";
import {DataType, analyseTypeofData, containsStreams, containsBinaries, isMixedJSONDataType} from "./lib/DataType";
import {
    TimeoutError,
    TimeoutType,
    InvalidActionError,
    BadConnectionError,
    BadConnectionType,
    BackError,
    InsufficientBufferSizeError, MaxSupportedArrayBufferSizeExceededError
} from "./lib/Errors";
import {dehydrateError, hydrateError} from "./lib/ErrorUtils";
import StreamCloseError from "./lib/streams/StreamCloseError";
import { Package } from "./lib/Package";
import PackageBuffer, {PackageBufferOptions} from "./lib/PackageBuffer";
import GroupTransport, {GroupTransportOptions} from "./lib/GroupTransport";
import {MAX_SUPPORTED_ARRAY_BUFFER_SIZE, sendPackage} from "./lib/Utils";
import {PING, PONG} from "./lib/Protocol";
import {BatchOption, BatchOptionsValue, ComplexTypesOption, isBatched, ResponseTimeoutOption, ReturnDataTypeOption} from "./lib/Options";
import DynamicGroupTransport, {DynamicGroupTransportOptions} from "./lib/DynamicGroupTransport";

export {
    Transport,
    TransportOptions,
    GroupTransport,
    GroupTransportOptions,
    DynamicGroupTransport,
    DynamicGroupTransportOptions,
    PackageBuffer,
    PackageBufferOptions,
    InsufficientBufferSizeError,
    MaxSupportedArrayBufferSizeExceededError,
    MAX_SUPPORTED_ARRAY_BUFFER_SIZE,
    JSONString,
    WriteStream,
    ReadStream,
    ChunkMiddleware,
    StreamCloseError,
    StreamCloseCode,
    StreamState,
    TimeoutError,
    TimeoutType,
    InvalidActionError,
    BadConnectionError,
    BadConnectionType,
    analyseTypeofData,
    containsStreams,
    containsBinaries,
    isMixedJSONDataType,
    DataType,
    TransmitListener,
    InvokeListener,
    Package,
    ComplexTypesOption,
    BatchOption,
    BatchOptionsValue,
    isBatched,
    ResponseTimeoutOption,
    ReturnDataTypeOption,
    hydrateError,
    dehydrateError,
    BackError,
    PING,
    PONG,
    sendPackage
}