/*
Author: Ing. Luca Gian Scaringella
GitHub: LucaCode
Copyright(c) Ing. Luca Gian Scaringella
 */

import {DataType} from "./DataType";
import {MAX_UINT_32} from "./Utils";

export const PING_UINT8 = 57;
export const PING = new Uint8Array([PING_UINT8]);
export const PONG_UINT8 = 65;
export const PONG = new Uint8Array([PONG_UINT8]);

export const NEXT_BINARIES_PACKET_TOKEN = MAX_UINT_32;

export const enum PacketType {
    Bundle,
    Transmit,
    Invoke,
    InvokeDataResp,
    InvokeErrResp,
    BinaryContent,
    StreamAccept,
    StreamChunk,
    StreamEnd,
    StreamDataPermission,
    WriteStreamClose,
    ReadStreamClose,
}

/**
 * Indexes:
 * 0: PacketType
 * 1: Receiver
 * 2: DataType
 * 3?: Data
 * 4?: Data-Meta-information
 * (Only provided when data is also provided (Could be a binaries' packet id))
 */
export type TransmitPacket = [PacketType.Transmit,string,DataType,any,any];

/**
 * Indexes:
 * 0: PacketType
 * 1: Procedure
 * 2: CallId
 * 3: DataType
 * 4?: Data
 * 5?: Data-Meta-information
 * (Only provided when data is also provided (Could be a binaries' packet id))
 */
export type InvokePacket = [PacketType.Invoke,string,number,DataType,any,any];

/**
 * Indexes:
 * 0: PacketType
 * 1: CallId
 * 2: DataType
 * 3?: Data
 * 4?: Data-Meta-information
 * (Only provided when data is also provided (Could be a binaries' packet id))
 */
export type InvokeDataRespPacket = [PacketType.InvokeDataResp,number,DataType,any,any];

/**
 * Indexes:
 * 0: PacketType
 * 1: CallId
 * 2: Err
 */
export type InvokeErrRespPacket = [PacketType.InvokeErrResp,number,any];

/**
 * Indexes:
 * 0: PacketType
 * 1: StreamId
 * 2: Buffer size (size is also allowed)
 */
export type StreamAcceptPacket = [PacketType.StreamAccept,number,number];

/**
 * Indexes:
 * 0: PacketType
 * 1: StreamId
 * 2: DataType
 * 3?: Data
 * 4?: Data-Meta-information
 * (Only provided when data is also provided (Could be a binaries' packet id))
 */
export type StreamChunkPacket = [PacketType.StreamChunk,number,DataType,any,any];

/**
 * Indexes:
 * 0: PacketType
 * 1: StreamId
 * 2?: DataType
 * 3?: Data
 * 4?: Data-Meta-information
 * (Only provided when data is also provided (Could be a binaries' packet id))
 */
export type StreamEndPacket = [PacketType.StreamEnd,number,DataType?,any?,any?];

/**
 * Indexes:
 * 0: PacketType
 * 1: StreamId
 * 2: Allowed size
 */
export type StreamDataPermissionPacket = [PacketType.StreamDataPermission,number,number];

/**
 * Indexes:
 * 0: PacketType
 * 1: StreamId
 * 2: Close code
 */
export type WriteStreamClosePacket = [PacketType.WriteStreamClose,number,number];

/**
 * Indexes:
 * 0: PacketType
 * 1: StreamId
 * 2: Close code (undefined means 200)
 */
export type ReadStreamClosePacket = [PacketType.ReadStreamClose,number,number?];

export type ActionPacket = TransmitPacket | InvokePacket | InvokeErrRespPacket |
    InvokeDataRespPacket | StreamAcceptPacket | StreamDataPermissionPacket |
    StreamChunkPacket | StreamEndPacket |
    WriteStreamClosePacket | ReadStreamClosePacket ;
export type BundlePacket = [PacketType.Bundle,ActionPacket[]];