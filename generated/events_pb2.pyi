from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SessionStarted(_message.Message):
    __slots__ = ("session_id", "user_id", "track_id", "bitrate", "timestamp")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    TRACK_ID_FIELD_NUMBER: _ClassVar[int]
    BITRATE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    user_id: str
    track_id: str
    bitrate: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, session_id: _Optional[str] = ..., user_id: _Optional[str] = ..., track_id: _Optional[str] = ..., bitrate: _Optional[str] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ChunksAckEvent(_message.Message):
    __slots__ = ("session_id", "acked_chunk_count", "timestamp")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ACKED_CHUNK_COUNT_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    acked_chunk_count: int
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, session_id: _Optional[str] = ..., acked_chunk_count: _Optional[int] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class BitrateChangedEvent(_message.Message):
    __slots__ = ("session_id", "new_bitrate", "timestamp")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    NEW_BITRATE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    new_bitrate: int
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, session_id: _Optional[str] = ..., new_bitrate: _Optional[int] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class OffsetChangedEvent(_message.Message):
    __slots__ = ("session_id", "new_chunk_offset", "old_chunk_offset", "timestamp")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    NEW_CHUNK_OFFSET_FIELD_NUMBER: _ClassVar[int]
    OLD_CHUNK_OFFSET_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    new_chunk_offset: int
    old_chunk_offset: int
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, session_id: _Optional[str] = ..., new_chunk_offset: _Optional[int] = ..., old_chunk_offset: _Optional[int] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SessionPaused(_message.Message):
    __slots__ = ("session_id", "timestamp")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, session_id: _Optional[str] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SessionResumed(_message.Message):
    __slots__ = ("session_id", "timestamp")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, session_id: _Optional[str] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SessionStopped(_message.Message):
    __slots__ = ("session_id", "total_chunks_sent", "timestamp")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TOTAL_CHUNKS_SENT_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    total_chunks_sent: int
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, session_id: _Optional[str] = ..., total_chunks_sent: _Optional[int] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
