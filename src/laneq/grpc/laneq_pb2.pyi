from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Priority(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PRIORITY_UNSPECIFIED: _ClassVar[Priority]
    PRIORITY_P0: _ClassVar[Priority]
    PRIORITY_P1: _ClassVar[Priority]
    PRIORITY_P2: _ClassVar[Priority]

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STATUS_UNSPECIFIED: _ClassVar[Status]
    STATUS_PENDING: _ClassVar[Status]
    STATUS_TAKEN: _ClassVar[Status]
    STATUS_DEFERRED: _ClassVar[Status]
    STATUS_DONE: _ClassVar[Status]
    STATUS_DROPPED: _ClassVar[Status]
    STATUS_PARKED: _ClassVar[Status]
PRIORITY_UNSPECIFIED: Priority
PRIORITY_P0: Priority
PRIORITY_P1: Priority
PRIORITY_P2: Priority
STATUS_UNSPECIFIED: Status
STATUS_PENDING: Status
STATUS_TAKEN: Status
STATUS_DEFERRED: Status
STATUS_DONE: Status
STATUS_DROPPED: Status
STATUS_PARKED: Status

class Directive(_message.Message):
    __slots__ = ("id", "priority", "body", "status", "lane", "created_at_unix", "taken_at_unix", "done_at_unix", "taken_by", "lease_until_unix", "requeue_count", "parent_id", "not_before_unix", "blocked_by")
    ID_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LANE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    TAKEN_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    DONE_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    TAKEN_BY_FIELD_NUMBER: _ClassVar[int]
    LEASE_UNTIL_UNIX_FIELD_NUMBER: _ClassVar[int]
    REQUEUE_COUNT_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    NOT_BEFORE_UNIX_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_BY_FIELD_NUMBER: _ClassVar[int]
    id: str
    priority: Priority
    body: str
    status: Status
    lane: str
    created_at_unix: int
    taken_at_unix: int
    done_at_unix: int
    taken_by: str
    lease_until_unix: int
    requeue_count: int
    parent_id: str
    not_before_unix: int
    blocked_by: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[str] = ..., priority: _Optional[_Union[Priority, str]] = ..., body: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ..., lane: _Optional[str] = ..., created_at_unix: _Optional[int] = ..., taken_at_unix: _Optional[int] = ..., done_at_unix: _Optional[int] = ..., taken_by: _Optional[str] = ..., lease_until_unix: _Optional[int] = ..., requeue_count: _Optional[int] = ..., parent_id: _Optional[str] = ..., not_before_unix: _Optional[int] = ..., blocked_by: _Optional[_Iterable[str]] = ...) -> None: ...

class PushRequest(_message.Message):
    __slots__ = ("body", "priority", "parent_id", "lane")
    BODY_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    LANE_FIELD_NUMBER: _ClassVar[int]
    body: str
    priority: Priority
    parent_id: str
    lane: str
    def __init__(self, body: _Optional[str] = ..., priority: _Optional[_Union[Priority, str]] = ..., parent_id: _Optional[str] = ..., lane: _Optional[str] = ...) -> None: ...

class PushResponse(_message.Message):
    __slots__ = ("id", "priority", "lane", "parent_id", "status", "summary")
    ID_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    LANE_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    id: str
    priority: Priority
    lane: str
    parent_id: str
    status: Status
    summary: str
    def __init__(self, id: _Optional[str] = ..., priority: _Optional[_Union[Priority, str]] = ..., lane: _Optional[str] = ..., parent_id: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ..., summary: _Optional[str] = ...) -> None: ...

class TakeRequest(_message.Message):
    __slots__ = ("consumer", "lane", "lease_duration_ms", "reap_stale_seconds")
    CONSUMER_FIELD_NUMBER: _ClassVar[int]
    LANE_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    REAP_STALE_SECONDS_FIELD_NUMBER: _ClassVar[int]
    consumer: str
    lane: str
    lease_duration_ms: int
    reap_stale_seconds: int
    def __init__(self, consumer: _Optional[str] = ..., lane: _Optional[str] = ..., lease_duration_ms: _Optional[int] = ..., reap_stale_seconds: _Optional[int] = ...) -> None: ...

class TakeResponse(_message.Message):
    __slots__ = ("directive", "consumer", "lane")
    DIRECTIVE_FIELD_NUMBER: _ClassVar[int]
    CONSUMER_FIELD_NUMBER: _ClassVar[int]
    LANE_FIELD_NUMBER: _ClassVar[int]
    directive: Directive
    consumer: str
    lane: str
    def __init__(self, directive: _Optional[_Union[Directive, _Mapping]] = ..., consumer: _Optional[str] = ..., lane: _Optional[str] = ...) -> None: ...

class PeekRequest(_message.Message):
    __slots__ = ("lane",)
    LANE_FIELD_NUMBER: _ClassVar[int]
    lane: str
    def __init__(self, lane: _Optional[str] = ...) -> None: ...

class PeekResponse(_message.Message):
    __slots__ = ("directive",)
    DIRECTIVE_FIELD_NUMBER: _ClassVar[int]
    directive: Directive
    def __init__(self, directive: _Optional[_Union[Directive, _Mapping]] = ...) -> None: ...

class ShowRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class ShowResponse(_message.Message):
    __slots__ = ("directive", "thread")
    DIRECTIVE_FIELD_NUMBER: _ClassVar[int]
    THREAD_FIELD_NUMBER: _ClassVar[int]
    directive: Directive
    thread: _containers.RepeatedCompositeFieldContainer[ThreadItem]
    def __init__(self, directive: _Optional[_Union[Directive, _Mapping]] = ..., thread: _Optional[_Iterable[_Union[ThreadItem, _Mapping]]] = ...) -> None: ...

class ThreadItem(_message.Message):
    __slots__ = ("id", "status", "created_at_unix")
    ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_FIELD_NUMBER: _ClassVar[int]
    id: str
    status: Status
    created_at_unix: int
    def __init__(self, id: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ..., created_at_unix: _Optional[int] = ...) -> None: ...

class ListingRequest(_message.Message):
    __slots__ = ("all_statuses", "lane", "thread")
    ALL_STATUSES_FIELD_NUMBER: _ClassVar[int]
    LANE_FIELD_NUMBER: _ClassVar[int]
    THREAD_FIELD_NUMBER: _ClassVar[int]
    all_statuses: bool
    lane: str
    thread: str
    def __init__(self, all_statuses: _Optional[bool] = ..., lane: _Optional[str] = ..., thread: _Optional[str] = ...) -> None: ...

class ListingResponse(_message.Message):
    __slots__ = ("directives",)
    DIRECTIVES_FIELD_NUMBER: _ClassVar[int]
    directives: _containers.RepeatedCompositeFieldContainer[Directive]
    def __init__(self, directives: _Optional[_Iterable[_Union[Directive, _Mapping]]] = ...) -> None: ...

class ReprioritizeRequest(_message.Message):
    __slots__ = ("id", "priority")
    ID_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    id: str
    priority: Priority
    def __init__(self, id: _Optional[str] = ..., priority: _Optional[_Union[Priority, str]] = ...) -> None: ...

class ReprioritizeResponse(_message.Message):
    __slots__ = ("id", "priority")
    ID_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    id: str
    priority: Priority
    def __init__(self, id: _Optional[str] = ..., priority: _Optional[_Union[Priority, str]] = ...) -> None: ...

class SetStatusRequest(_message.Message):
    __slots__ = ("id", "status")
    ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: str
    status: Status
    def __init__(self, id: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ...) -> None: ...

class SetStatusResponse(_message.Message):
    __slots__ = ("id", "status")
    ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: str
    status: Status
    def __init__(self, id: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ...) -> None: ...

class DeferRequest(_message.Message):
    __slots__ = ("id", "until_unix", "delay_ms", "blocked_by")
    ID_FIELD_NUMBER: _ClassVar[int]
    UNTIL_UNIX_FIELD_NUMBER: _ClassVar[int]
    DELAY_MS_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_BY_FIELD_NUMBER: _ClassVar[int]
    id: str
    until_unix: int
    delay_ms: int
    blocked_by: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[str] = ..., until_unix: _Optional[int] = ..., delay_ms: _Optional[int] = ..., blocked_by: _Optional[_Iterable[str]] = ...) -> None: ...

class DeferResponse(_message.Message):
    __slots__ = ("id", "status", "not_before_unix", "blocked_by")
    ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    NOT_BEFORE_UNIX_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_BY_FIELD_NUMBER: _ClassVar[int]
    id: str
    status: Status
    not_before_unix: int
    blocked_by: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ..., not_before_unix: _Optional[int] = ..., blocked_by: _Optional[_Iterable[str]] = ...) -> None: ...

class TouchRequest(_message.Message):
    __slots__ = ("id", "consumer", "lease_duration_ms")
    ID_FIELD_NUMBER: _ClassVar[int]
    CONSUMER_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    id: str
    consumer: str
    lease_duration_ms: int
    def __init__(self, id: _Optional[str] = ..., consumer: _Optional[str] = ..., lease_duration_ms: _Optional[int] = ...) -> None: ...

class TouchResponse(_message.Message):
    __slots__ = ("id", "lease_until_unix")
    ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_UNTIL_UNIX_FIELD_NUMBER: _ClassVar[int]
    id: str
    lease_until_unix: int
    def __init__(self, id: _Optional[str] = ..., lease_until_unix: _Optional[int] = ...) -> None: ...

class ReapRequest(_message.Message):
    __slots__ = ("expired_leases", "stale_seconds")
    EXPIRED_LEASES_FIELD_NUMBER: _ClassVar[int]
    STALE_SECONDS_FIELD_NUMBER: _ClassVar[int]
    expired_leases: bool
    stale_seconds: int
    def __init__(self, expired_leases: _Optional[bool] = ..., stale_seconds: _Optional[int] = ...) -> None: ...

class ReapResponse(_message.Message):
    __slots__ = ("mode", "reclaimed", "detail")
    MODE_FIELD_NUMBER: _ClassVar[int]
    RECLAIMED_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    mode: str
    reclaimed: int
    detail: str
    def __init__(self, mode: _Optional[str] = ..., reclaimed: _Optional[int] = ..., detail: _Optional[str] = ...) -> None: ...

class StatsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StatsResponse(_message.Message):
    __slots__ = ("by_status", "consumers")
    class ByStatusEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    BY_STATUS_FIELD_NUMBER: _ClassVar[int]
    CONSUMERS_FIELD_NUMBER: _ClassVar[int]
    by_status: _containers.ScalarMap[str, int]
    consumers: _containers.RepeatedCompositeFieldContainer[ConsumerStats]
    def __init__(self, by_status: _Optional[_Mapping[str, int]] = ..., consumers: _Optional[_Iterable[_Union[ConsumerStats, _Mapping]]] = ...) -> None: ...

class ConsumerStats(_message.Message):
    __slots__ = ("consumer", "active_leases", "total_claimed", "total_completed")
    CONSUMER_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_LEASES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_CLAIMED_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    consumer: str
    active_leases: int
    total_claimed: int
    total_completed: int
    def __init__(self, consumer: _Optional[str] = ..., active_leases: _Optional[int] = ..., total_claimed: _Optional[int] = ..., total_completed: _Optional[int] = ...) -> None: ...

class ThreadStatusRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class ThreadStatusResponse(_message.Message):
    __slots__ = ("root", "status", "total", "open", "open_items")
    ROOT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    OPEN_FIELD_NUMBER: _ClassVar[int]
    OPEN_ITEMS_FIELD_NUMBER: _ClassVar[int]
    root: str
    status: Status
    total: int
    open: int
    open_items: _containers.RepeatedCompositeFieldContainer[ThreadItem]
    def __init__(self, root: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ..., total: _Optional[int] = ..., open: _Optional[int] = ..., open_items: _Optional[_Iterable[_Union[ThreadItem, _Mapping]]] = ...) -> None: ...

class ParkRequest(_message.Message):
    __slots__ = ("id", "consumer")
    ID_FIELD_NUMBER: _ClassVar[int]
    CONSUMER_FIELD_NUMBER: _ClassVar[int]
    id: str
    consumer: str
    def __init__(self, id: _Optional[str] = ..., consumer: _Optional[str] = ...) -> None: ...

class ParkResponse(_message.Message):
    __slots__ = ("id", "status")
    ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: str
    status: Status
    def __init__(self, id: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ...) -> None: ...

class UnparkRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class UnparkResponse(_message.Message):
    __slots__ = ("id", "status")
    ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: str
    status: Status
    def __init__(self, id: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ...) -> None: ...
