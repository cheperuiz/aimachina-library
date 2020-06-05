# pylint: disable=import-error
# pylint: disable=no-name-in-module
import time
from enum import Enum
from dataclasses import dataclass, field
from PIL import Image

# pylint: enable=import-error

from utils.common import uuid_factory


class EventType(Enum):
    # Generic
    GENERIC_EVENT = 1
    GENERIC_MODEL_CREATED = 2
    GENERIC_MODEL_DELETED = 3
    GENERIC_MODEL_UPDATED = 4

    # Metrics
    METRICS_EVENT = 5

    # Streams
    STREAM_CREATED = 6
    STREAM_DELETED = 7
    STREAM_UPDATED = 8
    FRAMEPRODUCER_STARTED = 9
    FRAMEPRODUCER_STOPPED = 10
    FRAMEPRODUCER_FAILED = 11

    # Frames
    FRAME_GRABBED = 12
    FRAMEPRODUCER_METRICS = 13

    # Detector
    OBJECT_DETECTED = 14  # Detection
    OBJECTS_DETECTED = 15  # DetectionsList

    # Encoder
    OBJECT_ENCODED = 16  # Detection
    OBJECTS_ENCODED = 17  # DetectionsList

    # Tracker
    OBJECT_TRACKING_ACTIVE = 18
    OBJECT_TRACKING_MERGED = 19
    OBJECT_TRACKING_ENDED = 20

    # Trackable objects
    TRACKABLE_CREATED = 21
    TRACKABLE_UPDATED = 22
    TRACKABLE_DELETED = 23
    TRACKABLE_MATCHING_SET_ACTIVE = 24
    TRACKABLE_MATCHING_SET_INACTIVE = 25
    # Matcher
    MATCH_FOUND = 26

    # Files
    FILES_UPLOADED = 26
    IMAGE_UPLOADED = 27
    EXCEL_UPLOADED = 28
    ARCHIVE_UPLOADED = 29

    # Transactions
    TRANSACTION_LOADED = 30
    RECEIPT_CREATED = 31

    # OCR
    TEXT_DETECTED = 32
    TEXT_UPDATED = 33

    # Documents
    DOCUMENT_CREATED = 34
    DOCUMENT_INDEXED = 35
    DOCUMENT_UPDATED = 36
    DOCUMENT_DELETED = 37
    DOCUMENT_DEL_INDEX = 38

    # Transaction Indexed
    TRANSACTION_INDEXED = 39


@dataclass
class BaseEvent:
    timestamp: float = field(default=None)
    uuid: str = field(default=None)
    event_type: EventType = field(default=None)
    correlations: dict = field(default_factory=dict)

    def __post_init__(self):
        self.timestamp = self.timestamp or time.time()
        # pylint: disable=no-member
        self.uuid = self.uuid or uuid_factory(self.prefix or "EVENT")()
        self.event_type = self.event_type or EventType.GENERIC_EVENT

    def update_correlations(self, correlations):
        # pylint: disable=no-member
        new_correlations = self.correlations.copy()
        new_correlations.update(correlations)
        return new_correlations


@dataclass
class GenericEvent(BaseEvent):
    prefix: str = "GENERIC-EVENT"
    generic_model_data: dict = field(default_factory=dict)


@dataclass
class MetricsEvent(BaseEvent):
    metrics: dict = field(default_factory=dict)
    prefix: str = "METRICS-EVENT"

    def __init__(self, metrics: dict, event_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metrics = metrics
        self.event_type = event_type or EventType.METRICS_EVENT


@dataclass
class StreamEvent(BaseEvent):
    stream_data: dict = field(default_factory=dict)
    prefix = "STREAM-EVENT"

    def __init__(self, stream_data: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream_data = stream_data


@dataclass
class FrameEvent(BaseEvent):
    prefix: str = "FRAME-EVENT"
    frame: Image = field(default_factory=None)
    worker_id: str = field(default_factory=None)
    stream_id: str = field(default_factory=None)

    def __init__(self, frame: Image, worker_id: str, stream_id: str, event_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.frame = frame
        self.worker_id = worker_id
        self.stream_id = stream_id
        self.event_type = event_type or EventType.FRAME_GRABBED


@dataclass
class DetectionEvent(BaseEvent):
    prefix: str = "DETECTION-EVENT"
    detections: dict = field(default_factory=dict)

    def __init__(self, detections: dict, correlations: dict = {}, event_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.detections = detections
        self.uuid = detections.get("id", None) or detections["uuid"]
        self.event_type = event_type or EventType.OBJECT_DETECTED
        self.correlations = correlations


@dataclass
class EmbeddingEvent(BaseEvent):
    prefix: str = "EMBEDDING-EVENT"
    embeddings: list = field(default_factory=dict)

    def __init__(self, embeddings: dict, correlations: dict = {}, event_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.embeddings = embeddings
        self.uuid = embeddings["id"]
        self.event_type = event_type or EventType.OBJECT_ENCODED
        self.correlations = correlations


@dataclass
class TrackingEvent(BaseEvent):
    prefix: str = "TRACKING-EVENT"
    detection: dict = field(default_factory=dict)
    replaced: dict = field(default_factory=dict)

    def __init__(
        self, detection: dict, replaced: dict, correlations: dict = {}, event_type=None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.detection = detection
        self.replaced = replaced
        self.event_type = event_type or EventType.OBJECT_TRACKING_ACTIVE
        self.correlations = correlations


@dataclass
class TrackableEvent(BaseEvent):
    prefix: str = "TRACKABLE-EVENT"
    trackable: dict = field(default_factory=dict)

    def __init__(
        self, trackable: dict, correlations: dict = {}, event_type: EventType = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.trackable = trackable
        self.correlations = correlations
        self.event_type = event_type or EventType.TRACKABLE_CREATED


@dataclass
class MatchEvent(BaseEvent):
    prefix: str = "MATCH-EVENT"
    anchor: str = field(default_factory=str)
    test: str = field(default_factory=str)
    metrics: dict = field(default_factory=dict)

    def __init__(
        self, test: str, matches: dict, correlations: dict = {}, event_type: EventType = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.test = test
        self.matches = matches
        self.correlations = correlations
        self.event_type = event_type or EventType.MATCH_FOUND


@dataclass
class FileEvent(BaseEvent):
    prefix: str = "FILES-EVENT"

    def __init__(
        self, route: str, user_id: str, event_type: EventType = None, data: bytes = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.user_id = user_id
        self.route = route
        self.data = data
        self.event_type = event_type or EventType.FILES_UPLOADED


@dataclass
class ReceiptEvent(BaseEvent):
    prefix: str = "RECEIPT-EVENT"

    def __init__(self, receipt_data: dict, user_id: str, event_type: EventType = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_id = user_id
        self.receipt_data = receipt_data
        self.event_type = event_type or EventType.RECEIPT_CREATED


@dataclass
class TransactionEvent(BaseEvent):
    prefix: str = "TRANSACTION-EVENT"

    def __init__(self, transaction_data: dict, user_id: str, event_type: EventType = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_id = user_id
        self.transaction_data = transaction_data
        self.event_type = event_type or EventType.TRANSACTION_LOADED


@dataclass
class DocumentEvent(BaseEvent):
    prefix: str = "DOCUMENT-EVENT"
    document_id: str = field(default_factory=str)
    signature: str = field(default_factory=str)
    data: dict = field(default_factory=dict)
    user_id: str = field(default="")

    def __init__(
        self,
        document_id: str,
        signature: str = "",
        correlations: dict = {},
        data: dict = {},
        user_id: str = "",
        event_type: EventType = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.signature = signature
        self.data = data
        self.user_id = user_id
        self.document_id = document_id
        self.correlations = correlations
        self.event_type = event_type or EventType.DOCUMENT_CREATED
