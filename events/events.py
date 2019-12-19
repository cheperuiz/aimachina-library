# pylint: disable=import-error
# pylint: disable=no-name-in-module
import time
from enum import Enum, auto
from dataclasses import dataclass, field
from PIL import Image

# pylint: enable=import-error

from utils.common import uuid_factory


class EventType(Enum):
    # Generic
    GENERIC_EVENT = auto()
    GENERIC_MODEL_CREATED = auto()
    GENERIC_MODEL_DELETED = auto()
    GENERIC_MODEL_UPDATED = auto()

    # Metrics
    METRICS_EVENT = auto()
    # Streams
    STREAM_CREATED = auto()
    STREAM_DELETED = auto()
    STREAM_UPDATED = auto()
    FRAMEPRODUCER_STARTED = auto()
    FRAMEPRODUCER_STOPPED = auto()
    FRAMEPRODUCER_FAILED = auto()
    # Frames
    FRAME_GRABBED = auto()
    FRAMEPRODUCER_METRICS = auto()
    # Detector
    OBJECT_DETECTED = auto()  # Detection
    OBJECTS_DETECTED = auto()  # DetectionsList
    # Encoder
    OBJECT_ENCODED = auto()  # Detection
    OBJECTS_ENCODED = auto()  # DetectionsList
    # Tracker
    OBJECT_TRACKING_ACTIVE = auto()
    OBJECT_TRACKING_MERGED = auto()
    OBJECT_TRACKING_ENDED = auto()
    # Trackable objects
    TRACKABLE_CREATED = auto()
    TRACKABLE_UPDATED = auto()
    TRACKABLE_DELETED = auto()
    TRACKABLE_MATCHING_SET_ACTIVE = auto()
    TRACKABLE_MATCHING_SET_INACTIVE = auto()

    MATCH_FOUND = auto()
    # Files
    FILES_UPLOADED = auto()
    # Transactions
    TRANSACTIONS_LOADED = auto()

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
    prefix: str = "GENERIC"
    generic_model_data: dict = field(default_factory=dict)


@dataclass
class MetricsEvent(BaseEvent):
    metrics: dict = field(default_factory=dict)
    prefix: str = "METRICS"

    def __init__(self, metrics: dict, event_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metrics = metrics
        self.event_type = event_type or EventType.METRICS_EVENT


@dataclass
class StreamEvent(BaseEvent):
    stream_data: dict = field(default_factory=dict)
    prefix = "STREAM"

    def __init__(self, stream_data: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream_data = stream_data


@dataclass
class FrameEvent(BaseEvent):
    prefix: str = "FRAME"
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
    prefix: str = "DETECTION"
    detections: dict = field(default_factory=dict)

    def __init__(self, detections: dict, correlations: dict = {}, event_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.detections = detections
        self.uuid = detections["id"]
        self.event_type = event_type or EventType.OBJECT_DETECTED
        self.correlations = correlations


@dataclass
class EmbeddingEvent(BaseEvent):
    prefix: str = "EMBEDDING"
    embeddings: list = field(default_factory=dict)

    def __init__(self, embeddings: dict, correlations: dict = {}, event_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.embeddings = embeddings
        self.uuid = embeddings["id"]
        self.event_type = event_type or EventType.OBJECT_ENCODED
        self.correlations = correlations


@dataclass
class TrackingEvent(BaseEvent):
    prefix: str = "TRACKING"
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
    prefix: str = "TRACKABLE"
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
    prefix: str = "MATCH"
    anchor: str = field(default_factory=str)
    test: str = field(default_factory=str)
    metrics: dict = field(default_factory=dict)

    def __init__(
        self,
        anchor: str,
        test: str,
        metrics: dict,
        correlations: dict = {},
        event_type: EventType = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.anchor = anchor
        self.test = test
        self.metrics = metrics
        self.event_type = event_type or EventType.MATCH_FOUND

@dataclass
class FileEvent(BaseEvent):
    prefix: str = "FILES"

    def __init__(
        self, route: str, event_type: EventType = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.route = route
        self.event_type = event_type or EventType.FILES_UPLOADED
