#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from ..operators.map import MapStream as Map
from ..operators.filter import FilterStream as Filter
from ..operators.flatmap import FlatMapStream as FlatMap
from ..operators.flatmapiterable import FlatMapIterableStream as FlatMapIterable
from ..operators.input import InputStream as Input, InputKVStream as InputKV
from ..operators.join import JoinStream as Join, JoinLink
from ..operators.keyby import KeyByStream as KeyBy
from ..operators.link import LinkStream as Link
from ..operators.merge import MergeStream as Merge
from ..operators.multijoin import MultiJoinStream as MultiJoin, MultiJoinLink, MultiJoinLinkStream
from ..operators.process import ProcessStream as Process
from ..operators.delay import DelayStream as Delay
from ..operators.sink import SinkStream as Sink, SinkStreamWithResult as SinkWithResult
from ..operators.case import CaseStream as Case, WhenStream as When
from ..operators.split import SplitStream as Split
from ..operators.error import ErrorStream
from ..operators.functions import (
    MapFunction, MapHandler,
    FilterFunction, FilterHandler,
    FlatMapFunction, FlatMapHandler,
    JoinFunction, JoinHandler,
    MultiJoinFunction, MultiJoinHandler,
    KeyByFunction, KeyByHandler,
    ProcessFunction, ProcessHandler,
    DelayFunction,
    When as WhenInterface,
    BuildSwitchFunction,
    BuildSwitchHandler,
    default_build_switch,
)
from ..runtime.config.stream_types import (
    InputStreamConfig,
    MapStreamConfig,
    FilterStreamConfig,
    FlatMapStreamConfig,
    FlatMapIterableStreamConfig,
    JoinStreamConfig,
    MultiJoinStreamConfig,
    ProcessStreamConfig,
    KeyByStreamConfig,
    MergeStreamConfig,
    SplitStreamConfig,
    DelayStreamConfig,
    SinkStreamConfig,
    CycleLinkStreamConfig,
    CaseStreamConfig,
    WhenStreamConfig,
)
