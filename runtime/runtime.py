#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#  file for details.
#

from abc import abstractmethod
from pyservicelib.runtime import StreamExecutionEnvironment, ServiceStream

class StreamExecutionRuntime(StreamExecutionEnvironment):

    @abstractmethod
    def register_stream(self, stream: ServiceStream):
        pass