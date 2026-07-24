"""Generated gRPC transport adapter. DO NOT EDIT."""

from typing import Awaitable, Callable

import grpc
from inventory_service_api.generated import processorderitem_pb2, processorderitem_pb2_grpc
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.environment import Lifecycle


Request = processorderitem_pb2.ProcessOrderItemRequest
Response = processorderitem_pb2.ProcessOrderItemResponse
UnaryHandler = Callable[
    [Request, grpc.aio.ServicerContext[Request, Response]],
    Awaitable[Response],
]


class _InventoryApiServicer(processorderitem_pb2_grpc.InventoryServiceApiServicer):
    def __init__(self, handler: UnaryHandler):
        self._handler = handler

    async def ProcessOrderItem(
        self,
        request: Request,
        context: grpc.aio.ServicerContext[Request, Response],
    ) -> Response:
        return await self._handler(request, context)


class GrpcServer(Lifecycle):
    def __init__(self, host: str, port: int, handler: UnaryHandler):
        self._server = grpc.aio.server()
        processorderitem_pb2_grpc.add_InventoryServiceApiServicer_to_server(
            _InventoryApiServicer(handler),
            self._server,
        )
        self._server.add_insecure_port(f"{host}:{port}")

    async def start(self, ctx: Context) -> None:
        del ctx
        await self._server.start()

    async def stop(self, ctx: Context) -> None:
        await self._server.stop(grace=ctx.time_left)
