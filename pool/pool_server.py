import logging
import os
import ssl
import time
import traceback
from typing import Dict, Callable, Optional
import multiprocessing

from sanic import Sanic, Request, HTTPResponse, text
import yaml
from blspy import AugSchemeMPL, G2Element
from chia.protocols.pool_protocol import (
    PoolErrorCode,
    GetFarmerResponse,
    GetPoolInfoResponse,
    PostPartialRequest,
    PostFarmerRequest,
    PutFarmerRequest,
    validate_authentication_token,
    POOL_PROTOCOL_VERSION,
    AuthenticationPayload,
)
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.byte_types import hexstr_to_bytes
from chia.util.hash import std_hash
from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.consensus.constants import ConsensusConstants
from chia.util.ints import uint8, uint64, uint32
from chia.util.default_root import DEFAULT_ROOT_PATH
from chia.util.config import load_config

from .difficulty_adjustment import get_new_difficulty
from .record import FarmerRecord
from .pool import Pool
from .payment_manager.abstract import AbstractPaymentManager
from .store.abstract import AbstractPoolStore
from .util import error_response, RequestMetadata, sanic_jsonify

app = Sanic("Pool Server", configure_logging=True)
app.config.FORWARDED_SECRET = "34geggfdgdgdgertertgertetet5fgfdg4345431"


def allow_cors(response: HTTPResponse) -> HTTPResponse:
    response.headers.extend({"Access-Control-Allow-Origin": "*"})
    return response


def missing_argument() -> HTTPResponse:
    return error_response(
        PoolErrorCode.SERVER_EXCEPTION,
        f"Missing Required Argument",
    )


def check_authentication_token(launcher_id: bytes32, token: uint64, timeout: uint8) -> Optional[HTTPResponse]:
    if not validate_authentication_token(token, timeout):
        return error_response(
            PoolErrorCode.INVALID_AUTHENTICATION_TOKEN,
            f"authentication_token {token} invalid for farmer {launcher_id.hex()}.",
        )
    return None


def get_ssl_context(config):
    if config["server"]["server_use_ssl"] is False:
        return None
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(config["server"]["server_ssl_crt"], config["server"]["server_ssl_key"])
    return ssl_context


class PoolServer:
    def __init__(self, config: Dict, constants: ConsensusConstants, pool_store: Optional[AbstractPoolStore] = None,
                 difficulty_function: Callable = get_new_difficulty,
                 payment_manager: Optional[AbstractPaymentManager] = None):

        # We load our configurations from here
        with open(os.getcwd() + "/config.yaml") as f:
            pool_config: Dict = yaml.safe_load(f)

        self.log = logging.getLogger("sanic.root")
        self.pool = Pool(config, pool_config, constants, pool_store, difficulty_function, payment_manager)

        self.pool_config = pool_config
        self.host = pool_config["server"]["server_host"]
        self.port = int(pool_config["server"]["server_port"])

    async def start(self):
        await self.pool.start()

    async def stop(self):
        await self.pool.stop()

    def wrap_http_handler(self, f) -> Callable:
        async def inner(request) -> HTTPResponse:
            try:
                res_object = await f(request)
                if res_object is None:
                    res_object = {}
            except Exception as e:
                tb = traceback.format_exc()
                self.log.warning(f"Error while handling message: {tb}")
                if len(e.args) > 0:
                    res_error = error_response(PoolErrorCode.SERVER_EXCEPTION, f"{e.args[0]}")
                else:
                    res_error = error_response(PoolErrorCode.SERVER_EXCEPTION, f"{e}")
                return allow_cors(res_error)

            return allow_cors(res_object)

        return inner

    @staticmethod
    async def index(_) -> HTTPResponse:
        return text("Chia reference pool")

    async def get_pool_info(self, _) -> HTTPResponse:
        res: GetPoolInfoResponse = GetPoolInfoResponse(
            self.pool.info_name,
            self.pool.info_logo_url,
            uint64(self.pool.min_difficulty),
            uint32(self.pool.relative_lock_height),
            POOL_PROTOCOL_VERSION,
            str(self.pool.pool_fee),
            self.pool.info_description,
            self.pool.default_target_puzzle_hash,
            self.pool.authentication_token_timeout,
        )
        return sanic_jsonify(res)

    async def get_farmer(self, request_obj: Request) -> HTTPResponse:
        # TODO(pool): add rate limiting
        try:
            launcher_id: bytes32 = hexstr_to_bytes(request_obj.args.get("launcher_id"))
            authentication_token = uint64(request_obj.args.get("authentication_token"))
        except AttributeError:
            return missing_argument()

        authentication_token_error: Optional[HTTPResponse] = check_authentication_token(
            launcher_id, authentication_token, self.pool.authentication_token_timeout
        )
        if authentication_token_error is not None:
            return authentication_token_error

        farmer_record: Optional[FarmerRecord] = await self.pool.store.get_farmer_record(launcher_id)
        if farmer_record is None:
            return error_response(
                PoolErrorCode.FARMER_NOT_KNOWN, f"Farmer with launcher_id {launcher_id.hex()} unknown."
            )

        # Validate provided signature
        try:
            signature: G2Element = G2Element.from_bytes(hexstr_to_bytes(request_obj.args.get("signature")))
        except AttributeError:
            return missing_argument()
        message: bytes32 = std_hash(
            AuthenticationPayload("get_farmer", launcher_id, self.pool.default_target_puzzle_hash, authentication_token)
        )
        if not AugSchemeMPL.verify(farmer_record.authentication_public_key, message, signature):
            return error_response(
                PoolErrorCode.INVALID_SIGNATURE,
                f"Failed to verify signature {signature} for launcher_id {launcher_id.hex()}.",
            )

        response: GetFarmerResponse = GetFarmerResponse(
            farmer_record.authentication_public_key,
            farmer_record.payout_instructions,
            farmer_record.difficulty,
            farmer_record.points,
        )

        self.pool.log.info(f"get_farmer response {response.to_json_dict()}, " f"launcher_id: {launcher_id.hex()}")
        return sanic_jsonify(response)

    @staticmethod
    def post_metadata_from_request(request_obj: Request):
        return RequestMetadata(
            url=str(request_obj.url),
            scheme=request_obj.scheme,
            headers=request_obj.headers,
            cookies=dict(request_obj.cookies),
            query=dict({key: value[0] for (key, value) in request_obj.args.items()}),
            remote=request_obj.remote_addr,
        )

    async def post_farmer(self, request_obj: Request) -> HTTPResponse:
        # TODO(pool): add rate limiting
        post_farmer_request: PostFarmerRequest = PostFarmerRequest.from_json_dict(request_obj.json)

        authentication_token_error = check_authentication_token(
            post_farmer_request.payload.launcher_id,
            post_farmer_request.payload.authentication_token,
            self.pool.authentication_token_timeout,
        )
        if authentication_token_error is not None:
            return authentication_token_error

        post_farmer_response = await self.pool.add_farmer(
            post_farmer_request, self.post_metadata_from_request(request_obj))

        self.pool.log.info(
            f"post_farmer response {post_farmer_response}, "
            f"launcher_id: {post_farmer_request.payload.launcher_id.hex()}",
        )
        return sanic_jsonify(post_farmer_response)

    async def put_farmer(self, request_obj: Request) -> HTTPResponse:
        # TODO(pool): add rate limiting
        put_farmer_request: PutFarmerRequest = PutFarmerRequest.from_json_dict(request_obj.json)

        authentication_token_error = check_authentication_token(
            put_farmer_request.payload.launcher_id,
            put_farmer_request.payload.authentication_token,
            self.pool.authentication_token_timeout,
        )
        if authentication_token_error is not None:
            return authentication_token_error

        # Process the request
        put_farmer_response = await self.pool.update_farmer(put_farmer_request,
                                                            self.post_metadata_from_request(request_obj))

        self.pool.log.info(
            f"put_farmer response {put_farmer_response}, "
            f"launcher_id: {put_farmer_request.payload.launcher_id.hex()}",
        )
        return sanic_jsonify(put_farmer_response)

    async def post_partial(self, request_obj: Request) -> HTTPResponse:
        # TODO(pool): add rate limiting
        start_time = time.time()
        request = request_obj.json
        partial: PostPartialRequest = PostPartialRequest.from_json_dict(request)

        authentication_token_error = check_authentication_token(
            partial.payload.launcher_id,
            partial.payload.authentication_token,
            self.pool.authentication_token_timeout,
        )
        if authentication_token_error is not None:
            return authentication_token_error

        farmer_record: Optional[FarmerRecord] = await self.pool.store.get_farmer_record(partial.payload.launcher_id)
        if farmer_record is None:
            return error_response(
                PoolErrorCode.FARMER_NOT_KNOWN,
                f"Farmer with launcher_id {partial.payload.launcher_id.hex()} not known.",
            )

        post_partial_response = await self.pool.process_partial(partial, farmer_record, uint64(int(start_time)))

        self.pool.log.info(
            f"post_partial response {post_partial_response}, time: {time.time() - start_time} "
            f"launcher_id: {request['payload']['launcher_id']}"
        )
        return sanic_jsonify(post_partial_response)

    async def get_login(self, request_obj: Request) -> HTTPResponse:
        # TODO(pool): add rate limiting
        try:
            launcher_id: bytes32 = hexstr_to_bytes(request_obj.args.get("launcher_id"))
            authentication_token: uint64 = uint64(request_obj.args.get("authentication_token"))
            pps: bool = request_obj.args.get("pps", default=None)
        except AttributeError:
            return missing_argument()
        authentication_token_error = check_authentication_token(
            launcher_id, authentication_token, self.pool.authentication_token_timeout
        )
        if authentication_token_error is not None:
            return authentication_token_error

        farmer_record: Optional[FarmerRecord] = await self.pool.store.get_farmer_record(launcher_id)
        if farmer_record is None:
            return error_response(
                PoolErrorCode.FARMER_NOT_KNOWN, f"Farmer with launcher_id {launcher_id.hex()} unknown."
            )

        # Validate provided signature
        try:
            signature: G2Element = G2Element.from_bytes(hexstr_to_bytes(request_obj.args.get("signature")))
        except AttributeError:
            return missing_argument()
        message: bytes32 = std_hash(
            AuthenticationPayload("get_login", launcher_id, self.pool.default_target_puzzle_hash, authentication_token)
        )
        if not AugSchemeMPL.verify(farmer_record.authentication_public_key, message, signature):
            return error_response(
                PoolErrorCode.INVALID_SIGNATURE,
                f"Failed to verify signature {signature} for launcher_id {launcher_id.hex()}.",
            )

        self.pool.log.info(f"Login successful for launcher_id: {launcher_id.hex()}")

        return await self.login_response(launcher_id, pps)

    async def login_response(self, launcher_id, pps: bool):
        payment_record: Optional = await self.pool.store.get_payment_system(launcher_id)
        response = {}
        if payment_record is not None:
            if payment_record[0] == pps or pps is None:
                response = {'pps_enabled': payment_record[0], 'pps_changed': False}
            elif pps is True:
                await self.pool.store.change_payment_system(launcher_id, 1)
                response = {'pps_enabled': True, 'pps_changed': True}
            elif pps is False:
                await self.pool.store.change_payment_system(launcher_id, 0)
                response = {'pps_enabled': False, 'pps_changed': True}

        return sanic_jsonify(response)


server: Optional[PoolServer] = None


def start_pool_server(server_class=PoolServer,
                      pool_store: Optional[AbstractPoolStore] = None,
                      difficulty_function: Callable = get_new_difficulty,
                      payment_manager: Optional[AbstractPaymentManager] = None):
    global server
    workers = multiprocessing.cpu_count()
    config = load_config(DEFAULT_ROOT_PATH, "config.yaml")
    overrides = config["network_overrides"]["constants"][config["selected_network"]]
    constants: ConsensusConstants = DEFAULT_CONSTANTS.replace_str_to_bytes(**overrides)
    server = server_class(config, constants, pool_store, difficulty_function, payment_manager)
    app.add_route(server.wrap_http_handler(server.index), "/")
    app.add_route(server.wrap_http_handler(server.get_pool_info), "/pool_info")
    app.add_route(server.wrap_http_handler(server.get_farmer), "/farmer")
    app.add_route(server.wrap_http_handler(server.post_farmer), "/farmer", methods=["POST"], )
    app.add_route(server.wrap_http_handler(server.put_farmer), "/farmer", methods=["PUT"], )
    app.add_route(server.wrap_http_handler(server.post_partial), "/partial", methods=["POST"], )
    app.add_route(server.wrap_http_handler(server.get_login), "/login")
    ssl_context = get_ssl_context(server.pool_config)
    app.run(
        host=server.host,
        port=server.port,
        ssl=ssl_context,
        workers=workers,
        access_log=False,
        debug=True,
    )


@app.before_server_start
async def start(app, loop):
    await server.start()


@app.after_server_stop
async def stop(app, loop):
    logging.info("stopping pool")
    await server.stop()


def main():
    try:
        start_pool_server()
    except KeyboardInterrupt:
        app.stop()


if __name__ == "__main__":
    main()
