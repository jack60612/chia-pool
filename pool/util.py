from dataclasses import dataclass
from typing import Dict, Mapping

from chia.protocols.pool_protocol import PoolErrorCode, ErrorResponse
from chia.util.ints import uint16
from chia.util.json_util import dict_to_json_str
from sanic import HTTPResponse


def error_response(code: PoolErrorCode, message: str):
    error: ErrorResponse = ErrorResponse(uint16(code.value), message)
    return sanic_jsonify(error)


def sanic_jsonify(response) -> HTTPResponse:
    return HTTPResponse(body=dict_to_json_str(response), content_type="application/json")


def error_dict(code: PoolErrorCode, message: str):
    error: ErrorResponse = ErrorResponse(uint16(code.value), message)
    return error.to_json_dict()


@dataclass
class RequestMetadata:
    """
    HTTP-related metadata passed with HTTP requests
    """
    url: str  # original request url, as used by the client
    scheme: str  # for example https
    headers: Mapping[str, str]  # header names are all lower case
    cookies: Dict[str, str]
    query: Dict[str, str]  # query params passed in the url. These are not used by chia clients at the moment, but
    # allow for a lot of adjustments and thanks to including them now they can be used without introducing breaking changes
    remote: str  # address of the client making the request

    def __post_init__(self):
        self.headers = {k.lower(): v for k, v in self.headers.items()}
