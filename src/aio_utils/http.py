import attr as _attr
import enum as _enum
import typing as _ty
import urllib3 as _urllib3


class HttpRequestMethod(_enum.Enum):
    GET = "GET"
    POST = "POST"
    DELETE = "DELETE"
    PUT = "PUT"


@_attr.s(auto_attribs=True, init=True, slots=True)
class HttpRequest:
    method: HttpRequestMethod
    url: str
    params: _ty.Optional[_ty.Mapping[str, _ty.Any]] = None
    json: _ty.Optional[_ty.Mapping[str, _ty.Any]] = None
    headers: _ty.Optional[_ty.Mapping[str, _ty.Any]] = None

    @classmethod
    def create(cls, method, url, params=None, json=None, **headers):
        headers = _urllib3.make_headers(**headers)
        return cls(method, url, params, json, headers)

