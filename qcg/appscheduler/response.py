import json
from enum import IntEnum


class ResponseCode(IntEnum):
    OK = 0
    ERROR = 1


class Response:
    def __init__(self, code=ResponseCode.OK, msg=None, data=None):
        self.code = code
        self.msg = msg
        self.data = data

    @classmethod
    def Ok(cls, msg=None, data=None):
        return Response(ResponseCode.OK, msg, data)

    @classmethod
    def Error(cls, msg=None, data=None):
        return Response(ResponseCode.ERROR, msg, data)

    def toDict(self):
        res = {'code': int(self.code)}

        if self.msg is not None:
            res['message'] = self.msg

        if self.data is not None:
            res['data'] = self.data

        return res

    def toJSON(self):
        return json.dumps(self.toDict(), indent=2)
