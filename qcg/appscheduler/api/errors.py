class QCGPJMAError(Exception):
    pass


class InternalError(QCGPJMAError):
    pass


class InvalidJobDescription(QCGPJMAError):
    pass


class JobNotDefined(QCGPJMAError):
    pass


class ConnectionError(QCGPJMAError):
    pass


class WrongArguments(QCGPJMAError):
    pass


class FileError(QCGPJMAError):
    pass
