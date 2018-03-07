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
