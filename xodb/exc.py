

class XODBError(Exception):
    pass


class InvalidTermError(XODBError):
    pass


class AttributeRequired(XODBError):
    pass


class AttributeTypeRequired(XODBError):
    pass


class ValidationError(XODBError):
    pass


class PrefixError(XODBError):
    pass
