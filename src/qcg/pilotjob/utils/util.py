class Singletone(type):
    """Singletone metaclass.
    The role of this class (as name suggests) is to ensure that all objects of class defined as:
        SomeClass(metaclass=Singletone)
    has the same, common instance.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singletone, cls).__call__(*args, **kwargs)
        return cls._instances[cls]