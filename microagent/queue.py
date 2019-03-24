import ujson


class Queue:
    _queues: dict = {}

    def __new__(cls, *args, **kwargs):
        name = kwargs.get('name', args[0] if args else None)
        return cls._queues[name] if name in cls._queues else super().__new__(cls)

    def __init__(self, name: str, serializer=None):
        if name in self._queues:
            return

        self.name = name
        self.serializer = serializer or ujson
        self._queues[name] = self

    def __repr__(self):
        return f'<Queue {self.name}>'

    def __eq__(self, other):
        return self.name == other.name

    def serialize(self, data: dict) -> str:
        return self.serializer.dumps(data)

    def deserialize(self, data: str) -> dict:
        return self.serializer.loads(data)

    @classmethod
    def get(cls, name: str):
        ''' Get signal instance by name '''
        try:
            return cls._queues[name]
        except KeyError:
            raise Exception(f'No such signal {name}')

    @classmethod
    def get_all(cls):
        return cls._queues
