from abc import ABC, abstractmethod
from typing import Any, Tuple


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._index: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> Tuple[int, str]:
        if not self._data:
            raise ValueError("No data avaible")
        oldest_piece = self._value.pop(0)
        current_index = self.index
        self.index += 1
        return (current_index, oldest_piece)


class NumericProcessor(DataProcessor):
    def validate(self, data) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(item, (int, float)) for item in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        items = data if isinstance(data, list) else [data]
        for item in items:
            self._data.append((self._index, str(item)))
            self._index += 1
