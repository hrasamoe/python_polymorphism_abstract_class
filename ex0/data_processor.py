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
        return (self._data.pop(0))


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
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


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        items = data if isinstance(data, list) else [data]
        for item in items:
            self._data.append((self._index, item))
            self._index += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return (all(isinstance(k, str) and isinstance(v, str)
                        for k, v in data.items()))
        if isinstance(data, list):
            return all(self.validate(i) for i in data if isinstance(i, dict))
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        items = data if isinstance(data, list) else [data]
        for item in items:
            value_str = ": ".join(item.values())
            self._data.append((self._index, value_str))
            self._index += 1


if __name__ == "__main__":
    print("=== Code Nexus- Data Processor ===")
    print("Testing Numeric Processor...")
    num_proc = NumericProcessor()
    print(f" Trying to validate input '42': {num_proc.validate(42)}")
    print(f" Trying to validate input 'Hello': {num_proc.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_proc.ingest('foo')
    except Exception as e:
        print(f" Got exception: {e}")
    num_proc.ingest([1, 2, 3, 4, 5])
    print(" Processing data: [1, 2, 3, 4, 5]")
    print(" Extracting 3 values...")
    for _ in range(3):
        index, value = num_proc.output()
        print(f" Numeric value {index}: {value}")
    print()
    print("Testing Text Processor...")
    txt_p = TextProcessor()
    print(f" Trying to validate input '42': {txt_p.validate(42)}")
    txt_p.ingest(["Hello", "Nexus", "World"])
    print(" Processing data: ['Hello', 'Nexus', 'World']")
    print(" Extracting 1 value...")
    rank, val = txt_p.output()
    print(f" Text value {rank}: {val}")
    print()
    print("Testing Log Processor...")
    log_p = LogProcessor()
    print(f" Trying to validate input 'Hello': {log_p.validate('Hello')}")
    logs = [
        {"log_level": "NOTICE", "msg": "Connection to server"},
        {"log_level": "ERROR", "msg": "Unauthorized access!!"}
    ]
    log_p.ingest(logs)
    print(f" Processing data {logs}")
    print(" Extracting 2 values...")
    for _ in range(2):
        index, value = log_p.output()
        print(f" Numeric value {index}: {value}")
