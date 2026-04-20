#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, Tuple, List


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._index: int = 0
        self.total_processed: int = 0

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
            self.total_processed += 1


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
            self.total_processed += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return (all(isinstance(k, str) and isinstance(v, str)
                        for k, v in data.items()))
        if isinstance(data, list) and data:
            return all(self.validate(i) and isinstance(i, dict) for i in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        items = data if isinstance(data, list) else [data]
        for item in items:
            value_str = ": ".join(item.values())
            self._data.append((self._index, value_str))
            self._index += 1
            self.total_processed += 1


class DataStream:
    def __init__(self) -> None:
        self._processors: List[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: List[Any]) -> None:
        for element in stream:
            handled = False
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    handled = True
            if not handled:
                print(
                        "DataStream error - Can't process element in stream"
                        f"{element}"
                        )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
        for proc in self._processors:
            name = proc.__class__.__name__.replace("Processor", " Processor")
            total = proc.total_processed
            remaining = len(proc._data)
            print(f"{name}: total {total} items processed,"
                  f"remaining {remaining} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print()
    print("Initialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()
    print()
    print("Registering Nmeric Processor")
    num_proc = NumericProcessor()
    test = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING', 'log_message':
             'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}
        ],
        42,
        ['Hi', 'five']
    ]
    data_stream.register_processor(num_proc)
    print()
    print("Send first batch of data on stream: " + str(test))
    data_stream.process_stream(test)
    data_stream.print_processors_stats()
    print()
    print("Registering other data processors")
    txt_proc = TextProcessor()
    log_proc = LogProcessor()
    data_stream.register_processor(txt_proc)
    data_stream.register_processor(log_proc)
    print("Send the same batch again")
    data_stream.process_stream(test)
    data_stream.print_processors_stats()
    print()
    print("Consume some elements from the data processors:"
          " Numeric 3, Text 2 , Log 1")
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        txt_proc.output()
    log_proc.output()
    data_stream.print_processors_stats()
