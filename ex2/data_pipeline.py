#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, Tuple, List, Protocol


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


class ExportPlugin(Protocol):
    def process_output(self, data: List[Tuple[int, str]]) -> None:
        pass


class CSVExport:
    def process_output(self, data: List[Tuple[int, str]]) -> None:
        print("CSV Output:")
        print(','.join([val for _, val in data]))


class JSONExport:
    def process_output(self, data: List[Tuple[int, str]]) -> None:
        print("JSON Output:")
        items: List[str] = []
        for index, value in data:
            items.append(f'"item_{index}": "{value}"')
        print("{\n " + ", \n ".join(items) + "\n}")


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
        print()
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
        for proc in self._processors:
            name = proc.__class__.__name__.replace("Processor", " Processor")
            total = proc.total_processed
            remaining = len(proc._data)
            print(f"{name}: total {total} items processed,"
                  f"remaining {remaining} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            extracted: List[Tuple[int, str]] = []
            for _ in range(nb):
                try:
                    extracted.append(proc.output())
                except Exception:
                    break

            if extracted:
                plugin.process_output(extracted)


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print()
    print("Initialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()
    print()
    print("Registering Processor")
    num_proc = NumericProcessor()
    txt_proc = TextProcessor()
    log_proc = LogProcessor()
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
    data_stream.register_processor(txt_proc)
    data_stream.register_processor(log_proc)
    print()
    print("Send first batch of data on stream: " + str(test))
    data_stream.process_stream(test)
    data_stream.print_processors_stats()
    print()
    print("Send 3 processed data from each processor to CSV plugin:")
    data_stream.output_pipeline(3, CSVExport())
    print()
    data_stream.print_processors_stats()
    batch2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [
            {'log_level': 'ERROR', 'log_message': '500 server crash'},
            {'log_level': 'NOTICE', 'log_message':
             'Certificate expires in 10 days'}
        ],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]
    print()
    print("Send another batch of data" + str(batch2))
    data_stream.process_stream(batch2)
    data_stream.print_processors_stats()
    print()
    print("Send 5 processed data from each processor to a JSON Plugin:")
    data_stream.output_pipeline(5, JSONExport())
    data_stream.print_processors_stats()
