#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    """
    抽象化ポリモーフィズム基底クラス

    validate()      :自身で扱えるdataかvalidation
    process()       :データを処理後、文字列にして返す
    format_output() :文字列を整形して返す
    """

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """自身で扱えるdataかvalidation"""

        pass

    @abstractmethod
    def process(self, data: Any) -> str:
        """データを処理後、文字列にして返す"""

        pass

    def format_output(self, result: str) -> str:
        """文字列を整形して返す"""

        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    """
    ポリモーフィズム特化クラス
    intかfloatを扱う
    """

    def validate(self, data: Any) -> bool:
        """dataがint or floatかvalidation"""

        # 1.条件文作成
        is_list = isinstance(data, list)
        is_num = all(isinstance(value, (int, float)) for value in data)

        # 2.dataがlistで中身がintかfloatのみTrueとする
        if is_list and is_num:
            return True
        return False

    def process(self, data: Any) -> str:
        """要素数、合計値、平均値を文字列にして返す"""

        # 1.validation
        if not self.validate(data):
            raise ValueError(f"data is not numeric (data = {data})")
        if not data:
            return "Processed 0 numeric values, sum=0, avg=0"

        # 2.先に計算
        count = len(data)
        total = sum(data)
        avg = total / count

        # 3.text作成
        text = f"Processed {count} numeric values, sum={total}, avg={avg:.1f}"
        return text


class TextProcessor(DataProcessor):
    """
    ポリモーフィズム特化クラス
    strを扱う
    """

    def validate(self, data: Any) -> bool:
        """dataがstrかvalidation"""

        # 1.dataがstrのみTrueとする
        if isinstance(data, str):
            return True
        return False

    def process(self, data: Any) -> str:
        """文字数、単語数を文字列にして返す"""

        # 1.validation
        if not self.validate(data):
            raise ValueError(f"data is not str (data = {data})")

        # 2.先に計算
        count = len(data)
        words = len(data.split(" "))

        # 3.text作成
        text = f"Processed text: {count} characters, {words} words"
        return text


class LogProcessor(DataProcessor):
    """
    ポリモーフィズム特化クラス
    logを扱う
    """

    def validate(self, data: Any) -> bool:
        """dataがstrかつ、':'が一つ以上含まれているかvalidation"""

        # 1.dataがstrかつ、':'が一つ以上あればTrueとする
        if isinstance(data, str) and ':' in data:
            return True
        return False

    def process(self, data: Any) -> str:
        """文字列をコロンで区切って返す"""

        # 1.validation
        if not self.validate(data):
            raise ValueError(f"data is not log (data = {data})")

        # 2.先に区切る
        level, message = data.split(":", 1)
        message = message.strip()
        type = level

        # 3.ERRORだったらALERTに
        if level == "ERROR":
            type = "ALERT"

        # 4.text作成
        text = f"[{type}] {level} level detected: {message}"
        return text


def main() -> None:
    """ポリモーフィズムDEMO関数"""

    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    # 1.data_base作成
    data_base = [
        ("Numeric", NumericProcessor(), [1, 2, 3, 4, 5]),
        ("Text", TextProcessor(), "Hello Nexus World"),
        ("Log", LogProcessor(),  "ERROR: Connection timeout")
        ]

    # 2.process実行
    for name, processor, data in data_base:
        try:
            print(f"Initializing {name} Processor...")
            print(f"Processing data: {data}")
            if processor.validate(data):
                print(f"Validation: {name} data verified")
                output = processor.process(data)
                print(processor.format_output(output))
            else:
                print(f"Validation: {name} data denied")
        except ValueError:
            print(f"Prosessor: {name} data denied")
        print()

    # 3.Processing_DEMO
    print("=== Polymorphic Processing Demo ===")
    data_base = [
        ("Numeric", NumericProcessor(), [1, 2, 3]),
        ("Text", TextProcessor(), "Hello World!"),
        ("Log", LogProcessor(),  "INFO: System ready")
        ]
    print("Processing multiple data types through same interface...")
    count = 1
    for name, processor, data in data_base:
        try:
            if processor.validate(data):
                print(f"Result {count}: {processor.process(data)}")
            else:
                print(f"Validation: {name} data denied")
        except ValueError:
            print(f"Prosessor: {name} data denied")
        count += 1
    print()

    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
