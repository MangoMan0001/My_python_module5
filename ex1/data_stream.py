#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, Optional, Union


class DataStream(ABC):
    """
    抽象化ポリモーフィズム基底クラス
    リストを処理可能

    filter_data()   :list_dataの必要な要素のみ抜粋して返す
    process_batch() :listを処理後、文字列にして返す
    get_stats()     :文字列を整形して返す
    """

    def __init__(self, stream_id: str) -> None:
        """初期化関数"""

        self.stream_id = stream_id

    def filter_data(self,
                    data_batch: list[Any],
                    criteria: Optional[str] = None) -> list[Any]:
        """list_dataの必要な要素のみ抜粋して返す"""

        pass

    @abstractmethod
    def process_batch(self, data_batch: list[Any]) -> str:
        """listを処理後、文字列にして返す"""

        pass

    def get_stats(self) -> dict[str, Union[str, int, float]]:
        """stream_id、class_nameをdictとして返す"""

        return {
            "stream_id": self.stream_id,
            "type": self.__class__.__name__
        }


class SensorStream(DataStream):
    """
    抽象化ポリモーフィズム特化クラス
    センサーリストを処理可能
    """

    def __init__(self, stream_id: str) -> None:
        """初期化関数"""

        super().__init__(stream_id)
        self.total_processed = 0
        self.total_alert = {"type alert": 0, "critical sensor alerts": 0}

    def filter_data(self,
                    data_batch: list[Any],
                    criteria: Optional[str] = None) -> list[Any]:
        """
        1.dict型であること
        2.valueがintかfloatであること

        以上を抜粋して返す
        """

        clean_data = []
        for item in data_batch:
            if not isinstance(item, dict):
                self.total_alert["type alert"] += 1
                continue

            for value in item.values():
                if not isinstance(value, (int, float)):
                    self.total_alert["type alert"] += 1
                    continue

                if value < 0 or 100 < value:
                    self.total_alert["critical sensor alerts"] += 1
                    continue

            clean_data.append(item)
        return clean_data

    def process_batch(self, data_batch: list[Any]) -> str:
        """温度のみ抜粋して平均で割って、文字列にして返す"""

        temp_list = []
        clear_data = self.filter_data(data_batch)
        self.total_processed += len(clear_data)

        # 1.tempだけ抜粋
        for data in clear_data:
            for key, value in data.items():
                if key == "temp":
                    temp_list.append(value)

        # 2.平均を計算
        average = sum(temp_list) / len(temp_list)

        # 3.整形して出力
        count = len(clear_data)
        text = (f"Sensor analysis: {count} readings processed, "
                f"avg temp: {average:.1f}°C")
        return text

    def get_stats(self) -> dict[str, Union[str, int, float]]:
        """total_processedをdictに加えて返す"""

        stats = super().get_stats()
        stats["total_processed"] = self.total_processed

        return stats


class TransactionStream(DataStream):
    """
    抽象化ポリモーフィズム特化クラス
    Trancasctionリストを処理可能
    """

    def __init__(self, stream_id: str) -> None:
        """初期化関数"""

        super().__init__(stream_id)
        self.total_processed = 0
        self.total_alert = {"type error": 0, "key error": 0,
                            "large transaction": 0}

    def filter_data(
            self,
            data_batch: list[Any],
            criteria: Optional[str] = None) -> list[Any]:
        """
        1.dict型であること
        2.keyがbuyかsellであること
        3.valueがintかfloatのみであること

        以上を抜粋して返す
        """

        clean_data = []
        for item in data_batch:
            if not isinstance(item, dict):
                self.total_alert["type error"] += 1
                continue

            for key in item.keys():
                if key not in ["buy", "sell"]:
                    self.total_alert["key error"] += 1
                    continue

            for value in item.values():
                if not isinstance(value, (int, float)):
                    self.total_alert["type error"] += 1
                    continue

                if 1000 <= value:
                    self.total_alert["large transaction"] += 1
                    continue

            clean_data.append(item)

        return clean_data

    def process_batch(self, data_batch: list[Any]) -> str:
        """
        1.keyがbuyであれば+value
        2.keyがsellであれば-value

        以上を行った合計値を文字列にして返す
        """

        result = 0
        clear_data = self.filter_data(data_batch)
        self.total_processed += len(clear_data)

        # 1.just計算
        for data in clear_data:
            for key, value in data.items():
                if key == "buy":
                    result += value

                if key == "sell":
                    result -= value

        # 2.整形して出力
        count = len(clear_data)
        text = (f"Transaction analysis: {count} operations, "
                f"net flow: {result:+} units")

        return text

    def get_stats(self) -> dict[str, Union[str, int, float]]:
        """total_processedをdictに加えて返す"""

        stats = super().get_stats()
        stats["total_processed"] = self.total_processed

        return stats


class EventStream(DataStream):
    """
    抽象化ポリモーフィズム特化クラス
    Eventリストを処理可能
    """

    def __init__(self, stream_id: str) -> None:
        """初期化関数"""

        super().__init__(stream_id)
        self.total_processed = 0
        self.total_alert = {"type error": 0, "key error": 0}

    def filter_data(self,
                    data_batch: list[Any],
                    criteria: Optional[str] = None) -> list[Any]:
        """
        1.str型であること
        2.loginかerrorかlogoutであること

        以上を抜粋して返す
        """

        clean_data = []
        for item in data_batch:
            if not isinstance(item, str):
                self.total_alert["type error"] += 1
                continue

            if item not in ["login", "error", "logout"]:
                self.total_alert["key error"] += 1
                continue

            clean_data.append(item)

        return clean_data

    def process_batch(self, data_batch: list[Any]) -> str:
        """
        errorの数を数えて合計値を文字列にして返す
        """

        result = 0
        clear_data = self.filter_data(data_batch)
        self.total_processed += len(clear_data)

        # 1.just計算
        for data in clear_data:
            if data == "error":
                result += 1

        # 2.整形して出力
        count = len(clear_data)
        text = (f"Event analysis: {count} events, {result} error detected")

        return text

    def get_stats(self) -> dict[str, Union[str, int, float]]:
        """total_processedをdictに加えて返す"""

        stats = super().get_stats()
        stats["total_processed"] = self.total_processed

        return stats


class StreamProcessor:
    """
    ストイーム処理を統括するマネージャークラス
    """

    def process_stream(self, stream: DataStream, data_batch: list[Any]) -> str:
        """
        DataStream型とdata_batchを受けとり、処理を実行する
        """

        return stream.process_batch(data_batch)


def main() -> None:
    """
    list_data_stream_DEMO
    """

    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

    # 1.data_base作成
    id_dict = {"sensor": {"SENSOR": 1},
               "transaction": {"TRANS": 1},
               "event": {"EVENT": 1}}
    id_dict_stats = {"Sensor": {"SENSOR": 1},
                     "Transaction": {"TRANS": 1},
                     "Event": {"EVENT": 1}}
    data_base = (
        ("Environmental Data",
         "sensor",
         SensorStream,
         [{"temp": 22.5}, {"humidity": 65}, {"pressure": 1013}]),
        ("Financial Data",
         "transaction",
         TransactionStream,
         [{"buy": 100}, {"sell": 150}, {"buy": 75}]),
        ("System Events",
         "event",
         EventStream,
         ["login", "error", "logout"])
         )
    data_base_sats = (
        ("Sensor",
         "readings",
         SensorStream,
         [{"temp": 101}, {"temp": -1}]),
        ("Transaction",
         "operations",
         TransactionStream,
         [{"buy": 100}, {"sell": 150},
          {"buy": 75}, {"buy": 1000}]),
        ("Event",
         "events",
         EventStream,
         ["login", "error", "logout"])
         )

    # 2.process実行
    for type, name, stream, data in data_base:
        item_list = list(id_dict[name].items())
        key, value = item_list[0]
        id = f"{key}_{value:03}"
        stream_manager = StreamProcessor()

        # 2-1.例文に合わせるだけの整形
        data_list = []
        for item in data:
            if isinstance(item, dict):
                k, v = list(item.items())[0]
                data_list.append(str(f"{k}: {v}"))
            else:
                data_list.append(str(item))
        format_data = f"[{', '.join(data_list)}]"

        print(f"Initializing {name} Stream...\n"
              f"Stream ID: {id}, Type: {type}\n"
              f"Processing {name} batch: {format_data}")

        processor = stream(id)
        text = stream_manager(processor, data)
        print(text)
        print()

    # 3.statsDEMO
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print()

    alert_result = []
    print("Batch 1 Results:")
    for name, ing, stream, data in data_base_sats:
        item_list = list(id_dict_stats[name].items())
        key, value = item_list[0]
        id = f"{key}_{value:03}"
        processor = stream(id)

        processor.process_batch(data)
        stream_manager(processor, data)
        stats = processor.get_stats()
        count = stats["total_processed"]
        print(f"- {name} data: {count} {ing} processed")
        alert_result.append(processor.total_alert)
    print()

    print("Stream filtering active: High-priority data only")
    print("Filtered results: ", end="")
    for alert in alert_result:
        i = 0
        count = len(alert)
        for message, count in alert.items():
            if 0 < count:
                print(f"{count} {message}", end="")
            else:
                continue
            if i < count:
                print(", ", end="")
            i += 1
    print()
    print()

    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
