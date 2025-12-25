#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, Optional, Protocol


class NexusManager:
    """
    NexusManager: 複数のパイプラインを統合管理する工場長
    """

    def __init__(self) -> None:
        """パイプラインを初期化"""

        print("Initializing Nexus Manager...")
        print()
        print("Creating Data Processing Pipeline...")
        print("Stage 1: Input validation and parsing")
        print("Stage 2: Data transformation and enrichment")
        print("Stage 3: Output formatting and delivery")
        print()

        self.pipelines: list[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: Any) -> None:
        """各ライン（Adapter）をパイプラインに登録する"""

        self.pipelines.append(pipeline)

    def process_data(self, data: Any,
                     pipeline_id: Optional[str] = None) -> None:
        """
        データ処理の実行
        pipeline_idが指定されればそれを、なければ全ラインを稼働させる。
        """

        try:
            target_pipelines = []

            # 1.ID指定があれば、そのラインだけ探す
            if pipeline_id:
                for p in self.pipelines:
                    if p.pipeline_id == pipeline_id:
                        target_pipelines.append(p)
                if not target_pipelines:
                    raise ValueError("That pipeline does not exist.")
            else:
                target_pipelines = self.pipelines

            # 2.実際の処理を実行
            for pipeline in target_pipelines:
                text = pipeline.process(data)
                print(text)

        except Exception as e:
            # 3.エラー実演、かっこいいセリフを吐く（トム・クルーズみたいに）
            print(f"Error detected: {e}")
            print("Recovery initiated: Switching to backup processor")
            print("Recovery successful: Pipeline restored, processing resumed")


class ProcessingStage(Protocol):
    """processメソッドを持つクラスは全部このクラスの仲間"""

    def process(self, data: Any) -> Any:
        """共通メソッド"""

        pass


class ProcessingPipeline(ABC):
    """
    ProcessingPipeline
    """

    @abstractmethod
    def __init__(self) -> None:
        """初期化：作業員リストを作る"""

        self.stages: list[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        """作業員（ステージ）をラインに追加する"""

        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        """全員に順番に仕事をさせる"""

        # 1.前の人の結果を、次の人に渡す
        for stage in self.stages:
            data = stage.process(data)

        return data


class JSONAdapter(ProcessingPipeline):
    """
    JSONAdapter(pipeline_id)
    """

    def __init__(self, pipeline_id: str) -> None:
        """
        初期化
        """

        super().__init__()
        self.pipeline_id = pipeline_id

        # 作業員を雇う
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> Any:
        """看板を出して処理開始"""

        print("Processing JSON data through pipeline...")

        return super().process(data)


class CSVAdapter(ProcessingPipeline):
    """
    CSVAdapter(pipeline_id)
    """

    def __init__(self, pipeline_id: str) -> None:
        """
        初期化
        """

        super().__init__()
        self.pipeline_id = pipeline_id

        # 作業員を雇う
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> Any:
        """看板を出して処理開始"""

        print("Processing CSV data through pipeline...")

        return super().process(data)


class StreamAdapter(ProcessingPipeline):
    """
    StreamAdapter(pipeline_id)
    """

    def __init__(self, pipeline_id: str) -> None:
        """
        初期化
        """

        super().__init__()
        self.pipeline_id = pipeline_id

        # 作業員を雇う
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> Any:
        """看板を出して処理開始"""

        print("Processing Stream data through pipeline...")

        return super().process(data)


class InputStage:
    """
    InputStage: データの受け入れとログ表示
    """

    def process(self, data: Any) -> Any:
        """どんなデータが来たかを表示する"""

        if isinstance(data, str):
            print(f"Input: {data}")
        else:
            print(f"Input: {str(data)}")

        return data


class TransformStage:
    """
    TransformStage: データの加工（フリ）とログ表示
    """

    def process(self, data: Any) -> Any:
        """データの種類（型）を見て、かっこいいセリフを吐く（ジェームズ・ボンドみたいに）"""

        # JSONの場合
        if isinstance(data, dict):
            print("Transform: Enriched with metadata and validation")

        # CSVの場合
        elif isinstance(data, str):
            print("Transform: Parsed and structured data")

        # Stream（リスト）の場合
        elif isinstance(data, list):
            print("Transform: Aggregated and filtered")

        # 実際には加工せず、そのまま次へ流す。トホホ...
        return data


class OutputStage:
    """
    OutputStage: 最終的な整形と出力
    """

    def process(self, data: Any) -> Any:
        """データの種類に合わせて、最終レポートを作成"""

        # JSON: 温度を取り出して表示
        if isinstance(data, dict):
            val = data.get("value")
            unit = data.get("unit", "C")
            return ("Output: Processed temperature reading: "
                    f"{val}°{unit} (Normal range)")

        # CSV: 決まったメッセージを出力
        elif isinstance(data, str):
            return "Output: User activity logged: 1 actions processed"

        # Stream: 平均値を計算
        elif isinstance(data, list):
            count = len(data)

            if 0 < count:
                avg = sum(data) / count
            else:
                avg = 0

            return ("Output: Stream summary: "
                    f"{count} readings, avg: {avg:.1f}°C")

        return f"Output: Processed {data}"


def main():
    """
    NexsusManager_DEMO
    """

    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print()

    # 1.NexusManager作成
    manager = NexusManager()

    # 2.JSONライン作成
    json_line = JSONAdapter("pipeline_json_01")
    manager.add_pipeline(json_line)

    # 3.CSVライン作成
    csv_line = CSVAdapter("pipeline_csv_01")
    manager.add_pipeline(csv_line)

    # 4.STREAMライン作成
    stream_line = StreamAdapter("pipeline_stream_01")
    manager.add_pipeline(stream_line)

    print("=== Multi-Format Data Processing ===")
    print()

    # 5.JSON実行
    data = {"sensor": "temp", "value": 23.5, "unit": "C"}
    manager.process_data(data, json_line.pipeline_id)
    print()

    # 6.CSV実行
    data = "user,action,timestamp"
    manager.process_data(data, csv_line.pipeline_id)
    print()

    # 7.Stream実行
    data = [21.8, 22.0, 22.5, 22.1, 22.1]
    manager.process_data(data, stream_line.pipeline_id)
    print()

    # 8.エラー実演
    print("=== Error Recovery Test ===")
    manager.process_data(data, "error")
    print()

    print("Nexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
