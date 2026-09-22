import os
import json
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import AggregateFunction
from pyflink.common.time import Time
from datetime import datetime

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/cryptodb"

# ---------------------------------------------------------
# Pump & Dump Detector (KeyedProcessFunction)
# ---------------------------------------------------------
class PumpDumpDetector(KeyedProcessFunction):
    def __init__(self):
        self.last_price_state = None

    def open(self, runtime_context):
        self.last_price_state = runtime_context.get_state(ValueStateDescriptor("last_price", Types.FLOAT()))

    def process_element(self, value, ctx, out):
        try:
            trade = json.loads(value)
            symbol = trade.get('symbol')
            price = trade.get('price')
            if not symbol or not price:
                return
            
            last_price = self.last_price_state.value()
            if last_price is not None:
                change = (price - last_price) / last_price
                
                # Simple Pump detection (>1% in a short tick)
                if change > 0.01:
                    alert = {
                        "alert_time": trade.get('trade_time', int(datetime.now().timestamp() * 1000)),
                        "symbol": symbol,
                        "alert_type": "PUMP_DETECTED",
                        "message": f"Price surged by {change*100:.2f}% to {price}",
                        "price": price
                    }
                    out.collect(json.dumps(alert))
                # Dump detection (<-1% in a short tick)
                elif change < -0.01:
                    alert = {
                        "alert_time": trade.get('trade_time', int(datetime.now().timestamp() * 1000)),
                        "symbol": symbol,
                        "alert_type": "DUMP_DETECTED",
                        "message": f"Price dropped by {change*100:.2f}% to {price}",
                        "price": price
                    }
                    out.collect(json.dumps(alert))
            
            self.last_price_state.update(price)
        except Exception:
            pass

# ---------------------------------------------------------
# Aggregator for ClickHouse
# ---------------------------------------------------------
class TradeAggregator(AggregateFunction):
    def create_accumulator(self):
        # (symbol, sum_price, sum_volume, count)
        return ("", 0.0, 0.0, 0)

    def add(self, value, accumulator):
        try:
            trade = json.loads(value)
            symbol = trade.get('symbol', 'UNKNOWN')
            price = float(trade.get('price', 0.0))
            quantity = float(trade.get('quantity', 0.0))
            return (symbol, accumulator[1] + price, accumulator[2] + quantity, accumulator[3] + 1)
        except Exception:
            return accumulator

    def get_result(self, accumulator):
        symbol = accumulator[0]
        sum_price = accumulator[1]
        sum_volume = accumulator[2]
        count = accumulator[3]
        avg_price = sum_price / count if count > 0 else 0.0
        
        # We must return a Row to JDBC Sink, but in PyFlink we can return a tuple/list matching TypeInformation
        # Schema: window_start, window_end, symbol, total_volume, trade_count, avg_price
        # Note: We don't have window boundaries in get_result easily in AggregateFunction without WindowFunction,
        # but ClickHouse crypto_by_minute table uses window_start, window_end.
        # We can approximate with current time.
        now = int(datetime.now().timestamp() * 1000)
        return (now - 60000, now, symbol, sum_volume, count, avg_price)

    def merge(self, a, b):
        return (a[0], a[1] + b[1], a[2] + b[2], a[3] + b[3])

def extract_symbol(json_str):
    try:
        return json.loads(json_str).get("symbol", "UNKNOWN")
    except:
        return "UNKNOWN"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # 1. Source: Kafka crypto-trades
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics("crypto-trades") \
        .set_group_id("pyflink-pattern-detector") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # 2. Branch A: Pump & Dump Detector -> Kafka crypto-alerts
    alerts_stream = stream \
        .key_by(extract_symbol) \
        .process(PumpDumpDetector(), output_type=Types.STRING())

    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("crypto-alerts")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
    alerts_stream.sink_to(kafka_sink)

    # 3. Branch B: Aggregations -> ClickHouse JDBC Sink
    # Schema: window_start (BIGINT), window_end (BIGINT), symbol (STRING), total_volume (DOUBLE), trade_count (BIGINT), avg_price (DOUBLE)
    # ClickHouse driver uses long for DateTime64
    type_info = Types.ROW([
        Types.LONG(),    # window_start
        Types.LONG(),    # window_end
        Types.STRING(),  # symbol
        Types.DOUBLE(),  # total_volume
        Types.LONG(),    # trade_count
        Types.DOUBLE()   # avg_price
    ])

    agg_stream = stream \
        .key_by(extract_symbol) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(1))) \
        .aggregate(TradeAggregator(), output_type=type_info)

    jdbc_sink = JdbcSink.sink(
        "INSERT INTO crypto_by_minute (window_start, window_end, symbol, total_volume, trade_count, avg_price) VALUES (?, ?, ?, ?, ?, ?)",
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(CLICKHOUSE_URL)
            .with_driver_name("com.clickhouse.jdbc.ClickHouseDriver")
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_size(100)
            .with_batch_interval_ms(1000)
            .with_max_retries(3)
            .build()
    )

    agg_stream.add_sink(jdbc_sink)

    env.execute("PyFlink Crypto Patterns & Aggregation")

if __name__ == '__main__':
    main()
