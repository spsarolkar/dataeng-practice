package in.sunilsarolkar;


import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

// 🔹 11. Mixed: IoT Sensor Spike Detector
// Scenario: Ingest sensor data and detect anomalies.

// Streaming from Kafka:

// sensorId, temperature, humidity, timestamp

// Task:

// Apply watermark + sliding window

// Calculate average temperature per sensor

// Flag sensors where temperature increased by >10°C within 5 minutes

// 🧩 Involves: window joins, stateful stream, historical comparison via GroupState


class IotSensorSpikeDetector{

    public static class IotEvent implements  Serializable{
        String sensorId;
        int temperature;
        int humidity;
        Timestamp timestamp;

        public String getSensorId() {
            return sensorId;
        }

        public void setSensorId(String sensorId) {
            this.sensorId = sensorId;
        }

        public int getTemperature() {
            return temperature;
        }

        public void setTemperature(int temperature) {
            this.temperature = temperature;
        }

        public int getHumidity() {
            return humidity;
        }

        public void setHumidity(int humidity) {
            this.humidity = humidity;
        }

        public Timestamp getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
        }
        
    }

    public static class SensorTemperature implements Serializable{
        int minTemp=Integer.MAX_VALUE;
        int maxTemp=Integer.MIN_VALUE;

        public int getMinTemp() {
            return minTemp;
        }

        public void setMinTemp(int minTemp) {
            this.minTemp = minTemp;
        }

        public int getMaxTemp() {
            return maxTemp;
        }

        public void setMaxTemp(int maxTemp) {
            this.maxTemp = maxTemp;
        }

        
    }

    public static void main(String[] args) throws StreamingQueryException,TimeoutException {
        SparkSession spark=SparkSession.builder().appName("SpikeDetector")
        .master("local[*]").getOrCreate();
        
        Dataset<Row> queue=spark.readStream().format("kafka")
        .option("kafka.bootstrap.server","localhost:4042")
        .option("offset","latest").load();

        StructType iotEventSchema=new StructType()
        .add("sensorId",DataTypes.StringType)
        .add("temperature",DataTypes.IntegerType)
        .add("humidity",DataTypes.IntegerType)
        .add("timestamp",DataTypes.TimestampType);



        Encoder<IotEvent> iotmEncoder=Encoders.bean(IotEvent.class);
        Encoder<SensorTemperature> sensorTempEncoder=Encoders.bean(SensorTemperature.class);

        Dataset<IotEvent> raw=queue.select(col("value").cast(DataTypes.StringType))
        .select(from_json(col("value"),iotEventSchema)
        .as("data")).select("data.*").as(iotmEncoder);

        Dataset<String> alerts=raw.withWatermark("timestamp","5 minute")
        .groupByKey((MapFunction<IotEvent,String>)IotEvent::getSensorId,Encoders.STRING())
        .flatMapGroupsWithState(new FlatMapGroupsWithStateFunction<String, IotEvent, SensorTemperature, String>(){
            @Override
            public Iterator<String> call(String key, Iterator<IotEvent> iotEvents, GroupState<SensorTemperature> state) throws Exception {
                 List<String> alerts=new ArrayList<>();
                 SensorTemperature sensorTemperature=state.exists()?state.get():new SensorTemperature();
                while (iotEvents.hasNext()){
                    IotEvent event=iotEvents.next();
                    int currTemp=event.getTemperature();
                    if(currTemp < sensorTemperature.getMinTemp()) {
                        sensorTemperature.setMinTemp(currTemp);
                    }
                    if(currTemp > sensorTemperature.getMaxTemp()) {
                        sensorTemperature.setMaxTemp(currTemp);
                    }

                    if((sensorTemperature.getMaxTemp()-sensorTemperature.getMinTemp())>10){
                        alerts.add("Sensor Thermal runaway"+event.sensorId);
                    }

                    state.update(sensorTemperature);
                    
                }
                //state.setTimeoutDuration("5 minute");
                // below event should be latest event based on timestamp
                state.setTimeoutTimestamp(event.getTimestamp().getTime() + Duration.ofMinutes(5).toMillis());

                return alerts.iterator();
            } 
        }, OutputMode.Append(), sensorTempEncoder, Encoders.STRING(), GroupStateTimeout.EventTimeTimeout());//**** if we use EventTimeTimeout we should use state.setTimeoutTimestamp(event.getTimestamp().getTime() + Duration.ofMinutes(5).toMillis());



        StreamingQuery consoleStream=alerts.writeStream().format("console").outputMode(OutputMode.Append()).option("truncate", false).start();

        consoleStream.awaitTermination();

        spark.stop();
        
    }
}