package co.cask.tracker;

import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;

import java.util.concurrent.TimeUnit;

/**
 * MessageGeneratorFlowlet is used to generate test messages to the AuditLogFlowlet
 */
public class MessageGeneratorFlowlet extends AbstractFlowlet {
  private OutputEmitter<String> emitter;

  @Tick(delay = 1L, unit = TimeUnit.SECONDS)
  public void process() {
    String[] testData = new String[]{
      "{\"time\":1457465503859,\"entityId\":{\"namespace\":\"system\",\"artifact\":\"mongodb-plugins\"," +
        "\"version\":\"1.3.0-SNAPSHOT\",\"entity\":\"ARTIFACT\"},\"user\":\"\",\"type\":\"METADATA_CHANGE\"," +
        "\"payload\":{\"previous\":{\"SYSTEM\":{\"properties\":{\"plugin:MongoDB:batchsource\":" +
        "\"MongoDB:batchsource\",\"plugin:MongoDB:batchsink\":\"MongoDB:batchsink\"," +
        "\"pluginversion:MongoDB:batchsink\":\"MongoDB:1.3.0-SNAPSHOT\",\"pluginversion:MongoDB:batchsource\":" +
        "\"MongoDB:1.3.0-SNAPSHOT\",\"plugin:MongoDB:realtimesink\":\"MongoDB:realtimesink\"," +
        "\"pluginversion:MongoDB:realtimesink\":\"MongoDB:1.3.0-SNAPSHOT\"},\"tags\":[\"mongodb-plugins\"]}}," +
        "\"additions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}},\"deletions\":{\"SYSTEM\":{\"properties\":{}," +
        "\"tags\":[]}}}}\n",
      "{\"time\":1457465503910,\"entityId\":{\"namespace\":\"system\",\"artifact\":\"mongodb-plugins\",\"version\":" +
        "\"1.3.0-SNAPSHOT\",\"entity\":\"ARTIFACT\"},\"user\":\"\",\"type\":\"METADATA_CHANGE\",\"payload\":" +
        "{\"previous\":{\"SYSTEM\":{\"properties\":{\"plugin:MongoDB:batchsource\":\"MongoDB:batchsource\"," +
        "\"plugin:MongoDB:batchsink\":\"MongoDB:batchsink\",\"pluginversion:MongoDB:batchsink\":" +
        "\"MongoDB:1.3.0-SNAPSHOT\",\"pluginversion:MongoDB:batchsource\":\"MongoDB:1.3.0-SNAPSHOT\"," +
        "\"plugin:MongoDB:realtimesink\":\"MongoDB:realtimesink\",\"pluginversion:MongoDB:realtimesink\":" +
        "\"MongoDB:1.3.0-SNAPSHOT\"},\"tags\":[\"mongodb-plugins\"]}},\"additions\":{\"SYSTEM\":{\"properties\":{}," +
        "\"tags\":[\"mongodb-plugins\"]}},\"deletions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}}}}\n",
      "{\"time\":1457465506095,\"entityId\":{\"namespace\":\"system\",\"artifact\":\"kafka-plugins\",\"version\":" +
        "\"1.3.0-SNAPSHOT\",\"entity\":\"ARTIFACT\"},\"user\":\"\",\"type\":\"METADATA_CHANGE\",\"payload\":" +
        "{\"previous\":{\"SYSTEM\":{\"properties\":{\"pluginversion:Kafka:realtimesource\":\"Kafka:1.3.0-SNAPSHOT\"," +
        "\"pluginversion:KafkaProducer:realtimesink\":\"KafkaProducer:1.3.0-SNAPSHOT\"," +
        "\"plugin:KafkaProducer:realtimesink\":\"KafkaProducer:realtimesink\",\"plugin:Kafka:realtimesource\":" +
        "\"Kafka:realtimesource\"},\"tags\":[\"kafka-plugins\"]}},\"additions\":{\"SYSTEM\":{\"properties\":{}," +
        "\"tags\":[]}},\"deletions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}}}}\n",
      "{\"time\":1457465506141,\"entityId\":{\"namespace\":\"system\",\"artifact\":\"kafka-plugins\",\"version\":" +
        "\"1.3.0-SNAPSHOT\",\"entity\":\"ARTIFACT\"},\"user\":\"\",\"type\":\"METADATA_CHANGE\",\"payload\":" +
        "{\"previous\":{\"SYSTEM\":{\"properties\":{\"pluginversion:Kafka:realtimesource\":\"Kafka:1.3.0-SNAPSHOT\"," +
        "\"pluginversion:KafkaProducer:realtimesink\":\"KafkaProducer:1.3.0-SNAPSHOT\"," +
        "\"plugin:KafkaProducer:realtimesink\":\"KafkaProducer:realtimesink\",\"plugin:Kafka:realtimesource\":" +
        "\"Kafka:realtimesource\"},\"tags\":[\"kafka-plugins\"]}},\"additions\":{\"SYSTEM\":{\"properties\":{}," +
        "\"tags\":[\"kafka-plugins\"]}},\"deletions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}}}}\n",
      "{\"time\":1457467029436,\"entityId\":{\"namespace\":\"default\",\"application\":\"testCubeAdapter\"," +
        "\"entity\":\"APPLICATION\"},\"user\":\"\",\"type\":\"METADATA_CHANGE\",\"payload\":{\"previous\":" +
        "{\"SYSTEM\":{\"properties\":{},\"tags\":[]}},\"additions\":{\"SYSTEM\":{\"properties\":" +
        "{\"MapReduce:ETLMapReduce\":\"ETLMapReduce\",\"Workflow:ETLWorkflow\":\"ETLWorkflow\"," +
        "\"schedule:etlWorkflow\":\"etlWorkflow:ETL Batch schedule\",\"plugin:Table:batchsource\":" +
        "\"Table:batchsource\",\"plugin:Cube:batchsink\":\"Cube:batchsink\"},\"tags\":[]}},\"deletions\":" +
        "{\"SYSTEM\":{\"properties\":{},\"tags\":[]}}}}\n",
      "{\"time\":1457467029483,\"entityId\":{\"namespace\":\"default\",\"application\":\"testCubeAdapter\"," +
        "\"entity\":\"APPLICATION\"},\"user\":\"\",\"type\":\"METADATA_CHANGE\",\"payload\":{\"previous\":" +
        "{\"SYSTEM\":{\"properties\":{\"plugin:Table:batchsource\":\"Table:batchsource\",\"plugin:Cube:batchsink\":" +
        "\"Cube:batchsink\",\"MapReduce:ETLMapReduce\":\"ETLMapReduce\",\"Workflow:ETLWorkflow\":\"ETLWorkflow\"," +
        "\"schedule:etlWorkflow\":\"etlWorkflow:ETL Batch schedule\"},\"tags\":[]}},\"additions\":{\"SYSTEM\":" +
        "{\"properties\":{},\"tags\":[\"testCubeAdapter\",\"cdap-etl-batch\"]}},\"deletions\":{\"SYSTEM\":" +
        "{\"properties\":{},\"tags\":[]}}}}\n",
      "{\"time\":1457467029557,\"entityId\":{\"namespace\":\"default\",\"application\":\"testCubeAdapter\",\"type\":" +
        "\"Workflow\",\"program\":\"ETLWorkflow\",\"entity\":\"PROGRAM\"},\"user\":\"\",\"type\":\"METADATA_CHANGE\"," +
        "\"payload\":{\"previous\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}},\"additions\":{\"SYSTEM\":" +
        "{\"properties\":{},\"tags\":[\"ETLMapReduce\",\"Batch\",\"Workflow\",\"ETLWorkflow\"]}},\"deletions\":" +
        "{\"SYSTEM\":{\"properties\":{},\"tags\":[]}}}}\n",
      "{\"time\":1457467185081,\"entityId\":{\"namespace\":\"default\",\"application\":\"testCubeAdapter\"," +
        "\"entity\":\"APPLICATION\"},\"user\":\"\",\"type\":\"METADATA_CHANGE\",\"payload\":{\"previous\":{\"USER\":" +
        "{\"properties\":{},\"tags\":[]}},\"additions\":{\"USER\":{\"properties\":{},\"tags\":[]}},\"deletions\":" +
        "{\"USER\":{\"properties\":{},\"tags\":[]}}}}\n",
      "{\"time\":1457467185168,\"entityId\":{\"namespace\":\"default\",\"application\":\"testCubeAdapter\"," +
        "\"entity\":\"APPLICATION\"},\"user\":\"\",\"type\":\"METADATA_CHANGE\",\"payload\":{\"previous\":" +
        "{\"SYSTEM\":{\"properties\":{\"plugin:Table:batchsource\":\"Table:batchsource\",\"plugin:Cube:batchsink\":" +
        "\"Cube:batchsink\",\"MapReduce:ETLMapReduce\":\"ETLMapReduce\",\"Workflow:ETLWorkflow\":\"ETLWorkflow\"," +
        "\"schedule:etlWorkflow\":\"etlWorkflow:ETL Batch schedule\"},\"tags\":[\"testCubeAdapter\"," +
        "\"cdap-etl-batch\"]}},\"additions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}},\"deletions\":" +
        "{\"SYSTEM\":{\"properties\":{\"plugin:Table:batchsource\":\"Table:batchsource\",\"plugin:Cube:batchsink\":" +
        "\"Cube:batchsink\",\"MapReduce:ETLMapReduce\":\"ETLMapReduce\",\"Workflow:ETLWorkflow\":\"ETLWorkflow\"," +
        "\"schedule:etlWorkflow\":\"etlWorkflow:ETL Batch schedule\"},\"tags\":[\"testCubeAdapter\"," +
        "\"cdap-etl-batch\"]}}}}\n",
      "{\"time\":1457467185201,\"entityId\":{\"namespace\":\"default\",\"application\":\"testCubeAdapter\"," +
        "\"type\":\"Workflow\",\"program\":\"ETLWorkflow\",\"entity\":\"PROGRAM\"},\"user\":\"\",\"type\":" +
        "\"METADATA_CHANGE\",\"payload\":{\"previous\":{\"USER\":{\"properties\":{},\"tags\":[]}},\"additions\":" +
        "{\"USER\":{\"properties\":{},\"tags\":[]}},\"deletions\":{\"USER\":{\"properties\":{},\"tags\":[]}}}}\n",
      "{\"time\":1457467185252,\"entityId\":{\"namespace\":\"default\",\"application\":\"testCubeAdapter\"," +
        "\"type\":\"Workflow\",\"program\":\"ETLWorkflow\",\"entity\":\"PROGRAM\"},\"user\":\"\",\"type\":" +
        "\"METADATA_CHANGE\",\"payload\":{\"previous\":{\"SYSTEM\":{\"properties\":{},\"tags\":[\"ETLMapReduce\"," +
        "\"Batch\",\"Workflow\",\"ETLWorkflow\"]}},\"additions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}}," +
        "\"deletions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[\"ETLMapReduce\",\"Batch\",\"Workflow\"," +
        "\"ETLWorkflow\"]}}}}\n",
      "{\"time\":1457467196215,\"entityId\":{\"namespace\":\"default\",\"application\":\"input\",\"type\":" +
        "\"Service\",\"program\":\"DatasetService\",\"entity\":\"PROGRAM\"},\"user\":\"\",\"type\":" +
        "\"METADATA_CHANGE\",\"payload\":{\"previous\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}}," +
        "\"additions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[\"Realtime\",\"DatasetService\",\"Service\"]}}," +
        "\"deletions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}}}}\n",
      "{\"time\":1457467331680,\"entityId\":{\"namespace\":\"default\",\"application\":\"input\",\"type\":" +
        "\"Service\",\"program\":\"DatasetService\",\"entity\":\"PROGRAM\"},\"user\":\"\",\"type\":" +
        "\"METADATA_CHANGE\",\"payload\":{\"previous\":{\"USER\":{\"properties\":{},\"tags\":[]}},\"additions\":" +
        "{\"USER\":{\"properties\":{},\"tags\":[]}},\"deletions\":{\"USER\":{\"properties\":{},\"tags\":[]}}}}\n",
      "{\"time\":1457467331724,\"entityId\":{\"namespace\":\"default\",\"application\":\"input\",\"type\":" +
        "\"Service\",\"program\":\"DatasetService\",\"entity\":\"PROGRAM\"},\"user\":\"\",\"type\":" +
        "\"METADATA_CHANGE\",\"payload\":{\"previous\":{\"SYSTEM\":{\"properties\":{},\"tags\":[\"Realtime\"," +
        "\"Service\",\"DatasetService\"]}},\"additions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}}," +
        "\"deletions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[\"Realtime\",\"Service\",\"DatasetService\"]}}}}\n",
      "{\"time\":1457467342128,\"entityId\":{\"namespace\":\"default\",\"application\":\"input\",\"type\":" +
        "\"Service\",\"program\":\"DatasetService\",\"entity\":\"PROGRAM\"},\"user\":\"\",\"type\":" +
        "\"METADATA_CHANGE\",\"payload\":{\"previous\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}}," +
        "\"additions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[\"Realtime\",\"DatasetService\",\"Service\"]}}," +
        "\"deletions\":{\"SYSTEM\":{\"properties\":{},\"tags\":[]}}}}\n"
    };
    for (int i = 0; i < testData.length; i++) {
      emitter.emit(testData[i]);
    }
  }
}
