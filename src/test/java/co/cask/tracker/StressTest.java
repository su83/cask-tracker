/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tracker;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.audit.payload.access.AccessType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.tracker.entity.TrackerMeterRequest;
import co.cask.tracker.entity.TrackerMeterResult;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link TrackerApp}.
 */
public class StressTest extends TestBase {

  private List<AuditMessage> testData = new ArrayList<>();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();
  private static ApplicationManager testAppManager;
  private static ServiceManager trackerServiceManager;
  private static Random random = new Random();
  private List<String> datasets = new LinkedList<>();
  private List<String> streams = new LinkedList<>();

  private static final long BASE_TIMESTAMP = 1469051147000L;

  private String metadataPayload = "{ \"previous\": { \"USER\": { \"properties\": { \"uk\": \"uv\", \"uk1\": \"uv2\" }, " +
    "\"tags\": [ \"ut1\", \"ut2\" ] }, \"SYSTEM\": { \"properties\": { \"sk\": \"sv\" }, \"tags\": [] } }, " +
    "\"additions\": { \"SYSTEM\": { \"properties\": { \"sk\": \"sv\" }, \"tags\": [ \"t1\", \"t2\" ] } }, " +
    "\"deletions\": { \"USER\": { \"properties\": { \"uk\": \"uv\" }, \"tags\": [ \"ut1\" ] } } }";
  private MetadataPayload payload = GSON.fromJson(metadataPayload, MetadataPayload.class);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Before
  public void configureStream() throws Exception {
    testAppManager = deployApplication(TestAuditLogPublisherApp.class);
    FlowManager testFlowManager = testAppManager.getFlowManager(StreamToAuditLogFlow.FLOW_NAME).start();
    testFlowManager.waitForStatus(true);

    trackerServiceManager = testAppManager.getServiceManager(TrackerService.SERVICE_NAME).start();
    trackerServiceManager.waitForStatus(true);

    StreamManager streamManager = getStreamManager("testStream");
    generateTestData();
    for (AuditMessage auditMessage : testData) {
      streamManager.send(GSON.toJson(auditMessage));

    }
    RuntimeMetrics metrics = testFlowManager.getFlowletMetrics("auditLogPublisher");
    metrics.waitForProcessed(testData.size(), 60L, TimeUnit.SECONDS);
  }

  @After
  public void destroyApp() throws Exception {
    testAppManager.stopAll();
    clear();
  }

  @Test
  public void stressTest() throws Exception{
    TrackerMeterResult result = getTrackerMeterResponse(datasets, streams, HttpResponseStatus.OK.getCode());
    String gson = GSON.toJson(result);

  }

  private TrackerMeterResult getTrackerMeterResponse(List<String> datasets,
                                                     List<String> streams,
                                                     int expectedResponse) throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/tracker-meter", "POST",
                                         GSON.toJson(new TrackerMeterRequest(datasets, streams)),
                                         expectedResponse);
    return GSON.fromJson(response, TrackerMeterResult.class);
  }

  // Overload (String Type). For requests other than GET.
  private String getServiceResponse(ServiceManager serviceManager,
                                    String request, String type, String postRequest,
                                    int expectedResponseCode) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), request);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod(type);

    //Feed JSON data if POST
    if (type.equals("POST")) {
      connection.setDoOutput(true);
      connection.getOutputStream().write(postRequest.getBytes());
    }

    Assert.assertEquals(expectedResponseCode, connection.getResponseCode());
    String response;
    try {
      if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
      } else if (connection.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
        response = new String(ByteStreams.toByteArray(connection.getErrorStream()), Charsets.UTF_8);
      } else {
        throw new Exception("Invalid response code returned: " + connection.getResponseCode());
      }
    } finally {
      connection.disconnect();
    }
    return response;
  }


  // Adapted from https://wiki.cask.co/display/CE/Audit+information+publishing
  private void generateTestData() {
    for (int i = 0; i <= 1000; i++) {
      long randomTimestamp = getRandomTimestamp();
      String type = getRandomType();
      testData.add(new AuditMessage(randomTimestamp,
                                    EntityId.fromString(type + ":default." + type + i),
                                    "user1",
                                    AuditType.CREATE,
                                    AuditPayload.EMPTY_PAYLOAD));
      if (type.equals("stream")) {
         streams.add(type + i);
      } else if (type.equals("dataset")) {
        datasets.add(type + i);
      }
      //Add 1 to 15 messages for each entity
      int maxCount = random.nextInt(25) + 1;
      for (int count = 0; count <= maxCount; count++) {
        AuditType auditType = getRandomAuditType();
        if (auditType == AuditType.ACCESS) {
          AccessType accessType = getRandomAccessType();
          if (accessType == AccessType.READ) {
            addRead(randomTimestamp, type, type + i);
          } else if (accessType == AccessType.WRITE) {
            addWrite(randomTimestamp, type, type + i);
          } else {
            addUnknown(randomTimestamp, type, type + i);
          }
        } else if (auditType == AuditType.METADATA_CHANGE) {
          addMetadata(randomTimestamp, type, type + i);
        }
      }
    }
  }

  private void addRead(long timestamp, String entityType, String entityName) {
    if (entityType.equals("stream")) {
      testData.add(new AuditMessage(timestamp,
                                    NamespaceId.DEFAULT.stream(entityName),
                                    "user1",
                                    AuditType.ACCESS,
                                    new AccessPayload(AccessType.READ,
                                                      EntityId.fromString(getRandomProgramString()))
      ));
    } else {
      testData.add(new AuditMessage(timestamp,
                                    NamespaceId.DEFAULT.dataset(entityName),
                                    "user1",
                                    AuditType.ACCESS,
                                    new AccessPayload(AccessType.READ,
                                                      EntityId.fromString(getRandomProgramString()))
      ));
    }
  }

  private void addWrite(long timestamp, String entityType, String entityName) {
    if (entityType.equals("stream")) {
      testData.add(new AuditMessage(timestamp,
                                    NamespaceId.DEFAULT.stream(entityName),
                                    "user1",
                                    AuditType.ACCESS,
                                    new AccessPayload(AccessType.WRITE,
                                                      EntityId.fromString(getRandomProgramString()))
      ));
    } else {
      testData.add(new AuditMessage(timestamp,
                                    NamespaceId.DEFAULT.dataset(entityName),
                                    "user1",
                                    AuditType.ACCESS,
                                    new AccessPayload(AccessType.WRITE,
                                                      EntityId.fromString(getRandomProgramString()))
      ));
    }
  }

  private void addUnknown(long timestamp, String entityType, String entityName) {
    if (entityType.equals("stream")) {
      testData.add(new AuditMessage(timestamp,
                                    NamespaceId.DEFAULT.stream(entityName),
                                    "user1",
                                    AuditType.ACCESS,
                                    new AccessPayload(AccessType.UNKNOWN,
                                                      EntityId.fromString("system_service:explore"))
      ));
    } else {
      testData.add(new AuditMessage(timestamp,
                                    NamespaceId.DEFAULT.dataset(entityName),
                                    "user1",
                                    AuditType.ACCESS,
                                    new AccessPayload(AccessType.UNKNOWN,
                                                      EntityId.fromString("system_service:explore"))
      ));
    }
  }

  private void addMetadata(long timestamp, String entityType, String entityName) {
    if (entityType.equals("stream")) {
      testData.add(new AuditMessage(timestamp,
                                    NamespaceId.DEFAULT.stream(entityName),
                                    "user1",
                                    AuditType.METADATA_CHANGE,
                                    payload)
      );
    } else {
      testData.add(new AuditMessage(timestamp,
                                    NamespaceId.DEFAULT.dataset(entityName),
                                    "user1",
                                    AuditType.METADATA_CHANGE,
                                    payload)
      );
    }
  }

  private String getRandomProgramString(){
    int randomInt = random.nextInt(10);
    if (randomInt == 0) {
      return "system_service:explore";
    } else {
      return String.format("program:default.app%s.SERVICE.%s", random.nextInt(17) + 1, random.nextInt(5) + 1);
    }
  }

  private String getRandomType() {
    if (random.nextInt(2) == 1) {
      return EntityType.DATASET.name().toLowerCase();
    }
    return EntityType.STREAM.name().toLowerCase();
  }

  private long getRandomTimestamp() {
    return BASE_TIMESTAMP + random.nextInt(86400000);
  }

  private AuditType getRandomAuditType() {
    int randomInt = random.nextInt(10);
    if (randomInt < 3) {
      return AuditType.METADATA_CHANGE;
    }
    return AuditType.ACCESS;
  }
  private AccessType getRandomAccessType() {
    int randomInt = random.nextInt(10);
    if (randomInt < 3) {
      return AccessType.UNKNOWN;
    } else if (randomInt > 6) {
      return AccessType.WRITE;
    }
    return AccessType.READ;
  }
}
