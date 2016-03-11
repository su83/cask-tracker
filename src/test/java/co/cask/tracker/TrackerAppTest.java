/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.tracker.entity.AuditLogResponse;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Product;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link TrackerApp}.
 */
public class TrackerAppTest extends TestBase {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();
  // This is the starting unix timestamp for generating the test data.
  private static final long STARTING_TIMESTAMP = 1456956659468L;
  private static ApplicationManager testAppManager;
  private static ServiceManager serviceManager;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Before
  public void configureStream() throws Exception {
    testAppManager = deployApplicationWithScalaJar(TestAuditLogPublisherApp.class, null);
    FlowManager testFlowManager = testAppManager.getFlowManager(StreamToAuditLogFlow.FLOW_NAME).start();
    testFlowManager.waitForStatus(true);

    serviceManager = testAppManager.getServiceManager(AuditLogService.SERVICE_NAME).start();
    serviceManager.waitForStatus(true);

    StreamManager streamManager = getStreamManager("testStream");
    // Adapted from https://wiki.cask.co/display/CE/Audit+information+publishing
    String[] testData = new String[]{
      "{ \"time\": %d, \"entityId\": { \"namespace\": \"default\", \"stream\": \"stream1\", \"entity\": " +
        "\"STREAM\" }, \"user\": \"user1\", \"type\": \"ACCESS\", \"payload\": { \"accessType\": \"WRITE\", " +
        "\"accessor\": { \"namespace\": \"ns1\", \"application\": \"app1\", \"type\": \"Flow\", \"program\": " +
        "\"flow1\", \"run\": \"run1\", \"entity\": \"PROGRAM_RUN\" } } }",
      "{ \"time\": %d, \"entityId\": { \"namespace\": \"default\", \"stream\": \"stream1\", \"entity\": " +
        "\"STREAM\" }, \"user\": \"user1\", \"type\": \"ACCESS\", \"payload\": { \"accessType\": \"UNKNOWN\", " +
        "\"accessor\": { \"service\": \"explore\", \"entity\": \"SYSTEM_SERVICE\" } } }",
      "{ \"time\": %d, \"entityId\": { \"namespace\": \"default\", \"application\": \"app1\", \"entity\": " +
        "\"APPLICATION\" }, \"user\": \"user1\", \"type\": \"METADATA_CHANGE\", \"payload\": { \"previous\": " +
        "{ \"USER\": { \"properties\": { \"uk\": \"uv\", \"uk1\": \"uv2\" }, \"tags\": [ \"ut1\", \"ut2\" ] }, " +
        "\"SYSTEM\": { \"properties\": { \"sk\": \"sv\" }, \"tags\": [] } }, \"additions\": { \"SYSTEM\": { " +
        "\"properties\": { \"sk\": \"sv\" }, \"tags\": [ \"t1\", \"t2\" ] } }, \"deletions\": { \"USER\": { " +
        "\"properties\": { \"uk\": \"uv\" }, \"tags\": [ \"ut1\" ] } } } }",
      "{ \"time\": %d, \"entityId\": { \"namespace\": \"default\", \"dataset\": \"ds1\", \"entity\": " +
        "\"DATASET\" }, \"user\": \"user1\", \"type\": \"CREATE\", \"payload\": {} }",
    };
    long timer = STARTING_TIMESTAMP;
    for (int j = 0; j < 6; j++) {
      for (int i = 0; i < testData.length; i++) {
        streamManager.send(String.format(testData[i], timer));
        timer++;
      }
    }
    RuntimeMetrics metrics = testFlowManager.getFlowletMetrics("auditLogPublisher");
    metrics.waitForProcessed(12, 60L, TimeUnit.SECONDS);
  }

  @After
  public void destroyApp() throws Exception {
    testAppManager.stopAll();
    clear();
  }

  @Test
  public void testSingleResult() throws Exception {
    String response = getServiceResponse("auditlog/stream/stream1?startTime=1456956659467&endTime=1456956659469");
    AuditLogResponse resp = GSON.fromJson(response, AuditLogResponse.class);
    Assert.assertEquals(1,resp.getTotalResults());
    Assert.assertEquals("stream:default.stream1",
                        resp.getResults().get(0).getEntityId().toString());
  }

  @Test
  public void testMultipleResults() throws Exception {
    String response = getServiceResponse("auditlog/stream/stream1?endTime=1456956659490");
    AuditLogResponse resp = GSON.fromJson(response, AuditLogResponse.class);
    Assert.assertEquals(12,resp.getTotalResults());
    for (int i = 0; i < resp.getResults().size(); i++) {
      Assert.assertEquals("stream:default.stream1",
                          resp.getResults().get(i).getEntityId().toString());
    }
  }

  @Test
  public void testOffset() throws Exception {
    String response = getServiceResponse("auditlog/stream/stream1?offset=1&endTime=1456956659490");
    AuditLogResponse resp = GSON.fromJson(response, AuditLogResponse.class);
    Assert.assertEquals(12,resp.getTotalResults());
    Assert.assertEquals(1,resp.getOffset());
    Assert.assertEquals(10,resp.getResults().size());
    for (int i = 0; i < resp.getResults().size(); i++) {
      Assert.assertEquals("stream:default.stream1",
                          resp.getResults().get(i).getEntityId().toString());
    }
  }

  @Test
  public void testPageSize() throws Exception {
    String response = getServiceResponse("auditlog/stream/stream1?limit=1&endTime=1456956659490");
    AuditLogResponse resp = GSON.fromJson(response, AuditLogResponse.class);
    Assert.assertEquals(12,resp.getTotalResults());
    Assert.assertEquals(0,resp.getOffset());
    Assert.assertEquals(1,resp.getResults().size());
    Assert.assertEquals("stream:default.stream1",
                        resp.getResults().get(0).getEntityId().toString());
  }

  @Test
  public void testInvalidDatesError() throws Exception {
    String response = getServiceResponse("auditlog/stream/stream1?startTime=1&endTime=0");
    Assert.assertEquals("\"startTime must be before endTime.\"",response);
  }

  @Test
  public void testInvalidOffset() throws Exception {
    String response = getServiceResponse("auditlog/stream/stream1?offset=-1");
    Assert.assertEquals("\"offset cannot be negative.\"",response);
  }

  @Test
  public void testInvalidLimit() throws Exception {
    String response = getServiceResponse("auditlog/stream/stream1?limit=-1");
    Assert.assertEquals("\"limit cannot be negative.\"",response);
  }

  private static ApplicationManager deployApplicationWithScalaJar(Class appClass, Config config) {
    URL classUrl = Product.class.getClassLoader().getResource("scala/Product.class");
    String path = classUrl.getFile();
    if (config != null) {
      return deployApplication(appClass, config, new File(URI.create(path.substring(0, path.indexOf("!/")))));
    } else {
      return deployApplication(appClass, new File(URI.create(path.substring(0, path.indexOf("!/")))));
    }
  }

  private String getServiceResponse(String request) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), request);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    String response;
    try {
      if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
      } else if (connection.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
        response = new String(ByteStreams.toByteArray(connection.getErrorStream()), Charsets.UTF_8);
      } else {
        throw new Exception("Invalid response code returned: "+connection.getResponseCode());
      }
    } finally {
      connection.disconnect();
    }
    return response;
  }
}
