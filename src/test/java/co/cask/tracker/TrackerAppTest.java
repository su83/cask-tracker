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
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.tracker.entity.AuditLogResponse;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
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
  private static final Logger LOG = LoggerFactory.getLogger(AuditLogHandler.class);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Test
  public void testSingleResult() throws Exception {
    ApplicationManager testAppManager = deployApplicationWithScalaJar(TestAuditLogPublisherApp.class, null);
    FlowManager testFlowManager = testAppManager.getFlowManager(GeneratorToAuditLogFlow.FLOW_NAME).start();
    testFlowManager.waitForStatus(true);

    RuntimeMetrics metrics = testFlowManager.getFlowletMetrics("auditLogPublisher");
    metrics.waitForProcessed(10, 60, TimeUnit.SECONDS);

    ServiceManager serviceManager = testAppManager.getServiceManager(AuditLogService.SERVICE_NAME).start();
    serviceManager.waitForStatus(true);

    URL url = new URL(serviceManager.getServiceURL(),
                      "auditlog?type=program&name=ETLWorkflow&startTime=1457467029550&endTime=1457467029560");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    String response;
    try {
      response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
    } finally {
      connection.disconnect();
    }
    AuditLogResponse resp = GSON.fromJson(response, AuditLogResponse.class);
    Assert.assertEquals(1,resp.getTotalResults());
    Assert.assertEquals("program:default.testCubeAdapter.workflow.ETLWorkflow",
                        resp.getResults().get(0).getEntityId().toString());
  }

  @Test
  public void testMultipleResults() throws Exception {
    ApplicationManager testAppManager = deployApplicationWithScalaJar(TestAuditLogPublisherApp.class, null);
    FlowManager testFlowManager = testAppManager.getFlowManager(GeneratorToAuditLogFlow.FLOW_NAME).start();
    testFlowManager.waitForStatus(true);

    RuntimeMetrics metrics = testFlowManager.getFlowletMetrics("auditLogPublisher");
    metrics.waitForProcessed(10, 60, TimeUnit.SECONDS);

    ServiceManager serviceManager = testAppManager.getServiceManager(AuditLogService.SERVICE_NAME).start();
    serviceManager.waitForStatus(true);

    URL url = new URL(serviceManager.getServiceURL(), "auditlog?type=program&name=ETLWorkflow");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    String response;
    try {
      response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
    } finally {
      connection.disconnect();
    }
    AuditLogResponse resp = GSON.fromJson(response, AuditLogResponse.class);
    Assert.assertEquals(3,resp.getTotalResults());
    Assert.assertEquals("program:default.testCubeAdapter.workflow.ETLWorkflow",
                        resp.getResults().get(0).getEntityId().toString());
    Assert.assertEquals("program:default.testCubeAdapter.workflow.ETLWorkflow",
                        resp.getResults().get(1).getEntityId().toString());
    Assert.assertEquals("program:default.testCubeAdapter.workflow.ETLWorkflow",
                        resp.getResults().get(2).getEntityId().toString());
  }

  @Test
  public void testOffset() throws Exception {
    ApplicationManager testAppManager = deployApplicationWithScalaJar(TestAuditLogPublisherApp.class, null);
    FlowManager testFlowManager = testAppManager.getFlowManager(GeneratorToAuditLogFlow.FLOW_NAME).start();
    testFlowManager.waitForStatus(true);

    RuntimeMetrics metrics = testFlowManager.getFlowletMetrics("auditLogPublisher");
    metrics.waitForProcessed(10, 60, TimeUnit.SECONDS);

    ServiceManager serviceManager = testAppManager.getServiceManager(AuditLogService.SERVICE_NAME).start();
    serviceManager.waitForStatus(true);

    URL url = new URL(serviceManager.getServiceURL(), "auditlog?type=program&name=ETLWorkflow&offset=1");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    String response;
    try {
      response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
    } finally {
      connection.disconnect();
    }
    AuditLogResponse resp = GSON.fromJson(response, AuditLogResponse.class);
    Assert.assertEquals(3,resp.getTotalResults());
    Assert.assertEquals(1,resp.getOffset());
    Assert.assertEquals(2,resp.getResults().size());
    Assert.assertEquals("program:default.testCubeAdapter.workflow.ETLWorkflow",
                        resp.getResults().get(0).getEntityId().toString());
    Assert.assertEquals("program:default.testCubeAdapter.workflow.ETLWorkflow",
                        resp.getResults().get(1).getEntityId().toString());
  }

  @Test
  public void testPageSize() throws Exception {
    ApplicationManager testAppManager = deployApplicationWithScalaJar(TestAuditLogPublisherApp.class, null);
    FlowManager testFlowManager = testAppManager.getFlowManager(GeneratorToAuditLogFlow.FLOW_NAME).start();
    testFlowManager.waitForStatus(true);

    RuntimeMetrics metrics = testFlowManager.getFlowletMetrics("auditLogPublisher");
    metrics.waitForProcessed(10, 60, TimeUnit.SECONDS);

    ServiceManager serviceManager = testAppManager.getServiceManager(AuditLogService.SERVICE_NAME).start();
    serviceManager.waitForStatus(true);

    URL url = new URL(serviceManager.getServiceURL(), "auditlog?type=program&name=ETLWorkflow&pageSize=1");
    LOG.warn(url.toString());
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    String response;
    try {
      response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
    } finally {
      connection.disconnect();
    }
    AuditLogResponse resp = GSON.fromJson(response, AuditLogResponse.class);
    Assert.assertEquals(3,resp.getTotalResults());
    Assert.assertEquals(0,resp.getOffset());
    Assert.assertEquals(1,resp.getResults().size());
    Assert.assertEquals("program:default.testCubeAdapter.workflow.ETLWorkflow",
                        resp.getResults().get(0).getEntityId().toString());
  }

  private ApplicationManager deployApplicationWithScalaJar(Class appClass, Config config) {
    URL classUrl = Product.class.getClassLoader().getResource("scala/Product.class");
    String path = classUrl.getFile();
    if (config != null) {
      return deployApplication(appClass, config, new File(URI.create(path.substring(0, path.indexOf("!/")))));
    } else {
      return deployApplication(appClass, new File(URI.create(path.substring(0, path.indexOf("!/")))));
    }
  }
}
