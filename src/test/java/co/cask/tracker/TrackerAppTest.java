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

import co.cask.cdap.test.ApplicationManager;
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

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Test for {@link TrackerApp}.
 */
public class TrackerAppTest extends TestBase {
  private static final Gson GSON = new GsonBuilder().create();
  private static final Logger LOG = LoggerFactory.getLogger(AuditLogHandler.class);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Test
  public void test() throws Exception {
    // Deploy the TrackerApp application
    ApplicationManager appManager = deployApplication(TrackerApp.class);

    // Start AuditLog service and use it
    ServiceManager serviceManager = appManager.getServiceManager(AuditLogService.SERVICE_NAME).start();

    // Wait service startup
    serviceManager.waitForStatus(true);

    URL url = new URL(serviceManager.getServiceURL(), "auditlog?offset=5");

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    String response;
    try {
      response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
    } finally {
      connection.disconnect();
    }
    AuditLogResponse resp = GSON.fromJson(response, AuditLogResponse.class);
    Assert.assertEquals(4270,resp.getNumberOfResults());
    Assert.assertEquals(5,resp.getOffset());
  }
}
