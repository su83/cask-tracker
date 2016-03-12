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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.tracker.entity.AuditLogResponse;
import co.cask.tracker.entity.AuditLogTable;
import co.cask.tracker.utils.TimeMathParser;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;


/**
 * This class handles requests to the AuditLog API.
 */
public final class AuditLogHandler extends AbstractHttpServiceHandler {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();
  private static final String DEFAULT_PAGE_SIZE = "10";
  private static final long DEFAULT_START_TIME = 0L;
  // If we scan more than this + offset, we return early since the UI can't display that many anyway.
  private static final long MAX_RESULTS_TO_SCAN = 100;

  @UseDataSet(TrackerApp.AUDIT_LOG_DATASET_NAME)
  private AuditLogTable auditLogTable;

  private String namespace;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    namespace = context.getNamespace();
  }

  @Path("auditlog/{type}/{name}")
  @GET
  public void query(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("type") String entityType,
                    @PathParam("name") String name,
                    @QueryParam("offset") int offset,
                    @QueryParam("limit") @DefaultValue(DEFAULT_PAGE_SIZE) int limit,
                    @QueryParam("startTime") String startTime,
                    @QueryParam("endTime") String endTime) {
    if (limit < 0) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "limit cannot be negative.");
      return;
    }
    if (offset < 0) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "offset cannot be negative.");
      return;
    }
    long startTimeLong = DEFAULT_START_TIME;
    if (startTime != null) {
      try {
        startTimeLong = TimeMathParser.parseTimeInSeconds(startTime);
      } catch (IllegalArgumentException e) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "startTime was not in the correct format.");
        return;
      }
    }
    long endTimeLong = TimeMathParser.nowInSeconds();
    if (endTime != null) {
      try {
        endTimeLong = TimeMathParser.parseTimeInSeconds(endTime);
      } catch (IllegalArgumentException e) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "endTime was not in the correct format.");
        return;
      }
    }
    if (startTimeLong > endTimeLong) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "startTime must be before endTime.");
      return;
    }
    List<AuditMessage> logList = new ArrayList<>();
    int totalResults = 0;
    AuditMessage message;
    CloseableIterator<AuditMessage> messageIter = auditLogTable.scan(namespace,
                                                                     entityType,
                                                                     name,
                                                                     startTimeLong,
                                                                     endTimeLong);
    try {
      // First skip to the offset
      if (offset > 0) {
        while (totalResults < offset && (message = messageIter.next()) != null) {
          totalResults++;
        }
      }
      while ((message = messageIter.next()) != null) {
        totalResults++;
        if (totalResults <= (limit + offset)) {
          logList.add(message);
        }
        // End early if there are too many results to scan.
        if (totalResults >= (MAX_RESULTS_TO_SCAN + offset)) {
          break;
        }
      }
    } catch (NoSuchElementException e) {
      //no-op
    } finally {
      messageIter.close();
    }

    AuditLogResponse resp = new AuditLogResponse(totalResults, logList, offset);
    responder.sendJson(200, resp);
  }
}

