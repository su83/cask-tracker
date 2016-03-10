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

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.MessageType;
import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.tracker.entity.AuditLogResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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
  private static final int DEFAULT_PAGE_SIZE = 10;
  private static final long DEFAULT_START_TIME = 0L;
  private static final long DEFAULT_END_TIME = TimeMathParser.nowInSeconds();
  // If we scan more than this + offset, we return early since the UI can't display that many anyway.
  private static final long MAX_RESULTS_TO_SCAN = 100;

  @Property
  private final String auditLogTableName;
  private Table auditLogTable;

  private String namespace;

  public AuditLogHandler(String datasetName) {
    this.auditLogTableName = datasetName;
  }

  @Override
  public void configure() {
    super.configure();
  }

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    auditLogTable = context.getDataset(auditLogTableName);
    namespace = context.getNamespace();
  }

  @Path("auditlog/{type}/{name}")
  @GET
  public void query(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("type") String entityType,
                    @PathParam("name") String name,
                    @QueryParam("offset") int offset,
                    @QueryParam("limit") int limit,
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
    long endTimeLong = DEFAULT_END_TIME;
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
    if (limit == 0) {
      limit = DEFAULT_PAGE_SIZE;
    }
    List<AuditMessage> logList = new ArrayList<>();
    Scanner scanner = auditLogTable.scan(
      new Scan(
        Bytes.toBytes(AuditLogPublisher.getScanKey(this.namespace, entityType, name, startTimeLong)),
        Bytes.toBytes(AuditLogPublisher.getScanKey(this.namespace, entityType, name, endTimeLong))
      )
    );
    int totalResults = 0;
    Row row;
    // First skip to the offset
    if (offset > 0) {
      while (totalResults < offset && (row = scanner.next()) != null) {
        totalResults++;
      }
    }
    while ((row = scanner.next()) != null) {
      totalResults++;
      if (totalResults <= (limit + offset)) {
        AuditMessage message = getAuditMessage(row);
        logList.add(message);
      }
      // End early if there are too many results to scan.
      if (totalResults >= (MAX_RESULTS_TO_SCAN + offset)) {
        break;
      }
    }
    AuditLogResponse resp = new AuditLogResponse(totalResults, logList, offset);
    responder.sendJson(200, resp);
  }

  /**
   * Helper method to build the AuditMessage from a row
   * @param row the row from the scanner to build the AuditMessage from
   * @return a new AuditMessage based on the information in the row
   */
  private AuditMessage getAuditMessage(Row row) {
    EntityId entityId = GSON.fromJson(row.getString("entityId"), EntityId.class);
    MessageType messageType = MessageType.valueOf(row.getString("actionType"));
    AuditPayload payload;
    switch (messageType) {
      case METADATA_CHANGE:
        payload = GSON.fromJson(row.getString("metadata"), MetadataPayload.class);
        break;
      case ACCESS:
        payload = GSON.fromJson(row.getString("metadata"), AccessPayload.class);
        break;
      default:
        payload = AuditPayload.EMPTY_PAYLOAD;
    }
    return new AuditMessage(row.getLong("timestamp"),
                            entityId,
                            row.getString("user"),
                            messageType,
                            payload);
  }
}

