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
import javax.ws.rs.QueryParam;


/**
 * AuditLogHandler - Handle requests to the AuditLog API.
 */
public final class AuditLogHandler extends AbstractHttpServiceHandler {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();
  private static final Logger LOG = LoggerFactory.getLogger(AuditLogHandler.class);
  private static final int DEFAULT_PAGE_SIZE = 10;
  private static final long DEFAULT_START_TIME = 0L;
  private static final long DEFAULT_END_TIME = TimeMathParser.nowInSeconds();

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

  @Path("auditlog")
  @GET
  public void query(HttpServiceRequest request, HttpServiceResponder responder,
                    @QueryParam("type") String entityType,
                    @QueryParam("name") String name,
                    @QueryParam("offset") int offset,
                    @QueryParam("pageSize") int pageSize,
                    @QueryParam("startTime") String startTime,
                    @QueryParam("endTime") String endTime) {
    if (pageSize == 0) {
      pageSize = DEFAULT_PAGE_SIZE;
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
    List<AuditMessage> logList = new ArrayList<>();
    Scanner scanner = auditLogTable.scan(
      new Scan(
        Bytes.toBytes(AuditLogPublisher.getKey(this.namespace, entityType, name, startTimeLong)),
        Bytes.toBytes(AuditLogPublisher.getKey(this.namespace, entityType, name, endTimeLong))
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
      if (totalResults <= (pageSize + offset)) {
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
        AuditMessage message = new AuditMessage(row.getLong("timestamp"),
                                                entityId,
                                                row.getString("user"),
                                                messageType,
                                                payload);
        logList.add(message);
      }
    }
    AuditLogResponse resp = new AuditLogResponse(totalResults, logList, offset);
    responder.sendJson(200, resp);
  }
}

