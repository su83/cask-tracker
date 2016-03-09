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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.FlowletQueueId;
import co.cask.cdap.proto.id.NamespacedArtifactId;
import co.cask.cdap.proto.id.NamespacedId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import joptsimple.internal.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A flowlet to write Audit Log data to a dataset.
 */
public final class AuditLogPublisher extends AbstractFlowlet {
  private static final String DEFAULT_USER = "unknown";
  private static final Logger LOG = LoggerFactory.getLogger(AuditLogPublisher.class);
  private static final Gson GSON = new GsonBuilder()
        .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
        .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
        .create();
  @Property
  private final String auditLogDatasetName;
  private Table auditLog;

  /**
   * @param datasetName The name of the dataset to write the audit log to
   */
  public AuditLogPublisher(String datasetName) {
    this.auditLogDatasetName = datasetName;
  }

  /******
   * This method generates the key to use for the data table.
   * @param namespace the namespace where the entity exists
   * @param entityType the type of the entity
   * @param entityName the name of the entity
   * @param timestamp the timestamp of the entity to search for
   * @return A string that can be used as a key in the dataset
   */
  public static String getKey(String namespace, String entityType, String entityName, Long timestamp) {
    return namespace + "-" + entityType + "-" + entityName + "-" + timestamp;
  }

  @Override
  protected void configure() {
    super.configure();
    createDataset(auditLogDatasetName, Table.class);
  }

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    auditLog = context.getDataset(auditLogDatasetName);
  }


  @ProcessInput
  public void process(String event) {
    byte[] toStore = Bytes.toBytes(event);
    if (toStore.length > 0) {
      AuditMessage message = GSON.fromJson(event, AuditMessage.class);
      EntityId entityId = message.getEntityId();
      String namespace = Strings.EMPTY;
      String type = "";
      String name = "";
      if (entityId instanceof NamespacedId) {
        namespace = ((NamespacedId) entityId).getNamespace();
        EntityType entityType = entityId.getEntity();
        type = entityType.name().toLowerCase();
        // Unfortunately, there's no generic way to get the name of the entity
        // so we need this switch statement and a bunch of casting.
        switch (entityType) {
          case APPLICATION:
            name = ((ApplicationId) entityId).getApplication();
            break;
          case ARTIFACT:
            name = ((NamespacedArtifactId) entityId).getArtifact();
            break;
          case DATASET:
            name = ((DatasetId) entityId).getDataset();
            break;
          case DATASET_MODULE:
            name = ((DatasetModuleId) entityId).getModule();
            break;
          case DATASET_TYPE:
            name = ((DatasetTypeId) entityId).getType();
            break;
          case FLOWLET:
            name = ((FlowletId) entityId).getFlowlet();
            break;
          case FLOWLET_QUEUE:
            name = ((FlowletQueueId) entityId).getQueue();
            break;
          case NOTIFICATION_FEED:
            name = ((NotificationFeedId) entityId).getFeed();
            break;
          case PROGRAM:
            name = ((ProgramId) entityId).getProgram();
            break;
          case PROGRAM_RUN:
            name = ((ProgramRunId) entityId).getRun();
            break;
          case SCHEDULE:
            name = ((ScheduleId) entityId).getSchedule();
            break;
          case STREAM:
            name = ((StreamId) entityId).getStream();
            break;
          case STREAM_VIEW:
            name = ((StreamViewId) entityId).getView();
            break;
          default:
            LOG.warn("Unknown entity type: " + entityType);
        }
      } else {
        LOG.warn("Entity does not have a namespace: " + entityId);
      }
      String user = message.getUser();
      if (user == null || user.isEmpty()) {
        user = DEFAULT_USER;
      }
      this.auditLog.put(
        new Put(getKey(namespace, type, name, message.getTime()))
          .add("timestamp", message.getTime())
          .add("entityId", GSON.toJson(message.getEntityId()))
          .add("user", user)
          .add("actionType", message.getType().name())
          .add("entityKind", type)
          .add("entityName", name)
          .add("metadata", GSON.toJson(message.getPayload())));
    }
  }
}
