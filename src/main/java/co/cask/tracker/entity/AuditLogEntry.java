package co.cask.tracker.entity;

/**
 * Created by russellsavage on 3/2/16.
 */
public class AuditLogEntry {
  private long timestamp;
  private String user;
  private String actionType;
  private String entityKind;
  private String entityName;
  private String metadata;


  public AuditLogEntry(long timestamp, String user, String actionType, String entityKind, String entityName, String metadata) {
    this.timestamp = timestamp;
    this.user = user;
    this.actionType = actionType;
    this.entityKind = entityKind;
    this.entityName = entityName;
    this.metadata = metadata;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getActionType() {
    return actionType;
  }

  public void setActionType(String actionType) {
    this.actionType = actionType;
  }

  public String getEntityKind() {
    return entityKind;
  }

  public void setEntityKind(String entityKind) {
    this.entityKind = entityKind;
  }

  public String getEntityName() {
    return entityName;
  }

  public void setEntityName(String entityName) {
    this.entityName = entityName;
  }

  public String getMetadata() {
    return metadata;
  }

  public void setMetadata(String metadata) {
    this.metadata = metadata;
  }
}
