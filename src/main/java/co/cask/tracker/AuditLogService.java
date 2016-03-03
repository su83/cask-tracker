package co.cask.tracker;

import co.cask.cdap.api.service.AbstractService;

/**
 * Created by russellsavage on 3/2/16.
 */
public class AuditLogService extends AbstractService {

  public static final String SERVICE_NAME = "AuditLogAPI";

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("A service that exposes the Tracker audit log as an API.");
    addHandler(new AuditLogHandler());
  }
}

