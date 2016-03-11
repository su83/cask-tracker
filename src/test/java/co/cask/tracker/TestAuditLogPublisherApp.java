package co.cask.tracker;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.table.Table;

/**
 * This app is used to test the AuditLog flowlet.
 */
public class TestAuditLogPublisherApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("TestAuditLogPublisherApp");
    setDescription("A temp app to test the AuditLogPublisher flowlet");
    createDataset(TrackerApp.AUDIT_LOG_DATASET_NAME, Table.class);
    addFlow(new StreamToAuditLogFlow());
    addService(new AuditLogService());
  }
}
