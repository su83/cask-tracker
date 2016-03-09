package co.cask.tracker;

import co.cask.cdap.api.app.AbstractApplication;

/**
 * This app is used to test the AuditLog flowlet
 */
public class TestAuditLogPublisherApp extends AbstractApplication {
  public final static String AUDIT_LOG_DATASET_NAME = "testDataset";

  @Override
  public void configure() {
    setName("TestAuditLogPublisherApp");
    setDescription("A temp app to test the AuditLogPublisher flowlet");
    addFlow(new GeneratorToAuditLogFlow());
    addService(new AuditLogService(AUDIT_LOG_DATASET_NAME));
  }
}
