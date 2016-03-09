package co.cask.tracker;

import co.cask.cdap.api.flow.AbstractFlow;

/**
 * This class is used to create a test flow connecting the Generator to the AuditLog Flowlet
 */
public class GeneratorToAuditLogFlow extends AbstractFlow {
  public static final String FLOW_NAME = "GeneratorToAuditLogFlow";

  @Override
  public void configure() {
    setName(FLOW_NAME);
    setDescription("A temp flow to test the audit log");
    addFlowlet("messageGeneratorFlowlet", new MessageGeneratorFlowlet());
    addFlowlet("auditLogPublisher", new AuditLogPublisher(TestAuditLogPublisherApp.AUDIT_LOG_DATASET_NAME));
    connect("messageGeneratorFlowlet", "auditLogPublisher");
  }
}
