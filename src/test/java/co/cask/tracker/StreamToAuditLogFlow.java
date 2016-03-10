package co.cask.tracker;

import co.cask.cdap.api.flow.AbstractFlow;

/**
 * This class is used to create a test flow connecting the Generator to the AuditLog Flowlet.
 */
public class StreamToAuditLogFlow extends AbstractFlow {
  public static final String FLOW_NAME = "StreamToAuditLogFlow";

  @Override
  public void configure() {
    setName(FLOW_NAME);
    setDescription("A temp flow to test the audit log");
    //addFlowlet("messageGeneratorFlowlet", new MessageGeneratorFlowlet());
    addStream("testStream");
    addFlowlet("auditLogPublisher", new AuditLogPublisher(TestAuditLogPublisherApp.AUDIT_LOG_DATASET_NAME));
    connectStream("testStream","auditLogPublisher");
  }
}
