package co.cask.tracker;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.table.Table;

/**
 * A temp app to test the Data dictionary functionality
 */
public class TestDictionaryApp extends AbstractApplication {
  /**
   * method to configure the application.
   */
  @Override
  public void configure() {
    setName("TestDataDictionaryApp");
    setDescription("A temp app to test the Data dictionary functionality");
    createDataset(TrackerApp.DATA_DICTIONARY_DATASET_NAME, Table.class);
    addService(new TrackerService());
  }
}
