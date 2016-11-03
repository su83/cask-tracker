package co.cask.tracker;

import co.cask.cdap.api.service.AbstractService;

/**
 * A service for accessing the DataDictionary endpoints through a RESTful API.
 */
public class DataDictionaryService extends AbstractService {
  public static final String SERVICE_NAME = "DataDictionaryService";
  /**
   * Implements this method to configure this Service.
   */
  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("A service that exposes the data dictionary endpoints as an API.");
  }
}
