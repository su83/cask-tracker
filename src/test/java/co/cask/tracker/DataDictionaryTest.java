package co.cask.tracker;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import co.cask.tracker.entity.DictionaryResult;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import static co.cask.tracker.TestUtils.getServiceResponse;

/**
 * Unit tests for DataDictionary
 */
public class DataDictionaryTest extends TestBase {
  private static final Gson GSON = new Gson();
  private static final DictionaryResult dictionaryInput = new DictionaryResult(null, "String", true, false,
                                                                               "test description", null);
  private static final String requestJson = GSON.toJson(dictionaryInput);
  private static final DictionaryResult dictionaryInput2 = new DictionaryResult(null, "String", true, false,
                                                                                "this is a description", null);
  private static final String requestJson2 = GSON.toJson(dictionaryInput2);
  private static final DictionaryResult dictionaryInput3 = new DictionaryResult(null, "Int", null, null,
                                                                                "newDescription", null);
  private static final String requestJson3 = GSON.toJson(dictionaryInput3);
  private static ApplicationManager testAppManager;
  private static ServiceManager dictionaryServiceManager;

  private static String colName;
  private static String colNameSecond;
  private static String colNameThird;

  @Before
  public void configureStream() throws Exception {
    testAppManager = deployApplication(TestDictionaryApp.class);
    dictionaryServiceManager = testAppManager.getServiceManager(TrackerService.SERVICE_NAME).start();
    dictionaryServiceManager.waitForStatus(true);
  }

  // Tests for Add functionality
  @Test
  public void testAdd() throws Exception {
    colName = "mycol";
    String rjson = requestJson;
    // Test for adding a column
    getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName,
                       "POST", requestJson, HttpResponseStatus.OK.getCode());
    DataSetManager<Table> outputmanager = getDataset(TrackerApp.DATA_DICTIONARY_DATASET_NAME);
    Get get = new Get(colName);
    Row result = outputmanager.get().get(get);
    Assert.assertTrue(result.getString(DataDictionaryHandler.FieldNames.columnType.name()).
      equalsIgnoreCase(Schema.Type.STRING.name()));
    Assert.assertEquals(result.getString(DataDictionaryHandler.FieldNames.columnName.name()), colName);
    Assert.assertEquals(result.getString(DataDictionaryHandler.FieldNames.columnName.name()), colName);
    Assert.assertTrue(result.getBoolean(DataDictionaryHandler.FieldNames.isNullable.name()));
    Assert.assertFalse(result.getBoolean(DataDictionaryHandler.FieldNames.isPII.name()));

    // Test for duplicate column add
    String response = getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName, "POST", requestJson,
                                         HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals("mycol already exists in data dictionary", response);
    outputmanager.flush();
  }

  @Test
  public void testWithNullValues() throws Exception {
    colNameSecond = "colWithNullValues";
    // Test for adding column with optional FieldNames as null
    getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colNameSecond, "POST", requestJson3,
                       HttpResponseStatus.OK.getCode());
    DataSetManager<Table> outputmanager = getDataset(TrackerApp.DATA_DICTIONARY_DATASET_NAME);
    Row result = outputmanager.get().get(new Get(colNameSecond.toLowerCase()));
    Assert.assertEquals(dictionaryInput3.getColumnType(),
                        result.getString(DataDictionaryHandler.FieldNames.columnType.name()));
    outputmanager.flush();
  }

  @Test
  public void testGetAll() throws Exception {
    colName = "firstCol";
    colNameSecond = "secondCol";

    getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName, "POST", requestJson,
                       HttpResponseStatus.OK.getCode());
    getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colNameSecond, "POST", requestJson2,
                       HttpResponseStatus.OK.getCode());
    String response = getServiceResponse(dictionaryServiceManager, "v1/dictionary", "GET",
                                         HttpResponseStatus.OK.getCode());
    Type listType = new TypeToken<ArrayList<DictionaryResult>>() {
    }.getType();
    List<DictionaryResult> dictionaryResults = new Gson().fromJson(response, listType);
    Assert.assertNotNull(dictionaryResults);
    Assert.assertTrue(dictionaryResults.size() > 1);
  }

  @Test
  public void testUpdate() throws Exception {
    colName = "newColUpdate";

    getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName, "POST", requestJson,
                       HttpResponseStatus.OK.getCode());
    getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName, "PUT", requestJson3,
                       HttpResponseStatus.OK.getCode());

    DataSetManager<Table> outputmanager = getDataset(TrackerApp.DATA_DICTIONARY_DATASET_NAME);
    Row result = outputmanager.get().get(new Get(colName.toLowerCase()));
    String colType = result.getString(DataDictionaryHandler.FieldNames.columnType.name());
    String desc = result.getString(DataDictionaryHandler.FieldNames.description.name());
    Assert.assertEquals(desc, dictionaryInput3.getDescription());
    Assert.assertEquals(colType, dictionaryInput3.getColumnType());
    outputmanager.flush();
  }

  @Test
  public void testGetDictionaryFromSchema() throws Exception {
    getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + "col1",
                       "POST", requestJson, HttpResponseStatus.OK.getCode());
    getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + "col2",
                       "POST", requestJson2, HttpResponseStatus.OK.getCode());
    getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + "col3",
                       "POST", requestJson2, HttpResponseStatus.OK.getCode());
    List<String> inputColumns = new ArrayList<>();
    inputColumns.add("col1");
    inputColumns.add("col2");
    inputColumns.add("col4");

    String response = getServiceResponse(dictionaryServiceManager, "v1/dictionary", "POST", GSON.toJson(inputColumns),
                                         HttpResponseStatus.OK.getCode());
    Type hashMapType = new TypeToken<HashMap<String, List>>() {
    }.getType();
    HashMap result = GSON.fromJson(response, hashMapType);
    List<String> errors = (List<String>) result.get(DataDictionaryHandler.ERROR);
    List<DictionaryResult> dictionaryresults = (List<DictionaryResult>) result.get(DataDictionaryHandler.RESULTS);

    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(2, dictionaryresults.size());
    Assert.assertEquals(errors.get(0), "col4");
  }

  @Test
  public void testValidate() throws Exception {
    colName = "columnValidate1";
    colNameSecond = "columnValidate2";
    colNameThird = "wrongCol";
    getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName,
                       "POST", requestJson, HttpResponseStatus.OK.getCode());
    DictionaryResult dictionaryResult;

    // Assert response for wrong column name
    dictionaryResult = new DictionaryResult(colNameThird, "Float", false, false, null, null);
    String responseWithWrongCol = getServiceResponse(dictionaryServiceManager, "v1/dictionary/validate", "POST",
                                                     GSON.toJson(dictionaryResult),
                                                     HttpResponseStatus.NOT_FOUND.getCode());
    Type hashMapType = new TypeToken<HashMap<String, String>>() {
    }.getType();
    HashMap results = GSON.fromJson(responseWithWrongCol, hashMapType);
    Assert.assertEquals(2, results.size());

    // Assert values with wrong schema
    dictionaryResult = new DictionaryResult(colName, "Float", false, false, null, null);
    String responseWithErrors = getServiceResponse(dictionaryServiceManager, "v1/dictionary/validate", "POST",
                                                   GSON.toJson(dictionaryResult),
                                                   HttpResponseStatus.CONFLICT.getCode());
    Type linkedHashMapType = new TypeToken<LinkedHashMap<String, Object>>() {
    }.getType();
    HashMap result = GSON.fromJson(responseWithErrors, linkedHashMapType);
    List<String> reason = (List<String>) result.get("reason");

    Assert.assertEquals(1, reason.size());
    Assert.assertEquals(7, result.size());
    Assert.assertEquals(result.get("columnType"), "Float");
    Assert.assertEquals(result.get("expectedType"), "String");

    // Assert status code with correct schema
    dictionaryResult = new DictionaryResult(colName, "String", false, false, null, null);
    getServiceResponse(dictionaryServiceManager, "v1/dictionary/validate", "POST",
                       GSON.toJson(dictionaryResult), HttpResponseStatus.OK.getCode());
  }
}
