package co.cask.tracker;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.tracker.entity.DictionaryResult;
import co.cask.tracker.utils.DiscoveryMetadataClient;
import co.cask.tracker.utils.ParameterCheck;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import static co.cask.tracker.utils.DiscoveryHelper.createMetadataClient;

/**
 * This class handles requests to the DataDictionary API.
 */
public final class DataDictionaryHandler extends AbstractHttpServiceHandler {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(DataDictionaryHandler.class);
  private Table dataDictionaryTable;
  private DiscoveryMetadataClient discoveryMetadataClient;
  private byte[][] schema;

  public static final String ENTITY_NAME = "entityName";
  public static final String TYPE = "type";
  public static final String IS_VALID = "isValid";
  public static final String DATA_SETS = "datasets";
  public static final String RESULTS = "Results";
  public static final String ERROR = "Error";

  @Property
  private String zookeeperQuorum;

  public DataDictionaryHandler(String zookeeperQuorum) {
    this.zookeeperQuorum = zookeeperQuorum;
  }

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    dataDictionaryTable = context.getDataset(TrackerApp.DATA_DICTIONARY_DATASET_NAME);
    schema = new byte[][]{Bytes.toBytes(FieldNames.columnName.name()), Bytes.toBytes(FieldNames.columnType.name()),
      Bytes.toBytes(FieldNames.isNullable.name()), Bytes.toBytes(FieldNames.isPII.name()),
      Bytes.toBytes(FieldNames.description.name()), Bytes.toBytes(FieldNames.datasets.name())};
  }

  @Path("/v1/dictionary/{columnName}")
  @POST
  public void add(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("columnName") String columnName) {
    ByteBuffer requestContents = request.getContent();

    // Send error if empty request is received.
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), "Request body is empty.");
      return;
    }

    // Send error if column already exists in dataset.
    byte[] rowField = Bytes.toBytes(columnName.toLowerCase());
    byte[] existing = dataDictionaryTable.get(rowField, Bytes.toBytes(FieldNames.columnName.name()));
    if (existing != null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), String.format("%s already exists in data" +
                                                                                    " dictionary", columnName));
      return;
    }

    // Send error if columnType is invalid.
    String payload = StandardCharsets.UTF_8.decode(requestContents).toString();
    DictionaryResult colProperties = GSON.fromJson(payload, DictionaryResult.class);
    if (!ParameterCheck.isValidColumnType(colProperties.getColumnType())) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), String.format("%s is not a valid column type.",
                                                                                  columnName));
      return;
    }

    if (colProperties.getDescription() == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), String.format("Description can not be null for %s",
                                                                                  columnName));
      return;
    }

    Boolean nullable = colProperties.isNullable() == null ? false : colProperties.isNullable();
    Boolean pii = colProperties.isPII() == null ? false : colProperties.isPII();

    HashMap<String, Object> columnDatasetMetadata = getDatasetMetadata(request, columnName,
                                                                       colProperties.getColumnType());
    if ("false".equalsIgnoreCase((String) columnDatasetMetadata.get(IS_VALID))) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(),
                          String.format("Column %s exists with different type", columnName));
      return;
    }
    if (columnDatasetMetadata.get(DATA_SETS) != null) {
      colProperties.setDatasets((List<String>) columnDatasetMetadata.get(DATA_SETS));
    }
    // Add row to table and send  200 response code.
    byte[][] row = new byte[][]{Bytes.toBytes(columnName), Bytes.toBytes(colProperties.getColumnType()),
      Bytes.toBytes(nullable), Bytes.toBytes(pii), Bytes.toBytes(colProperties.getDescription()),
      Bytes.toBytes(colProperties.getDatasets())};
    dataDictionaryTable.put(rowField, schema, row);
    LOG.info("{} added to data dictionary", columnName);
    responder.sendStatus(HttpResponseStatus.OK.getCode());
  }

  @Path("/v1/dictionary")
  @GET
  public void getFullDictionary(HttpServiceRequest request, HttpServiceResponder responder) {
    List<DictionaryResult> results = new ArrayList<>();
    Scanner scanner = dataDictionaryTable.scan(null, null);
    Row row;
    try {
      while ((row = scanner.next()) != null) {
        results.add(createDictionaryResultFromRow(row));
      }
      responder.sendJson(HttpResponseStatus.OK.getCode(), results);
    } finally {
      scanner.close();
    }
  }

  @PUT
  @Path("/v1/dictionary/{columnName}")
  public void update(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("columnName") String columnName) {

    ByteBuffer requestContents = request.getContent();

    // Send error if empty request is received.
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), "Request body is empty.");
      return;
    }

    // Send error if column does not already exist in dataset
    byte[] rowField = Bytes.toBytes(columnName.toLowerCase());
    Row existing = dataDictionaryTable.get(rowField);
    if (existing.isEmpty()) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.getCode(), String.format("%s is not present in data dictionary",
                                                                                columnName));
      return;
    }

    String payload = StandardCharsets.UTF_8.decode(requestContents).toString();
    DictionaryResult newColProperties = GSON.fromJson(payload, DictionaryResult.class);

    //Update the values of the field.
    Put put = new Put(rowField);
    if (newColProperties.getColumnType() != null) {
      // Send error if columnType is invalid.
      if (!ParameterCheck.isValidColumnType(newColProperties.getColumnType())) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), String.format("%s is not a valid column type.",
                                                                                    columnName));
        return;
      }
      put.add(FieldNames.columnType.name(), newColProperties.getColumnType());
    }
    if (newColProperties.getDescription() != null) {
      put.add(FieldNames.description.name(), newColProperties.getDescription());
    }
    if (newColProperties.isNullable() != null) {
      put.add(FieldNames.isNullable.name(), newColProperties.isNullable());
    }
    if (newColProperties.isPII() != null) {
      put.add(FieldNames.isPII.name(), newColProperties.isPII());
    }
    dataDictionaryTable.put(put);
    responder.sendStatus(HttpResponseStatus.OK.getCode());
  }

  @Path("/v1/dictionary/{columnName}")
  @DELETE
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("columnName") String columnName) {

    // Send error if column does not already exist in dataset
    byte[] rowField = Bytes.toBytes(columnName.toLowerCase());
    Row existing = dataDictionaryTable.get(rowField);
    if (existing.isEmpty()) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.getCode(), String.format("%s is not present in data dictionary",
                                                                                columnName));
      return;
    }
    dataDictionaryTable.delete(rowField);
    responder.sendStatus(HttpResponseStatus.OK.getCode());
  }

  @Path("/v1/dictionary/validate")
  @POST
  public void validate(HttpServiceRequest request, HttpServiceResponder responder) {

    ByteBuffer requestContents = request.getContent();

    // Send error if empty request is recieved
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), "Request body is empty.");
      return;
    }
    String payload = StandardCharsets.UTF_8.decode(requestContents).toString();
    DictionaryResult fromSchema = GSON.fromJson(payload, DictionaryResult.class);
    Row row = dataDictionaryTable.get(new Get(fromSchema.getColumnName().toLowerCase()));
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    if (row.isEmpty()) {
      result.put("columnName", fromSchema.getColumnName());
      result.put("reason", "The column does not exist in the data dictionary.");
      responder.sendJson(HttpResponseStatus.NOT_FOUND.getCode(), result);
      return;
    }
    DictionaryResult fromTable = createDictionaryResultFromRow(row);
    result = fromTable.validate(fromSchema);
    if (result.isEmpty()) {
      responder.sendStatus(HttpResponseStatus.OK.getCode());
    } else {
      responder.sendJson(HttpResponseStatus.CONFLICT.getCode(), result);
    }
  }

  @Path("/v1/dictionary")
  @POST
  public void getDictionaryForSchema(HttpServiceRequest request, HttpServiceResponder responder) {
    ByteBuffer requestContents = request.getContent();
    // Send error if empty request is received.
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), "Request body is empty.");
      return;
    }

    String payload = StandardCharsets.UTF_8.decode(requestContents).toString();
    Type listType = new TypeToken<List<String>>() {
    }.getType();
    List<String> columns = GSON.fromJson(payload, listType);
    HashMap<String, List> results = new HashMap<>();
    List<String> errors = new ArrayList<>();
    List<DictionaryResult> dictionaryResults = new ArrayList<>();
    Row row;
    for (String column : columns) {
      row = dataDictionaryTable.get(new Get(column.toLowerCase()));
      if (row.isEmpty()) {
        errors.add(column);
      } else {
        dictionaryResults.add(createDictionaryResultFromRow(row));
      }
      results.put(RESULTS, dictionaryResults);
      results.put(ERROR, errors);
      responder.sendJson(HttpResponseStatus.OK.getCode(), results);
    }
  }

  private DictionaryResult createDictionaryResultFromRow(Row row) {
    DictionaryResult result;
    String datasetList;
    String columnName = row.getString(FieldNames.columnName.name());
    String coulmnType = row.getString(FieldNames.columnType.name());
    Boolean isNullable = "true".equals(row.getString(FieldNames.isNullable.name()));
    Boolean isPII = "true".equals(row.getString(FieldNames.isPII.name()));
    List<String> datasets;
    if ((datasetList = row.getString(FieldNames.datasets.name())) == null) {
      datasets = new ArrayList<>();
    } else {
      datasets = Lists.newArrayList(Splitter.on(",").split(datasetList));
    }
    String description = row.getString(FieldNames.description.name());
    result = new DictionaryResult(columnName, coulmnType, isNullable, isPII, description, datasets);
    return result;
  }

  private HashMap<String, Object> getDatasetMetadata(HttpServiceRequest request, String column, String type) {
    NamespaceId namespaceId = new NamespaceId(getContext().getNamespace());
    List<HashMap<String, String>> searchRecords;
    HashMap<String, Object> results = new HashMap<>();
    List<String> entities = new ArrayList<>();
    results.put(IS_VALID, "true");
    try {
      searchRecords = getDiscoveryMetadataClient(request).getMetadataSearchRecords(namespaceId, column);
      for (HashMap<String, String> map : searchRecords) {
        if (type.equalsIgnoreCase(map.get(TYPE))) {
          entities.add(map.get(ENTITY_NAME));
        } else {
          results.put(IS_VALID, "false");
          return results;
        }
      }
    } catch (Exception e) {
      LOG.error("Unable to fetch dataset information for {}", column, e);
    }
    results.put(DATA_SETS, entities);
    return results;
  }

  private DiscoveryMetadataClient getDiscoveryMetadataClient(HttpServiceRequest request) throws UnauthorizedException {
    if (discoveryMetadataClient == null) {
      this.discoveryMetadataClient = createMetadataClient(request, zookeeperQuorum);
    }
    return this.discoveryMetadataClient;
  }

  /**
   * Enum to hold field names for Data dictionary table
   */
  public enum FieldNames {
    columnName,
    columnType,
    isNullable,
    isPII,
    datasets,
    description
  }
}
