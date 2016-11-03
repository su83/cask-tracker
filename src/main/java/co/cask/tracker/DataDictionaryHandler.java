package co.cask.tracker;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.tracker.entity.DictionaryResult;
import co.cask.tracker.utils.ParameterCheck;
import com.google.gson.Gson;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Scope.row;

/**
 * This class handles requests to the DataDictionary API.
 */
public final class DataDictionaryHandler extends AbstractHttpServiceHandler {

  private String namespace;
  private Table dataDictionaryTable;
  private  byte[][] schema;
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(DataDictionaryHandler.class);

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    namespace = context.getNamespace();
    dataDictionaryTable = context.getDataset(TrackerApp.DATA_DICTIONARY_DATASET_NAME);
    schema = new byte[][]{Bytes.toBytes("columnName"), Bytes.toBytes("columnType"),
      Bytes.toBytes("isNullable"), Bytes.toBytes("isPII"), Bytes.toBytes("description"), Bytes.toBytes("datasets"),
      Bytes.toBytes("numberUsing")};
  }

  @Path("/v1/dictionary/{columnName}")
  @POST
  public void add(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("columnName") String columnName, @QueryParam("payload") String payload) {
    //TODO validate parameters
    //TODO find a way to populate datasets
    ByteBuffer requestContents = request.getContent();
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), "Request body is empty.");
      return;
    }
    byte[] rowField = Bytes.toBytes(columnName.toLowerCase());
    byte[] existing = dataDictionaryTable.get(rowField, Bytes.toBytes(columnName));
    if(existing != null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), String.format("%s already exists in data" +
                                                                                    " dictionary", columnName));
      return;
    }
    DictionaryResult colProperties = GSON.fromJson(payload, DictionaryResult.class);
    if(!ParameterCheck.isValidColumnType(colProperties.getColumnType())){
      String schemaTypes = Schema.Type.values().toString();
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), String.format("%s is not a vlid column type.",
                                                                                  columnName));
    }
    byte[][] row = new byte[][]{rowField, Bytes.toBytes(colProperties.getColumnType()),
      Bytes.toBytes(colProperties.isNullable()), Bytes.toBytes(colProperties.isPII()),
      Bytes.toBytes(colProperties.getDescription()), null, Bytes.toBytes(1)};

    dataDictionaryTable.put(Bytes.toBytes(columnName.toLowerCase()), schema, row);
    LOG.info("{} added to data dictionary", columnName);
    responder.sendStatus(HttpResponseStatus.CREATED.getCode());
  }
}
