package co.cask.tracker;

import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.tracker.entity.AuditLogEntry;
import co.cask.tracker.entity.AuditLogResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Created by russellsavage on 3/2/16.
 */
public final class AuditLogHandler extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AuditLogHandler.class);
  private static final int DEFAULT_PAGE_SIZE = 10;

  @Path("auditlog")
  @GET
  public void query(HttpServiceRequest request, HttpServiceResponder responder,
                    @QueryParam("offset") int offset,
                    @QueryParam("pageSize") int pageSize,
                    @QueryParam("startTime") String startTime,
                    @QueryParam("endTime") String endTime) {
    if(pageSize == 0) {
      pageSize = DEFAULT_PAGE_SIZE;
    }
    long startTimeLong = TimeMathParser.nowInSeconds()-86400;
    if(startTime != null) {
      try {
        startTimeLong = TimeMathParser.parseTimeInSeconds(startTime);
      } catch(IllegalArgumentException e) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "startTime was not in the correct format.");
        return;
      }
    }
    long endTimeLong = TimeMathParser.nowInSeconds();
    if(endTime != null) {
      try {
        endTimeLong = TimeMathParser.parseTimeInSeconds(endTime);
      } catch(IllegalArgumentException e) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST.getCode(), "endTime was not in the correct format.");
        return;
      }
    }

    List<AuditLogEntry> logList = new ArrayList<>();
    for(int i = offset; i < (offset+pageSize); i++) {
      logList.add(new AuditLogEntry(startTimeLong+i, "user"+i, "action"+i, "kind"+i, "name"+i, "metadata"+i));
    }
    AuditLogResponse resp = new AuditLogResponse(logList.size()*427, logList, offset);
    responder.sendJson(200,resp);
  }
}

