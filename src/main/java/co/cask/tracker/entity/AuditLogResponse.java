package co.cask.tracker.entity;

import java.util.List;

/**
 * Created by russellsavage on 3/2/16.
 */
public class AuditLogResponse {
  private int numberOfResults;
  private List<AuditLogEntry> results;
  private int offset;


  public AuditLogResponse(int numberOfResults, List<AuditLogEntry> results, int offset) {
    this.numberOfResults = numberOfResults;
    this.results = results;
    this.offset = offset;
  }

  public int getNumberOfResults() {
    return numberOfResults;
  }

  public void setNumberOfResults(int numberOfResults) {
    this.numberOfResults = numberOfResults;
  }

  public List<AuditLogEntry> getResults() {
    return results;
  }

  public void setResults(List<AuditLogEntry> results) {
    this.results = results;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }
}
