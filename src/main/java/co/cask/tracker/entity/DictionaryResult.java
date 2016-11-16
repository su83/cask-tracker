package co.cask.tracker.entity;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Entity class to hold results for Data dictionary
 */
public class DictionaryResult {

  private String columnName;
  private String columnType;
  private Boolean isNullable;
  private Boolean isPII;
  private String description;
  private List<String> datasets;

  public DictionaryResult(String columnName, String columnType, Boolean isNullable, Boolean isPII,
                          @Nullable String description, List<String> datasets) {
    this.columnName = columnName;
    this.columnType = columnType;
    this.isNullable = isNullable;
    this.isPII = isPII;
    this.description = description;
    this.datasets = datasets;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getColumnType() {
    return columnType;
  }

  public Boolean isNullable() {
    return isNullable;
  }

  public Boolean isPII() {
    return isPII;
  }

  public String getDescription() {
    return description;
  }

  public String getDatasets() {
    return (datasets == null || datasets.isEmpty()) ? "" : Joiner.on(",").join(datasets);
  }

  public void setDatasets(List<String> datasets) {
    this.datasets = datasets;
  }

  public LinkedHashMap<String, Object> validate(DictionaryResult other) {
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    List<String> reason = new ArrayList<>();
    if (!this.columnName.equals(other.columnName)) {
      reason.add("The column case did not match the data dictionary.");
    }
    if (!this.columnType.equalsIgnoreCase(other.columnType)) {
      reason.add("The column type did not match the data dictionary.");
    }
    if (!this.isNullable().equals(other.isNullable)) {
      reason.add("IsNullable value did not match the data dictionary.");
    }
    if (!reason.isEmpty()) {
      result.put("columnName", other.columnName);
      result.put("expectedName", this.columnName);
      result.put("isNullable", other.isNullable);
      result.put("expectedNullable", this.isNullable);
      result.put("columnType", other.columnType);
      result.put("expectedType", this.columnType);
      result.put("reason", reason);
    }
    return result;
  }

}
