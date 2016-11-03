package co.cask.tracker.entity;

import co.cask.cdap.api.data.schema.Schema;
import org.apache.commons.lang.enums.EnumUtils;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Created by Abhinav on 11/3/16.
 */
public class DictionaryResult {

  private String columnName;
  private final String columnType;
  private final Boolean isNullable;
  private final Boolean isPII;
  private final String description;
  private List<String> datasets;
  private int numberUsing;

  public DictionaryResult(String columnName, String columnType, Boolean isNullable, Boolean isPII,
                          @Nullable String description, List<String> datasets, int numberUsing) {
    this.columnName = columnName;
    this.columnType = columnType;
    this.isNullable = isNullable;
    this.isPII = isPII;
    this.description = description;
    this.datasets = datasets;
    this.numberUsing = numberUsing;
  }

  public DictionaryResult(String columnType, Boolean isNullable, Boolean isPII, String description) {
    this.columnType = columnType;
    this.isNullable = isNullable;
    this.isPII = isPII;
    this.description = description;
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

  public List<String> getDatasets() {
    return datasets;
  }

  public int getNumberUsing() {
    return numberUsing;
  }

}
