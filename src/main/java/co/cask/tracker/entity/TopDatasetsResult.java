/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tracker.entity;

/**
 * A POJO to hold the results for the TopN query.
 */
public class TopDatasetsResult implements Comparable<TopDatasetsResult> {
  private final String entityName;
  private final String entityType;
  private long read;
  private long write;

  public  TopDatasetsResult(String entityName, String entityType) {
    this.entityName = entityName;
    this.entityType = entityType;
    this.read = 0L;
    this.write = 0L;
  }

  public long getRead() {
    return read;
  }

  public long getWrite() {
    return write;
  }

  public void setRead(long read) {
    this.read = read;
  }

  public void setWrite(long write) {
    this.write = write;
  }

  @Override
  public int compareTo(TopDatasetsResult o) {
    return Long.compare(o.getRead() + o.getWrite(), getRead() + getWrite());
  }
}
