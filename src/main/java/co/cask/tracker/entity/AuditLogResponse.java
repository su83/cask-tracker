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

import co.cask.cdap.proto.audit.AuditMessage;

import java.util.List;

/**
 * A class to represent the JSON response of the AuditLog API response.
 */
public class AuditLogResponse {
  private final int totalResults;
  private final List<AuditMessage> results;
  private final int offset;

  public AuditLogResponse(int numberOfResults, List<AuditMessage> results, int offset) {
    this.totalResults = numberOfResults;
    this.results = results;
    this.offset = offset;
  }

  public int getTotalResults() {
    return totalResults;
  }

  public List<AuditMessage> getResults() {
    return results;
  }

  public int getOffset() {
    return offset;
  }
}
