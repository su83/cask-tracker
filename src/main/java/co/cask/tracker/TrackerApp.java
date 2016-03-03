/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.tracker;

import co.cask.cdap.api.app.AbstractApplication;

/**
 * A CDAP Extension that provides the ability to track data ingested either through Cask Hydrator or Custom
 * CDAP Application and provide input to data governance process on cluster.
 */
public class TrackerApp extends AbstractApplication {
  public static final String AUDIT_LOG_DATASET_NAME = "AuditLog";


  @Override
  public void configure() {
    setName("Tracker");
    setDescription("A CDAP Extension that provides the ability to track data throughout the CDAP platform");
    //createDataset(AUDIT_LOG_DATASET_NAME, KeyValueTable.class);
    addService(new AuditLogService());
  }



}
