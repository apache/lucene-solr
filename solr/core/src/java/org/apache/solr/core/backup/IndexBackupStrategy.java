/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.core.backup;

import java.net.URI;

/**
 * This interface defines the strategy used to backup the Solr collection index data.
 */
public interface IndexBackupStrategy {
  /**
   * Backups the index data for specified Solr collection at the given location.
   *
   * @param basePath The base location where the backup needs to be stored.
   * @param collectionName The name of the collection which needs to be backed up.
   * @param backupName The unique name of the backup.
   */
  public void createBackup(URI basePath, String collectionName, String backupName);
}
