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

import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Collections;

import org.apache.solr.cloud.ShardRequestProcessor;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A concrete implementation of {@linkplain IndexBackupStrategy} which copies files associated
 * with the index commit to the specified location.
 */
public class CopyFilesStrategy implements IndexBackupStrategy {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ShardRequestProcessor processor;

  public CopyFilesStrategy(ShardRequestProcessor processor) {
    this.processor = processor;
  }

  @Override
  public void createBackup(URI basePath, String collectionName, String backupName) {
    ZkStateReader zkStateReader = processor.getZkStateReader();

    for (Slice slice : zkStateReader.getClusterState().getCollection(collectionName).getActiveSlices()) {
      Replica replica = slice.getLeader();
      String coreName = replica.getStr(CORE_NAME_PROP);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.BACKUPCORE.toString());
      params.set(NAME, slice.getName());
      // TODO - Fix the core level BACKUP operation to accept a URI so that it can also work with other file-systems.
      params.set("location", basePath.getPath()); // note: index dir will be here then the "snapshot." + slice name
      params.set(CORE_NAME_PROP, coreName);

      processor.sendShardRequest(replica.getNodeName(), params);
      log.debug("Sent backup request to core={} for backupname={}", coreName, backupName);
    }

    processor.processResponses( true, "Could not backup all replicas", Collections.emptySet());
  }
}
