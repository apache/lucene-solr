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
package org.apache.solr.core.snapshots;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.noggit.JSONWriter;

/**
 * This class defines the meta-data about a collection level snapshot
 */
public class CollectionSnapshotMetaData implements JSONWriter.Writable {
  public static class CoreSnapshotMetaData implements JSONWriter.Writable {
    private final String coreName;
    private final String indexDirPath;
    private final long generationNumber;
    private final boolean leader;
    private final String shardId;
    private final Collection<String> files;

    public CoreSnapshotMetaData(String coreName, String indexDirPath, long generationNumber, String shardId, boolean leader, Collection<String> files) {
      this.coreName = coreName;
      this.indexDirPath = indexDirPath;
      this.generationNumber = generationNumber;
      this.shardId = shardId;
      this.leader = leader;
      this.files = files;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public CoreSnapshotMetaData(NamedList resp) {
      this.coreName = (String)resp.get(CoreAdminParams.CORE);
      this.indexDirPath = (String)resp.get(SolrSnapshotManager.INDEX_DIR_PATH);
      this.generationNumber = (Long)resp.get(SolrSnapshotManager.GENERATION_NUM);
      this.shardId = (String)resp.get(SolrSnapshotManager.SHARD_ID);
      this.leader = (Boolean)resp.get(SolrSnapshotManager.LEADER);
      this.files = (Collection<String>)resp.get(SolrSnapshotManager.FILE_LIST);
    }

    public String getCoreName() {
      return coreName;
    }

    public String getIndexDirPath() {
      return indexDirPath;
    }

    public long getGenerationNumber() {
      return generationNumber;
    }

    public Collection<String> getFiles() {
      return files;
    }

    public String getShardId() {
      return shardId;
    }

    public boolean isLeader() {
      return leader;
    }

    @Override
    public void write(JSONWriter arg0) {
      LinkedHashMap<String, Object> info = new LinkedHashMap<String, Object>();
      info.put(CoreAdminParams.CORE, getCoreName());
      info.put(SolrSnapshotManager.INDEX_DIR_PATH, getIndexDirPath());
      info.put(SolrSnapshotManager.GENERATION_NUM, getGenerationNumber());
      info.put(SolrSnapshotManager.SHARD_ID, getShardId());
      info.put(SolrSnapshotManager.LEADER, isLeader());
      info.put(SolrSnapshotManager.FILE_LIST, getFiles());
      arg0.write(info);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public NamedList toNamedList() {
      NamedList result = new NamedList();
      result.add(CoreAdminParams.CORE, getCoreName());
      result.add(SolrSnapshotManager.INDEX_DIR_PATH, getIndexDirPath());
      result.add(SolrSnapshotManager.GENERATION_NUM, getGenerationNumber());
      result.add(SolrSnapshotManager.SHARD_ID, getShardId());
      result.add(SolrSnapshotManager.LEADER, isLeader());
      result.add(SolrSnapshotManager.FILE_LIST, getFiles());
      return result;
    }
  }

  public static enum SnapshotStatus {
    Successful, InProgress, Failed;
  }

  private final String name;
  private final SnapshotStatus status;
  private final Date creationDate;
  private final List<CoreSnapshotMetaData> replicaSnapshots;

  public CollectionSnapshotMetaData(String name) {
    this(name, SnapshotStatus.InProgress, new Date(), Collections.<CoreSnapshotMetaData>emptyList());
  }

  public CollectionSnapshotMetaData(String name, SnapshotStatus status, Date creationTime, List<CoreSnapshotMetaData> replicaSnapshots) {
    this.name = name;
    this.status = status;
    this.creationDate = creationTime;
    this.replicaSnapshots = replicaSnapshots;
  }

  @SuppressWarnings("unchecked")
  public CollectionSnapshotMetaData(Map<String, Object> data) {
    this.name = (String)data.get(CoreAdminParams.NAME);
    this.status = SnapshotStatus.valueOf((String)data.get(SolrSnapshotManager.SNAPSHOT_STATUS));
    this.creationDate = new Date((Long)data.get(SolrSnapshotManager.CREATION_DATE));
    this.replicaSnapshots = new ArrayList<>();

    List<Object> r = (List<Object>) data.get(SolrSnapshotManager.SNAPSHOT_REPLICAS);
    for (Object x : r) {
      Map<String, Object> info = (Map<String, Object>)x;
      String coreName = (String)info.get(CoreAdminParams.CORE);
      String indexDirPath = (String)info.get(SolrSnapshotManager.INDEX_DIR_PATH);
      long generationNumber = (Long) info.get(SolrSnapshotManager.GENERATION_NUM);
      String shardId = (String)info.get(SolrSnapshotManager.SHARD_ID);
      boolean leader = (Boolean) info.get(SolrSnapshotManager.LEADER);
      Collection<String> files = (Collection<String>)info.get(SolrSnapshotManager.FILE_LIST);
      replicaSnapshots.add(new CoreSnapshotMetaData(coreName, indexDirPath, generationNumber, shardId, leader, files));
    }
  }

  @SuppressWarnings("unchecked")
  public CollectionSnapshotMetaData(NamedList<Object> data) {
    this.name = (String)data.get(CoreAdminParams.NAME);
    String statusStr = (String)data.get(SolrSnapshotManager.SNAPSHOT_STATUS);
    this.creationDate = new Date((Long)data.get(SolrSnapshotManager.CREATION_DATE));
    this.status = SnapshotStatus.valueOf(statusStr);
    this.replicaSnapshots = new ArrayList<>();

    NamedList<Object> r = (NamedList<Object>) data.get(SolrSnapshotManager.SNAPSHOT_REPLICAS);
    for (Map.Entry<String,Object> x : r) {
      NamedList<Object> info = (NamedList<Object>)x.getValue();
      String coreName = (String)info.get(CoreAdminParams.CORE);
      String indexDirPath = (String)info.get(SolrSnapshotManager.INDEX_DIR_PATH);
      long generationNumber = (Long) info.get(SolrSnapshotManager.GENERATION_NUM);
      String shardId = (String)info.get(SolrSnapshotManager.SHARD_ID);
      boolean leader = (Boolean) info.get(SolrSnapshotManager.LEADER);
      Collection<String> files = (Collection<String>)info.get(SolrSnapshotManager.FILE_LIST);
      replicaSnapshots.add(new CoreSnapshotMetaData(coreName, indexDirPath, generationNumber, shardId, leader, files));
    }
  }

  public String getName() {
    return name;
  }

  public SnapshotStatus getStatus() {
    return status;
  }

  public Date getCreationDate() {
    return creationDate;
  }

  public List<CoreSnapshotMetaData> getReplicaSnapshots() {
    return replicaSnapshots;
  }

  public List<CoreSnapshotMetaData> getReplicaSnapshotsForShard(String shardId) {
    List<CoreSnapshotMetaData> result = new ArrayList<>();
    for (CoreSnapshotMetaData d : replicaSnapshots) {
      if (d.getShardId().equals(shardId)) {
        result.add(d);
      }
    }
    return result;
  }

  public boolean isSnapshotExists(String shardId, Replica r) {
    for (CoreSnapshotMetaData d : replicaSnapshots) {
      if (d.getShardId().equals(shardId) && d.getCoreName().equals(r.getCoreName())) {
        return true;
      }
    }
    return false;
  }

  public Collection<String> getShards() {
    Set<String> result = new HashSet<>();
    for (CoreSnapshotMetaData d : replicaSnapshots) {
      result.add(d.getShardId());
    }
    return result;
  }

  @Override
  public void write(JSONWriter arg0) {
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    result.put(CoreAdminParams.NAME, this.name);
    result.put(SolrSnapshotManager.SNAPSHOT_STATUS, this.status.toString());
    result.put(SolrSnapshotManager.CREATION_DATE, this.getCreationDate().getTime());
    result.put(SolrSnapshotManager.SNAPSHOT_REPLICAS, this.replicaSnapshots);
    arg0.write(result);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public NamedList toNamedList() {
    NamedList result = new NamedList();
    result.add(CoreAdminParams.NAME, this.name);
    result.add(SolrSnapshotManager.SNAPSHOT_STATUS, this.status.toString());
    result.add(SolrSnapshotManager.CREATION_DATE, this.getCreationDate().getTime());

    NamedList replicas = new NamedList();
    for (CoreSnapshotMetaData x : replicaSnapshots) {
      replicas.add(x.getCoreName(), x.toNamedList());
    }
    result.add(SolrSnapshotManager.SNAPSHOT_REPLICAS, replicas);

    return result;
  }
}
