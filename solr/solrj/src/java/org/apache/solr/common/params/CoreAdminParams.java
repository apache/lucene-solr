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
package org.apache.solr.common.params;

import java.util.Locale;

import org.apache.solr.common.SolrException;

/**
 * @since solr 1.3
 */
public abstract class CoreAdminParams
{
  /** What Core are we talking about **/
  public final static String CORE = "core";

  /** Should the STATUS request include index info **/
  public final static String INDEX_INFO = "indexInfo";
  
  /** If you rename something, what is the new name **/
  public final static String NAME = "name";

  /** Core data directory **/
  public final static String DATA_DIR = "dataDir";

  /** Core updatelog directory **/
  public final static String ULOG_DIR = "ulogDir";

  /** Name of the other core in actions involving 2 cores **/
  public final static String OTHER = "other";

  /** What action **/
  public final static String ACTION = "action";
  
  /** If you specify a schema, what is its name **/
  public final static String SCHEMA = "schema";

  /** If you specify a configset, what is its name **/
  public final static String CONFIGSET = "configSet";
  
  /** If you specify a config, what is its name **/
  public final static String CONFIG = "config";
  
  /** Specifies a core instance dir. */
  public final static String INSTANCE_DIR = "instanceDir";

  /** If you specify a file, what is its name **/
  public final static String FILE = "file";
  
  /** If you merge indexes, what are the index directories.
   * The directories are specified by multiple indexDir parameters. */
  public final static String INDEX_DIR = "indexDir";

  /** If you merge indexes, what is the source core's name
   * More than one source core can be specified by multiple srcCore parameters */
  public final static String SRC_CORE = "srcCore";

  /** The collection name in solr cloud */
  public final static String COLLECTION = "collection";

  /** The replica name in solr cloud */
  public final static String REPLICA = "replica";
  
  /** The shard id in solr cloud */
  public final static String SHARD = "shard";

  /** The target core to which a split index should be written to
   * Multiple targetCores can be specified by multiple targetCore parameters */
  public final static String TARGET_CORE = "targetCore";

  /** The hash ranges to be used to split a shard or an index */
  public final static String RANGES = "ranges";

  public final static String GET_RANGES = "getRanges";

  public static final String ROLES = "roles";

  public static final String REQUESTID = "requestid";

  public static final String CORE_NODE_NAME = "coreNodeName";
  
  /** Prefix for core property name=value pair **/
  public final static String PROPERTY_PREFIX = "property.";

  /** If you unload a core, delete the index too */
  public final static String DELETE_INDEX = "deleteIndex";

  public static final String DELETE_DATA_DIR = "deleteDataDir";

  public static final String DELETE_INSTANCE_DIR = "deleteInstanceDir";

  public static final String DELETE_METRICS_HISTORY = "deleteMetricsHistory";

  public static final String LOAD_ON_STARTUP = "loadOnStartup";
  
  public static final String TRANSIENT = "transient";

  // Node to create a replica on for ADDREPLICA at least.
  public static final String NODE = "node";

  /**
   * A parameter to specify the name of the backup repository to be used.
   */
  public static final String BACKUP_REPOSITORY = "repository";

  /**
   * A parameter to specify the location where the backup should be stored.
   */
  public static final String BACKUP_LOCATION = "location";

  /**
   * The ID of the shard-backup prior to the latest one (indicated by {@link #SHARD_BACKUP_ID}
   */
  public static final String PREV_SHARD_BACKUP_ID = "prevShardBackupId";

  /**
   * The ID of the shard-backup in question
   */
  public static final String SHARD_BACKUP_ID = "shardBackupId";

  /**
   * A parameter to specify last number of backups (delete the rest)
   */
  public static final String MAX_NUM_BACKUP_POINTS = "maxNumBackupPoints";

  /**
   * Unique id of the backup
   */
  public static final String BACKUP_ID = "backupId";

  /**
   * Purging/deleting all index files, shard-backup metadata files, backup property files that are unreachable, uncompleted or corrupted.
   */
  public static final String BACKUP_PURGE_UNUSED = "purgeUnused";

  /**
   * A parameter to specify whether incremental backup is used
   */
  public static final String BACKUP_INCREMENTAL = "incremental";

  /**
   * A parameter to specify the name of the commit to be stored during the backup operation.
   */
  public static final String COMMIT_NAME = "commitName";

  /**
   * A boolean parameter specifying if a core is being created as part of a new collection
   */
  public static final String NEW_COLLECTION = "newCollection";

  /**
   * Tells the CoreAdminHandler that the new Core will be a replica of a particular {@link org.apache.solr.common.cloud.Replica.Type}
   */
  public static final String REPLICA_TYPE = "replicaType";

  public enum CoreAdminAction {
    STATUS(true),
    UNLOAD,
    RELOAD,
    CREATE,
    SWAP,
    RENAME,
    MERGEINDEXES,
    SPLIT,
    PREPRECOVERY,
    REQUESTRECOVERY,
    REQUESTSYNCSHARD,
    DELETEALIAS,
    REQUESTBUFFERUPDATES,
    REQUESTAPPLYUPDATES,
    OVERSEEROP,
    REQUESTSTATUS(true),
    REJOINLEADERELECTION,
    //internal API used by force shard leader election
    FORCEPREPAREFORLEADERSHIP,
    INVOKE,
    //Internal APIs to backup and restore a core
    BACKUPCORE,
    RESTORECORE,
    CREATESNAPSHOT,
    DELETESNAPSHOT,
    LISTSNAPSHOTS;

    public final boolean isRead;

    CoreAdminAction(boolean isRead) {
      this.isRead = isRead;
    }

    CoreAdminAction() {
      this.isRead = false;
    }

    public static CoreAdminAction get( String p ) {
      if (p != null) {
        try {
          return CoreAdminAction.valueOf(p.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Wrong core admin action");
        }
      }
      return null; 
    }
  }
}
