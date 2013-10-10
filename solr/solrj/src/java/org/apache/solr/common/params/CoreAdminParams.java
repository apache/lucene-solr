/**
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

/**
 * @since solr 1.3
 */
public abstract class CoreAdminParams
{
  /** What Core are we talking about **/
  public final static String CORE = "core";

  /** Should the STATUS request include index info **/
  public final static String INDEX_INFO = "indexInfo";

  /** Persistent -- should it save the cores state? **/
  public final static String PERSISTENT = "persistent";
  
  /** If you rename something, what is the new name **/
  public final static String NAME = "name";

  /** If you rename something, what is the new name **/
  public final static String DATA_DIR = "dataDir";
  
  public final static String ULOG_DIR = "ulogDir";

  /** Name of the other core in actions involving 2 cores **/
  public final static String OTHER = "other";

  /** What action **/
  public final static String ACTION = "action";
  
  /** If you specify a schema, what is its name **/
  public final static String SCHEMA = "schema";
  
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

  /** The shard id in solr cloud */
  public final static String SHARD = "shard";
  
  /** The shard range in solr cloud */
  public final static String SHARD_RANGE = "shard.range";

  /** The shard range in solr cloud */
  public final static String SHARD_STATE = "shard.state";

  /** The parent shard if applicable */
  public final static String SHARD_PARENT = "shard.parent";

  /** The target core to which a split index should be written to
   * Multiple targetCores can be specified by multiple targetCore parameters */
  public final static String TARGET_CORE = "targetCore";

  /** The hash ranges to be used to split a shard or an index */
  public final static String RANGES = "ranges";
  
  public static final String ROLES = "roles";
  
  public static final String CORE_NODE_NAME = "coreNodeName";
  
  /** Prefix for core property name=value pair **/
  public final static String PROPERTY_PREFIX = "property.";

  /** If you unload a core, delete the index too */
  public final static String DELETE_INDEX = "deleteIndex";

  public static final String DELETE_DATA_DIR = "deleteDataDir";

  public static final String DELETE_INSTANCE_DIR = "deleteInstanceDir";

  public static final String LOAD_ON_STARTUP = "loadOnStartup";
  
  public static final String TRANSIENT = "transient";

  public enum CoreAdminAction {
    STATUS,  
    LOAD,
    UNLOAD,
    RELOAD,
    CREATE,
    PERSIST,
    SWAP,
    RENAME,
    MERGEINDEXES,
    SPLIT,
    PREPRECOVERY,
    REQUESTRECOVERY, 
    REQUESTSYNCSHARD,
    CREATEALIAS,
    DELETEALIAS,
    REQUESTAPPLYUPDATES,
    LOAD_ON_STARTUP,
    TRANSIENT;
    
    public static CoreAdminAction get( String p )
    {
      if( p != null ) {
        try {
          return CoreAdminAction.valueOf( p.toUpperCase(Locale.ROOT) );
        }
        catch( Exception ex ) {}
      }
      return null; 
    }
  }
}
