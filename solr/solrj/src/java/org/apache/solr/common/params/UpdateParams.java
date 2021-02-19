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

/**
 * A collection of standard params used by Update handlers
 *
 *
 * @since solr 1.2
 */
public interface UpdateParams
{

  /** Open up a new searcher as part of a commit */
  public static String OPEN_SEARCHER = "openSearcher";

  /** wait for the searcher to be registered/visible */
  public static String WAIT_SEARCHER = "waitSearcher";

  public static String SOFT_COMMIT = "softCommit";
  
  /** overwrite indexing fields */
  public static String OVERWRITE = "overwrite";
  
  /** Commit everything after the command completes */
  public static String COMMIT = "commit";

  /** Commit within a certain time period (in ms) */
  public static String COMMIT_WITHIN = "commitWithin";

  /** Optimize the index and commit everything after the command completes */
  public static String OPTIMIZE = "optimize";

  /** expert: calls IndexWriter.prepareCommit */
  public static String PREPARE_COMMIT = "prepareCommit";

  /** Rollback update commands */
  public static String ROLLBACK = "rollback";

  public static String COLLECTION = "collection";

  /** Select the update processor chain to use.  A RequestHandler may or may not respect this parameter */
  public static final String UPDATE_CHAIN = "update.chain";

  /** Override the content type used for UpdateLoader **/
  public static final String ASSUME_CONTENT_TYPE = "update.contentType";
  
  /**
   If optimizing, set the maximum number of segments left in the index after optimization.  Integer.MAX_INT is the default to respect maxMergeSegmentsMB
   */
  public static final String MAX_OPTIMIZE_SEGMENTS = "maxSegments";

  public static final String EXPUNGE_DELETES = "expungeDeletes";

  /** Return versions of updates? */
  public static final String VERSIONS = "versions";

  /**
   * If set to true, then Solr must fail to process any Atomic Update which can not 
   * be done "In-Place" with out re-indexing the entire document.
   */
  public static final String REQUIRE_PARTIAL_DOC_UPDATES_INPLACE = "update.partial.requireInPlace";
}
