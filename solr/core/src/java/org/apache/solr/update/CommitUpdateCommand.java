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
package org.apache.solr.update;

import org.apache.solr.request.SolrQueryRequest;

/**
 *
 */
public class CommitUpdateCommand extends UpdateCommand {
  public boolean optimize;
  public boolean openSearcher=true;     // open a new searcher as part of a hard commit
  public boolean waitSearcher=true;
  public boolean expungeDeletes = false;
  public boolean softCommit = false;
  public boolean prepareCommit = false;

  /**
   * During optimize, optimize down to &lt;= this many segments.  Must be &gt;= 1
   *
   * @see org.apache.lucene.index.IndexWriter#forceMerge(int)
   */
  public int maxOptimizeSegments = Integer.MAX_VALUE; // So we respect MaxMergeSegmentsMB by default

  public CommitUpdateCommand(SolrQueryRequest req, boolean optimize) {
    super(req);
    this.optimize=optimize;
  }

  @Override
  public String name() {
    return "commit";
  }

  @Override
  public String toString() {
    return super.toString() + ",optimize="+optimize
            +",openSearcher="+openSearcher
            +",waitSearcher="+waitSearcher
            +",expungeDeletes="+expungeDeletes
            +",softCommit="+softCommit
            +",prepareCommit="+prepareCommit
            +'}';
  }
}
