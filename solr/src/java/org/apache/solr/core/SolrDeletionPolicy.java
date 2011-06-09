package org.apache.solr.core;
/**
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

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.DateField;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;


/**
 * Standard Solr deletion policy that allows reserving index commit points
 * for certain amounts of time to support features such as index replication
 * or snapshooting directly out of a live index directory.
 *
 *
 * @see org.apache.lucene.index.IndexDeletionPolicy
 */
public class SolrDeletionPolicy implements IndexDeletionPolicy, NamedListInitializedPlugin {
  public static Logger log = LoggerFactory.getLogger(SolrCore.class);

  private String maxCommitAge = null;
  private int maxCommitsToKeep = 1;
  private int maxOptimizedCommitsToKeep = 0;

  public void init(NamedList args) {
    String keepOptimizedOnlyString = (String) args.get("keepOptimizedOnly");
    String maxCommitsToKeepString = (String) args.get("maxCommitsToKeep");
    String maxOptimizedCommitsToKeepString = (String) args.get("maxOptimizedCommitsToKeep");
    String maxCommitAgeString = (String) args.get("maxCommitAge");

    if (maxCommitsToKeepString != null && maxCommitsToKeepString.trim().length() > 0)
      maxCommitsToKeep = Integer.parseInt(maxCommitsToKeepString);
    if (maxCommitAgeString != null && maxCommitAgeString.trim().length() > 0)
      maxCommitAge = "-" + maxCommitAgeString;
    if (maxOptimizedCommitsToKeepString != null && maxOptimizedCommitsToKeepString.trim().length() > 0) {
      maxOptimizedCommitsToKeep = Integer.parseInt(maxOptimizedCommitsToKeepString);
    }
    
    // legacy support
    if (keepOptimizedOnlyString != null && keepOptimizedOnlyString.trim().length() > 0) {
      boolean keepOptimizedOnly = Boolean.parseBoolean(keepOptimizedOnlyString);
      if (keepOptimizedOnly) {
        maxOptimizedCommitsToKeep = Math.max(maxOptimizedCommitsToKeep, maxCommitsToKeep);
        maxCommitsToKeep=0;
      }
    }
  }

  static String str(IndexCommit commit) {
    StringBuilder sb = new StringBuilder();
    try {
      sb.append("commit{");

      Directory dir = commit.getDirectory();

      if (dir instanceof FSDirectory) {
        FSDirectory fsd = (FSDirectory) dir;
        sb.append("dir=").append(fsd.getDirectory());
      } else {
        sb.append("dir=").append(dir);
      }

      sb.append(",segFN=").append(commit.getSegmentsFileName());
      sb.append(",version=").append(commit.getVersion());
      sb.append(",generation=").append(commit.getGeneration());
      sb.append(",filenames=").append(commit.getFileNames());
    } catch (Exception e) {
      sb.append(e);
    }
    return sb.toString();
  }

  static String str(List commits) {
    StringBuilder sb = new StringBuilder();
    sb.append("num=").append(commits.size());

    for (IndexCommit commit : (List<IndexCommit>) commits) {
      sb.append("\n\t");
      sb.append(str(commit));
    }
    return sb.toString();
  }

  /**
   * Internal use for Lucene... do not explicitly call.
   */
  public void onInit(List commits) throws IOException {
    log.info("SolrDeletionPolicy.onInit: commits:" + str(commits));
    updateCommits((List<IndexCommit>) commits);
  }

  /**
   * Internal use for Lucene... do not explicitly call.
   */
  public void onCommit(List commits) throws IOException {
    log.info("SolrDeletionPolicy.onCommit: commits:" + str(commits));
    updateCommits((List<IndexCommit>) commits);
  }


  private void updateCommits(List<IndexCommit> commits) {
    // to be safe, we should only call delete on a commit point passed to us
    // in this specific call (may be across diff IndexWriter instances).
    // this will happen rarely, so just synchronize everything
    // for safety and to avoid race conditions

    synchronized (this) {
      long maxCommitAgeTimeStamp = -1L;
      IndexCommit newest = commits.get(commits.size() - 1);
      log.info("newest commit = " + newest.getVersion());

      int optimizedKept = newest.isOptimized() ? 1 : 0;
      int totalKept = 1;

      // work our way from newest to oldest, skipping the first since we always want to keep it.
      for (int i=commits.size()-2; i>=0; i--) {
        IndexCommit commit = commits.get(i);

        // delete anything too old, regardless of other policies
        try {
          if (maxCommitAge != null) {
            if (maxCommitAgeTimeStamp==-1) {
              DateMathParser dmp = new DateMathParser(DateField.UTC, Locale.US);
              maxCommitAgeTimeStamp = dmp.parseMath(maxCommitAge).getTime();
            }
            if (commit.getTimestamp() < maxCommitAgeTimeStamp) {
              commit.delete();
              continue;
            }
          }
        } catch (Exception e) {
          log.warn("Exception while checking commit point's age for deletion", e);
        }

        if (optimizedKept < maxOptimizedCommitsToKeep && commit.isOptimized()) {
          totalKept++;
          optimizedKept++;
          continue;
        }

        if (totalKept < maxCommitsToKeep) {
          totalKept++;
          continue;
        }
                                                  
        commit.delete();
      }

    } // end synchronized
  }

  private String getId(IndexCommit commit) {
    StringBuilder sb = new StringBuilder();
    Directory dir = commit.getDirectory();

    // For anything persistent, make something that will
    // be the same, regardless of the Directory instance.
    if (dir instanceof FSDirectory) {
      FSDirectory fsd = (FSDirectory) dir;
      File fdir = fsd.getDirectory();
      sb.append(fdir.getPath());
    } else {
      sb.append(dir);
    }

    sb.append('/');
    sb.append(commit.getGeneration());
    sb.append('_');
    sb.append(commit.getVersion());
    return sb.toString();
  }

  public String getMaxCommitAge() {
    return maxCommitAge;
  }

  public int getMaxCommitsToKeep() {
    return maxCommitsToKeep;
  }

  public int getMaxOptimizedCommitsToKeep() {
    return maxOptimizedCommitsToKeep;
  }

  public void setMaxCommitsToKeep(int maxCommitsToKeep) {
    synchronized (this) {
      this.maxCommitsToKeep = maxCommitsToKeep;
    }
  }

  public void setMaxOptimizedCommitsToKeep(int maxOptimizedCommitsToKeep) {
    synchronized (this) {
      this.maxOptimizedCommitsToKeep = maxOptimizedCommitsToKeep;
    }    
  }

}
