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
 * @version $Id$
 * @see org.apache.lucene.index.IndexDeletionPolicy
 */
public class SolrDeletionPolicy implements IndexDeletionPolicy, NamedListInitializedPlugin {
  public static Logger log = LoggerFactory.getLogger(SolrCore.class);

  private boolean keepOptimizedOnly = false;
  private String maxCommitAge = null;
  private int maxCommitsToKeep = 1;

  public void init(NamedList args) {
    String keepOptimizedOnlyString = (String) args.get("keepOptimizedOnly");
    String maxCommitsToKeepString = (String) args.get("maxCommitsToKeep");
    String maxCommitAgeString = (String) args.get("maxCommitAge");
    if (keepOptimizedOnlyString != null && keepOptimizedOnlyString.trim().length() > 0)
      keepOptimizedOnly = Boolean.parseBoolean(keepOptimizedOnlyString);
    if (maxCommitsToKeepString != null && maxCommitsToKeepString.trim().length() > 0)
      maxCommitsToKeep = Integer.parseInt(maxCommitsToKeepString);
    if (maxCommitAgeString != null && maxCommitAgeString.trim().length() > 0)
      maxCommitAge = "-" + maxCommitAgeString;
  }

  static String str(IndexCommit commit) {
    StringBuilder sb = new StringBuilder();
    try {
      sb.append("commit{");

      Directory dir = commit.getDirectory();

      if (dir instanceof FSDirectory) {
        FSDirectory fsd = (FSDirectory) dir;
        sb.append("dir=").append(fsd.getFile());
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
    IndexCommit comm = (IndexCommit) commits.get(0);
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
    DateMathParser dmp = new DateMathParser(DateField.UTC, Locale.US);

    synchronized (this) {
      IndexCommit last = commits.get(commits.size() - 1);
      log.info("last commit = " + last.getVersion());

      int numCommitsToDelete = commits.size() - maxCommitsToKeep;
      int i = 0;
      for (IndexCommit commit : commits) {
        // don't delete the last commit point
        if (commit == last) {
          continue;
        }

        if (i < numCommitsToDelete) {
          commit.delete();
          i++;
          continue;
        }

        try {
          if (maxCommitAge != null)
            if (commit.getTimestamp() < dmp.parseMath(maxCommitAge).getTime()) {
              commit.delete();
              continue;
            }
        } catch (Exception e) {
          log.warn("Exception while checking commit point's age for deletion", e);
        }

        if (keepOptimizedOnly) {
          if (!commit.isOptimized()) {
            commit.delete();
            log.info("Marking unoptimized index " + getId(commit) + " for deletion.");
          }
        }
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
      File fdir = fsd.getFile();
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

  public boolean isKeepOptimizedOnly() {
    return keepOptimizedOnly;
  }

  public String getMaxCommitAge() {
    return maxCommitAge;
  }

  public int getMaxCommitsToKeep() {
    return maxCommitsToKeep;
  }
}
