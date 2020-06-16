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
package org.apache.solr.core;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Standard Solr deletion policy that allows reserving index commit points
 * for certain amounts of time to support features such as index replication
 * or snapshooting directly out of a live index directory.
 *
 *
 * @see org.apache.lucene.index.IndexDeletionPolicy
 */
public class SolrDeletionPolicy extends IndexDeletionPolicy implements NamedListInitializedPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String maxCommitAge = null;
  private int maxCommitsToKeep = 1;
  private int maxOptimizedCommitsToKeep = 0;

  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
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

  /**
   * Internal use for Lucene... do not explicitly call.
   */
  @Override
  public void onInit(List<? extends IndexCommit> commits) throws IOException {
    if (commits.isEmpty()) {
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("SolrDeletionPolicy.onInit: commits: {}", new CommitsLoggingDebug(commits));
    }
    updateCommits(commits);
  }

  /**
   * Internal use for Lucene... do not explicitly call.
   */
  @Override
  public void onCommit(List<? extends IndexCommit> commits) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("SolrDeletionPolicy.onCommit: commits: {}", new CommitsLoggingDebug(commits));
    }
    updateCommits(commits);
  }

  private static class CommitsLoggingInfo {
    private List<? extends IndexCommit> commits;

    public CommitsLoggingInfo(List<? extends IndexCommit> commits) {
      this.commits = commits;
    }

    public final String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("num=").append(commits.size());
      for (IndexCommit c : commits) {
        sb.append("\n\tcommit{");
        appendDetails(sb, c);
        sb.append("}");
      }
      // add an end brace
      return sb.toString();
    }

    protected void appendDetails(StringBuilder sb, IndexCommit c) {
      Directory dir = c.getDirectory();
      if (dir instanceof FSDirectory) {
        FSDirectory fsd = (FSDirectory) dir;
        sb.append("dir=").append(fsd.getDirectory());
      } else {
        sb.append("dir=").append(dir);
      }
      sb.append(",segFN=").append(c.getSegmentsFileName());
      sb.append(",generation=").append(c.getGeneration());
    }
  }

  private static class CommitsLoggingDebug extends CommitsLoggingInfo {
    public CommitsLoggingDebug(List<? extends IndexCommit> commits) {
      super(commits);
    }

    protected void appendDetails(StringBuilder sb, IndexCommit c) {
      super.appendDetails(sb, c);
      try {
        sb.append(",filenames=");
        sb.append(c.getFileNames());
      } catch (IOException e) {
        sb.append(e);
      }
    }
  }

  private void updateCommits(List<? extends IndexCommit> commits) {
    // to be safe, we should only call delete on a commit point passed to us
    // in this specific call (may be across diff IndexWriter instances).
    // this will happen rarely, so just synchronize everything
    // for safety and to avoid race conditions

    synchronized (this) {
      long maxCommitAgeTimeStamp = -1L;
      IndexCommit newest = commits.get(commits.size() - 1);
      if (log.isDebugEnabled()) {
        log.debug("newest commit generation = {}", newest.getGeneration());
      }
      int singleSegKept = (newest.getSegmentCount() == 1) ? 1 : 0;
      int totalKept = 1;

      // work our way from newest to oldest, skipping the first since we always want to keep it.
      for (int i=commits.size()-2; i>=0; i--) {
        IndexCommit commit = commits.get(i);

        // delete anything too old, regardless of other policies
        try {
          if (maxCommitAge != null) {
            if (maxCommitAgeTimeStamp==-1) {
              DateMathParser dmp = new DateMathParser(DateMathParser.UTC);
              maxCommitAgeTimeStamp = dmp.parseMath(maxCommitAge).getTime();
            }
            if (IndexDeletionPolicyWrapper.getCommitTimestamp(commit) < maxCommitAgeTimeStamp) {
              commit.delete();
              continue;
            }
          }
        } catch (Exception e) {
          log.warn("Exception while checking commit point's age for deletion", e);
        }

        if (singleSegKept < maxOptimizedCommitsToKeep && commit.getSegmentCount() == 1) {
          totalKept++;
          singleSegKept++;
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
      File fdir = fsd.getDirectory().toFile();
      sb.append(fdir.getPath());
    } else {
      sb.append(dir);
    }

    sb.append('/');
    sb.append(commit.getGeneration());
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
