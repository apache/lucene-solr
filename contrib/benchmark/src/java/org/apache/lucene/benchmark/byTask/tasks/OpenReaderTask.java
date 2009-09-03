package org.apache.lucene.benchmark.byTask.tasks;

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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

/**
 * Open an index reader.
 * <br>Other side effects: index reader object in perfRunData is set.
 * <br> Optional params readOnly,commitUserData eg. OpenReader(false,commit1)
 */
public class OpenReaderTask extends PerfTask {
  public static final String USER_DATA = "userData";
  private boolean readOnly = true;
  private String commitUserData = null;

  public OpenReaderTask(PerfRunData runData) {
    super(runData);
  }

  public int doLogic() throws IOException {
    Directory dir = getRunData().getDirectory();
    Config config = getRunData().getConfig();
    IndexReader r = null;
    if (commitUserData != null) {
      r = openCommitPoint(commitUserData, dir, config, readOnly);
    } else {
      IndexDeletionPolicy indexDeletionPolicy = CreateIndexTask.getIndexDeletionPolicy(config);
      r = IndexReader.open(dir, indexDeletionPolicy, readOnly); 
    }
    getRunData().setIndexReader(r);
    return 1;
  }
 
  public static IndexReader openCommitPoint(String userData, Directory dir, Config config, boolean readOnly) throws IOException {
    IndexReader r = null;
    Collection commits = IndexReader.listCommits(dir);
    Iterator i = commits.iterator();
    while (i.hasNext()) {
      IndexCommit ic = (IndexCommit)i.next();
      Map map = ic.getUserData();
      String ud = null;
      if (map != null) {
        ud = (String)map.get(USER_DATA);
      }
      if (ud != null && ud.equals(userData)) {
        IndexDeletionPolicy indexDeletionPolicy = CreateIndexTask.getIndexDeletionPolicy(config);
        r = IndexReader.open(ic, indexDeletionPolicy, readOnly);
        break;
      }
    }
    if (r == null) throw new IOException("cannot find commitPoint userData:"+userData);
    return r;
  }
  
  public void setParams(String params) {
    super.setParams(params);
    if (params != null) {
      String[] split = params.split(",");
      if (split.length > 0) {
        readOnly = Boolean.valueOf(split[0]).booleanValue();
      }
      if (split.length > 1) {
        commitUserData = split[1];
      }
    }
  }

  public boolean supportsParams() {
    return true;
  }
}
