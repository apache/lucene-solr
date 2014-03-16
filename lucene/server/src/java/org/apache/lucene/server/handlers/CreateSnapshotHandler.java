package org.apache.lucene.server.handlers;

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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.MyIndexSearcher;
import org.apache.lucene.server.params.BooleanType; 
import org.apache.lucene.server.params.Param; 
import org.apache.lucene.server.params.Request; 
import org.apache.lucene.server.params.StringType; 
import org.apache.lucene.server.params.StructType; 
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/** Handles {@code createSnapshot}. */
public class CreateSnapshotHandler extends Handler {

  final static StructType TYPE = new StructType(
                               new Param("indexName", "Index Name", new StringType()),
                               new Param("openSearcher", "Pass true if you intend to do searches against this snapshot, by passing searcher: {snapshot: X} to @search", new BooleanType(), false));
                                          
  @Override
  public String getTopDoc() {
    return "Creates a snapshot in the index, which is saved point-in-time view of the last commit in the index such that no files referenced by that snapshot will be deleted by ongoing indexing until the snapshot is released with @releaseSnapshot.  Note that this will reference the last commit, so be sure to call commit first if you have pending changes that you'd like to be included in the snapshot.<p>This can be used for backup purposes, i.e. after creating the snapshot you can copy all referenced files to backup storage, and then release the snapshot once complete.  To restore the backup, just copy all the files back and restart the server.  It can also be used for transactional purposes, i.e. if you sometimes need to search a specific snapshot instead of the current live index.<p>Creating a snapshot is very fast (does not require any file copying), but over time it will consume extra disk space as old segments are merged in the index.  Be sure to release the snapshot once you're done.  Snapshots survive shutdown and restart of the server.  Returns all protected filenames referenced by this snapshot: these files will not change and will not be deleted until the snapshot is released.  This returns the directories and files referenced by the snapshot.";
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  /** Sole constructor. */
  public CreateSnapshotHandler(GlobalState state) {
    super(state);
  }

  static void fillFiles(JSONObject o, String path, IndexCommit commit) throws IOException {
    JSONArray arr = new JSONArray();
    for(String sub : commit.getFileNames()) {
      arr.add(sub);
    }
    o.put(path, arr);
  }

  @Override
  public FinishRequest handle(final IndexState state, Request r, Map<String,List<String>> params) throws Exception {
    state.verifyStarted(r);

    if (!state.hasCommit()) {
      r.fail("this index has no commits; please call commit first");
    }

    // nocommit not thread safe vs commitHandler?
    final boolean openSearcher = r.getBoolean("openSearcher");
    
    return new FinishRequest() {
      @Override
      public String finish() throws IOException {
        IndexCommit c = state.snapshots.snapshot();
        IndexCommit tc = state.taxoSnapshots.snapshot();
        long stateGen = state.incRefLastCommitGen();

        JSONObject result = new JSONObject();
        
        SegmentInfos sis = new SegmentInfos();
        sis.read(state.origIndexDir, c.getSegmentsFileName());
        state.snapshotGenToVersion.put(c.getGeneration(), sis.getVersion());

        if (openSearcher) {
          // nocommit share w/ SearchHandler's method:
          // TODO: this "reverse-NRT" is silly ... we need a reader
          // pool somehow:
          SearcherAndTaxonomy s2 = state.manager.acquire();
          try {
            // This returns a new reference to us, which
            // is decRef'd in the finally clause after
            // search is done:
            long t0 = System.nanoTime();
            IndexReader r = DirectoryReader.openIfChanged((DirectoryReader) s2.searcher.getIndexReader(), c);
            IndexSearcher s = new MyIndexSearcher(r, state);
            try {
              state.slm.record(s);
            } finally {
              s.getIndexReader().decRef();
            }
            long t1 = System.nanoTime();
            result.put("newSnapshotSearcherOpenMS", ((t1-t0)/1000000.0));
          } finally {
            state.manager.release(s2);
          }
        }

        // TODO: suggest state?

        // nocommit must also snapshot snapshots state!?
        // hard to think about

        fillFiles(result, "index", c);
        fillFiles(result, "taxonomy", tc);
        JSONArray arr = new JSONArray();
        arr.add("state." + stateGen);
        result.put("state", arr);
        result.put("id", c.getGeneration() + ":" + tc.getGeneration() + ":" + stateGen);

        return result.toString();
      }
    };
  }
}
