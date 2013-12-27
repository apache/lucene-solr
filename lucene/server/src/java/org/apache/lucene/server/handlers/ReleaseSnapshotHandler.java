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

import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.Param; 
import org.apache.lucene.server.params.Request; 
import org.apache.lucene.server.params.StringType; 
import org.apache.lucene.server.params.StructType; 

/** Handles {@code releaseSnapshot}. */
public class ReleaseSnapshotHandler extends Handler {

  final static StructType TYPE = new StructType(  
                               new Param("indexName", "Index Name", new StringType()),
                               new Param("id", "The id for this snapshot; this must have been previously created via @createSnapshot.", new StringType()));

  @Override
  public String getTopDoc() {
    return "Releases a snapshot previously created with @createSnapshot.";
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  /** Sole constructor. */
  public ReleaseSnapshotHandler(GlobalState state) {
    super(state);
  }

  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {

    final IndexState.Gens gens = new IndexState.Gens(r, "id");

    return new FinishRequest() {
      @Override
      public String finish() throws IOException {

        // SearcherLifetimeManager pruning thread will drop
        // the searcher (if it's old enough) next time it
        // wakes up:
        state.snapshots.release(gens.indexGen);
        state.writer.getIndexWriter().deleteUnusedFiles();
        state.snapshotGenToVersion.remove(gens.indexGen);

        state.taxoSnapshots.release(gens.taxoGen);
        state.taxoInternalWriter.deleteUnusedFiles();
        state.decRef(gens.stateGen);

        return "{}";
      }
    };
  }
}