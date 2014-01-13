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

import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.server.DirectoryFactory;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.*;
import org.apache.lucene.server.params.PolyType.PolyEntry;
import org.apache.lucene.server.params.Request.PolyResult;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.packed.PackedInts;
import net.minidev.json.JSONValue;

/** For changing index settings that cannot be changed while
 *  the index is running. */
public class SettingsHandler extends Handler {

  // TODO: merge scheduler, CMS max threads, etc.

  // TODO: add "includeDefaults" bool ... if true then we
  // return ALL settings (incl default ones)

  /** Parameters accepted by this handler. */
  public static final StructType TYPE =
    new StructType(
        new Param("indexName", "Index name", new StringType()),
        RegisterFieldHandler.MATCH_VERSION_PARAM,
        new Param("nrtCachingDirectory.maxMergeSizeMB", "Largest merged segment size to cache in RAMDirectory", new FloatType(), 5.0),
        new Param("nrtCachingDirectory.maxSizeMB", "Largest overall size for all files cached in NRTCachingDirectory; set to 0 to disable NRTCachingDirectory", new FloatType(), 60.0),
        new Param("concurrentMergeScheduler.maxThreadCount", "How many merge threads to allow at once", new IntType(), 1),
        new Param("concurrentMergeScheduler.maxMergeCount", "Maximum backlog of pending merges before indexing threads are stalled", new IntType(), 2),

        new Param("index.verbose", "Turn on IndexWriter's infoStream (to stdout)", new BooleanType(), false),
        // nocommit how to accept any class on the CP that
        // implements NormsFormat and has default ctor ...?
        new Param("normsFormat", "Which NormsFormat should be used for all indexed fields.",
                  new StructType(
                      new Param("class", "Which NormsFormat implementation to use",
                          new PolyType(NormsFormat.class, "Lucene42",
                              new PolyEntry("Lucene42", "Lucene42NormsFormat",
                                  new Param("acceptableOverheadRatio", "How much, if any, compression should be used; pass 7.0 or higher for no compression, and 0.0 for maximum compression.", new FloatType(), PackedInts.FASTEST)),
                              new PolyEntry("Lucene40", "Lucene40NormsFormat"))))),
        new Param("directory", "Base Directory implementation to use (NRTCachingDirectory will wrap this); either one of the core implementations (FSDirectory, MMapDirectory, NIOFSDirectory, SimpleFSDirectory, RAMDirectory (for temporary indices!) or a fully qualified path to a Directory implementation that has a public constructor taking a single File argument",
            new StringType(), "FSDirectory"));

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Change global offline settings for this index.  This returns the currently set settings; pass no settings changes to retrieve current settings.";
  }

  /** Sole constructor. */
  public SettingsHandler(GlobalState state) {
    super(state);
  }

  @Override
  public FinishRequest handle(final IndexState state, Request r, Map<String,List<String>> params) throws Exception {
    // nocommit how to / should we make this truly thread
    // safe?
    final DirectoryFactory df;
    final String directoryJSON;
    if (r.hasParam("directory")) {
      directoryJSON = r.getRaw("directory").toString();
      df = DirectoryFactory.get(r.getString("directory"));
    } else {
      df = null;
      directoryJSON = null;
    }

    if (r.hasParam("normsFormat")) {
      Request r2 = r.getStruct("normsFormat");
      Request.PolyResult npr = r2.getPoly("class");
      float acceptableOverheadRatio;
      if (npr.name.equals("Lucene42") && npr.r.hasParam("acceptableOverheadRatio")) {
        acceptableOverheadRatio = npr.r.getFloat("acceptableOverheadRatio");
      } else {
        acceptableOverheadRatio = PackedInts.FASTEST;
      }

      state.setNormsFormat(npr.name, acceptableOverheadRatio);
      
      // Sneaky: if we don't do this, and if the PolyResult
      // was a struct, then state.mergeSimpleSettings below
      // gets angry because it doesn't know what to do w/
      // this parameter:
      r.clearParam("normsFormat");
    }

    // nocommit these settings take effect even if there is
    // an error?
    state.mergeSimpleSettings(r);

    return new FinishRequest() {
      @Override
      public String finish() {
        if (df != null) {
          state.setDirectoryFactory(df, JSONValue.parse(directoryJSON));
        }
        return state.getSettingsJSON();
      }
    };
  }
}

