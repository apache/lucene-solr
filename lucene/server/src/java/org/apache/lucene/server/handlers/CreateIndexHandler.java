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

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;

/** Handles {@code createIndex}. */
public class CreateIndexHandler extends Handler {
  private static StructType TYPE = new StructType(
                                       new Param("indexName", "Index name", new StringType()),
                                       new Param("rootDir", "Filesystem path where all state is stored", new StringType()));

  /** Sole constructor. */
  public CreateIndexHandler(GlobalState state) {
    super(state);
    requiresIndexName = false;
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Create an index";
  }

  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {
    final String indexName = r.getString("indexName");
    if (!IndexState.isSimpleName(indexName)) {
      r.fail("indexName", "invalid indexName \"" + indexName + "\": must be [a-zA-Z_][a-zA-Z0-9]*");
    }
    final File rootDir;
    if (r.hasParam("rootDir")) {
      rootDir = new File(r.getString("rootDir"));
    } else {
      rootDir = null;
    }

    return new FinishRequest() {
      @Override
      public String finish() throws Exception {
        try {
          globalState.createIndex(indexName, rootDir);
        } catch (IllegalArgumentException iae) {
          r.fail("invalid indexName \"" + indexName + "\": " + iae.toString(), iae);
        }

        return "{}";
      }
    };
  }
}
