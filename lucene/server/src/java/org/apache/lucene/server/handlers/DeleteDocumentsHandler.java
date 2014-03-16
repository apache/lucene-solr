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

import org.apache.lucene.index.Term;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.ListType;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import net.minidev.json.JSONObject;

/** Handles {@code deleteDocuments}. */
public class DeleteDocumentsHandler extends Handler {
  // TODO: support delete by query too:
  // TODO: support bulk api?
  final static StructType TYPE = new StructType(
                               new Param("indexName", "Index name", new StringType()),
                               new Param("field", "Field to match to identify the deleted documents.", new StringType()),
                               new Param("values", "Values to delete.", new ListType(new StringType())));

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Delete documents.  Returns the index generation (indexGen) that reflext the deletion.";
  }

  /** Sole constructor. */
  public DeleteDocumentsHandler(GlobalState state) {
    super(state);
  }

  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {
    final String field = r.getString("field");
    final List<Object> ids = r.getList("values");
    final Term[] terms = new Term[ids.size()];
    for(int i=0;i<terms.length;i++) {
      // TODO: how to allow arbitrary binary keys?  how to
      // pass binary data via json...?  byte array?
      terms[i] = new Term(field, (String) ids.get(i));
    }
    state.verifyStarted(r);

    return new FinishRequest() {
      @Override
      public String finish() throws IOException {
        state.writer.deleteDocuments(terms);
        JSONObject o = new JSONObject();
        o.put("indexGen", state.writer.getGeneration());
        return o.toString();
      }
    };
  }
}
