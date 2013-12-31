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

import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.*;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

/** Handles {@code addDocuments}, by delegating the single
 *  document to {@link BulkAddDocumentsHandler} */
public class AddDocumentsHandler extends Handler {

  final static StructType TYPE = new StructType(
                               new Param("indexName", "Index name", new StringType()),
                               new Param("parent", "The (one) parent document for this block.  The value of this key is a single document that @addDocument expects.  Be sure to add an indexed field to only the parent document so that you can subsequently provide the filter that identifies only parent documents.",
                                         AddDocumentHandler.DOCUMENT_TYPE),
                               new Param("children", "List of child documents.",
                                         new ListType(AddDocumentHandler.DOCUMENT_TYPE)));

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Adds one document block (= single parent and multiple children) to the index.  This can be used for block grouping and block joins.  Returns the index generation (indexGen) that contains this added document block.";
  }

  /** Sole constructor. */
  public AddDocumentsHandler(GlobalState state) {
    super(state);
  }

  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {

    state.verifyStarted(r);

    // NOTE: somewhat wasteful since we re-serialize to
    // string only to re-parse the JSON, but this allows
    // single-source (bulk) for parsing, and apps that care
    // about performance will use bulk APIs:

    JSONObject raw = r.getRawParams();
    StringBuilder sb = new StringBuilder();
    sb.append("{\"indexName\": \"");
    sb.append(state.name);
    sb.append("\", \"documents\": [");
    sb.append(raw.toString());
    sb.append("]}");
    raw.clear();

    final String bulkRequestString = sb.toString();

    return new FinishRequest() {
      @Override
      public String finish() throws Exception {
        String result = globalState.getHandler("bulkAddDocuments").handleStreamed(new StringReader(bulkRequestString), null);
        if (result.indexOf("errors") != -1) {
          JSONObject o = (JSONObject) JSONValue.parseStrict(result);
          if (o.containsKey("errors")) {
            JSONObject err = (JSONObject) ((JSONArray) o.get("errors")).get(0);
            throw new IllegalArgumentException((String) err.get("exception"));
          }
        }
        return result;
      }
    };
  }
}
