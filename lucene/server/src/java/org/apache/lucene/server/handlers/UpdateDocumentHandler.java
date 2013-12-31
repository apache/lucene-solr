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
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

/** Handles {@code updateDocument}, by delegating the single
 *  document to {@link BulkUpdateDocumentHandler}. */
public class UpdateDocumentHandler extends Handler {
  final StructType TYPE = new StructType(
                               new Param("indexName", "Index Name", new StringType()),
                               new Param("term", "Identifies which document to replace", 
                                         new StructType(
                                             new Param("field", "Field", new StringType()),
                                             new Param("term", "Text", new StringType()))));

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Replaces one document in the index.  Returns the index generation (indexGen) that contains this added document.";
  }

  /** Sole constructor. */
  public UpdateDocumentHandler(GlobalState state) {
    super(state);
    TYPE.params.putAll(AddDocumentHandler.DOCUMENT_TYPE.params);
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
        String result = globalState.getHandler("bulkUpdateDocument").handleStreamed(new StringReader(bulkRequestString), null);
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
