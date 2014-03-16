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

import org.apache.lucene.server.FieldDef;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.StringLiveFieldValues;
import org.apache.lucene.server.params.ListType;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/** Handles {@code liveValues}. */
public class LiveValuesHandler extends Handler {
  private final static StructType TYPE = new StructType (
                                             new Param("indexName", "Index name", new StringType()),
                                             new Param("ids", "List of ids to retrieve", new ListType(new StringType())),
                                             new Param("field", "Which field value to look up", new StringType()));
  @Override
  public String getTopDoc() {
    return "Lookup live field values.";
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  /** Sole constructor. */
  public LiveValuesHandler(GlobalState state) {
    super(state);
  }

  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {
    FieldDef fd = state.getField(r, "field");

    if (fd.liveValuesIDField == null) {
      r.fail("field", "field \"" + fd.liveValuesIDField + "\" was not registered with liveValues");
    }

    final List<Object> idValues = r.getList("ids");

    final StringLiveFieldValues lv = state.liveFieldValues.get(fd.name);
    assert lv != null;

    return new FinishRequest() {
      @Override
      public String finish() throws IOException {
        JSONObject result = new JSONObject();
        JSONArray arr = new JSONArray();
        result.put("values", arr);
        for(Object o : idValues) {
          arr.add(lv.get((String) o));
        }
        return result.toString();
      }
    };
  }  
}
