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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.*;
import net.minidev.json.JSONObject;

/** Handles {@code setCommitUserData}. */
public class SetCommitUserDataHandler extends Handler {

  private static StructType TYPE = new StructType(
                                                  new Param("indexName", "Index name", new StringType()),
                                                  new Param("userData", "Custom Map<String,String>", new AnyType()));

  /** Sole constructor. */
  public SetCommitUserDataHandler(GlobalState state) {
    super(state);
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Sets custom user data in the index, later retrieved with getCommitUserData.";
  }
  
  @Override
  public FinishRequest handle(final IndexState state, Request r, Map<String,List<String>> params) throws Exception {

    final Map<String,String> userData = new HashMap<String,String>();
    for(Map.Entry<String,Object> ent : ((JSONObject) r.getAndRemoveRaw("userData")).entrySet()) {
      if (ent.getValue() instanceof String == false) {
        r.failWrongClass("userData", "all values must be String ", ent.getValue());
      }
      userData.put(ent.getKey(), (String) ent.getValue());
    }

    return new FinishRequest() {
      @Override
      public String finish() throws IOException {
        state.writer.getIndexWriter().setCommitData(userData);
        return "{}";
      }
    };
  }
}
