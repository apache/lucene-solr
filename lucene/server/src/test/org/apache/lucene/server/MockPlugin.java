package org.apache.lucene.server;

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

import org.apache.lucene.server.handlers.Handler;
import org.apache.lucene.server.params.IntType;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StructType;
import org.apache.lucene.server.plugins.Plugin;
import net.minidev.json.JSONObject;

public class MockPlugin extends Plugin {

  @Override
  public String getTopDoc() {
    return "Adds foobar param to AddDocument";
  }

  @Override
  public String getName() {
    return "Mock";
  }

  private static class AddDocumentPreHandler implements PreHandle {

    @Override
    public void invoke(Request r) {
      Request r2 = r.getStruct("fields");
      if (r2.hasParam("mockFoobar")) {
        int x = r2.getInt("mockFoobar");
        JSONObject params = r2.getRawParams(); 
        params.put("intfield", 2*x);
      }
    }
  }

  public MockPlugin(GlobalState state) {
    Handler addDocHandler = state.getHandler("addDocument");

    // Register our pre-processor in addDocument:
    addDocHandler.addPreHandle(new AddDocumentPreHandler());

    StructType fieldsType = ((StructType) addDocHandler.getType().params.get("fields").type);

    fieldsType.addParam(new Param("mockFoobar", "Testing adding a new silly param", new IntType()));
  }
}
