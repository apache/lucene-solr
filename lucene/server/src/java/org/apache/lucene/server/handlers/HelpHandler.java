package org.apache.lucene.server.handlers;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONStyle;
import net.minidev.json.JSONStyleIdent;

import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;

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

public class HelpHandler extends Handler {
  
  /** The parameters this handler accepts. */
  public final static StructType TYPE =
    new StructType(
        new Param("message", "Optional message to tell user why Help was invoked.", new StringType())
    );
    
  /** Sole constructor. */
  public HelpHandler(GlobalState globalState) {
    super(globalState);
    requiresIndexName = false;
  }

  @Override
  public FinishRequest handle(IndexState state, Request request,
      Map<String,List<String>> params) throws Exception {
    final JSONObject result = new JSONObject();
    if(params.get("message")!=null) {
      result.put("message", params.get("message"));
    }
    Map<String,JSONObject> handlerInfo = new LinkedHashMap<String,JSONObject>();
    for(Map.Entry<String,Handler> entry : globalState.getHandlers().entrySet()) {
      String handlerName = entry.getKey();
      Handler handler = entry.getValue();
      String handlerDesc = handler.getTopDoc();
      Map<String,String> paramInfo = new LinkedHashMap<String,String>();
      for(Map.Entry<String,Param> paramEntry : handler.getType().params.entrySet()) {
        Param param = paramEntry.getValue();
        paramInfo.put(paramEntry.getKey(), param.name + " (" + param.type.getClass().getSimpleName() + ")");
      }
      JSONObject handlerJson = new JSONObject();
      handlerJson.put("description", handlerDesc);
      handlerJson.put("parameters", paramInfo);
      handlerInfo.put(handlerName, handlerJson);
    }
    result.put("Available Handlers", handlerInfo);
        
    return new FinishRequest() {
      @Override
      public String finish() {
        return result.toString(new JSONStyleIdent());
      }
    };
  }
    
  @Override
  public StructType getType() {
    return TYPE;
  }
  
  @Override
  public String getTopDoc() {
    return "Display help on server usage.";
  }
  
}
