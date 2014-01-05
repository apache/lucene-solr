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

import java.io.Reader;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import static org.apache.lucene.server.IndexState.AddDocumentContext;

/** Reads more than one { ... } request in a single
 *  connection, but each request must be separated by at
 *  least one whitespace char. */

public class BulkAddDocumentHandler extends Handler {

  private StructType TYPE = new StructType(
                                     new Param("indexName", "Index name", new StringType()),
                                     new Param("documents", "List of documents", new ListType(AddDocumentHandler.DOCUMENT_TYPE)));

  /** Sole constructor. */
  public BulkAddDocumentHandler(GlobalState state) {
    super(state);
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public boolean doStream() {
    return true;
  }

  @Override
  public String getTopDoc() {
    return "Add more than one document in a single request.  Returns the index generation (indexGen) that contains all added documents.";
  }

  @Override
  public FinishRequest handle(IndexState state, Request r, Map<String,List<String>> params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String handleStreamed(Reader reader, Map<String,List<String>> params) throws Exception {
    JsonFactory jfactory = new JsonFactory();

    JsonParser parser = jfactory.createJsonParser(reader);

    if (parser.nextToken() != JsonToken.START_OBJECT) {
      throw new IllegalArgumentException("expected JSON object");
    }
    if (parser.nextToken() != JsonToken.FIELD_NAME) {
      throw new IllegalArgumentException("expected indexName first");
    }
    if (!parser.getText().equals("indexName")) {
      throw new IllegalArgumentException("expected indexName first");
    }
    if (parser.nextToken() != JsonToken.VALUE_STRING) {
      throw new IllegalArgumentException("indexName should be string");
    }

    IndexState state = globalState.get(parser.getText());
    state.verifyStarted(null);
    if (parser.nextToken() != JsonToken.FIELD_NAME) {
      throw new IllegalArgumentException("expected documents next");
    }
    if (!parser.getText().equals("documents")) {
      throw new IllegalArgumentException("expected documents after indexName");
    }

    if (parser.nextToken() != JsonToken.START_ARRAY) {
      throw new IllegalArgumentException("documents should be a list");
    }

    AddDocumentContext ctx = new AddDocumentContext();

    AddDocumentHandler addDocHandler = (AddDocumentHandler) globalState.getHandler("addDocument");
    int count = 0;
    while (true) {
      Document doc = addDocHandler.parseDocument(state, parser);
      if (doc == null) {
        break;
      }
      globalState.indexService.submit(state.getAddDocumentJob(count, null, doc, ctx));
      count++;
    }

    // nocommit this is ... lameish:
    while (true) {
      if (ctx.addCount.get() == count) {
        break;
      }
      Thread.sleep(1);
    }

    JSONObject o = new JSONObject();
    o.put("indexGen", state.writer.getGeneration());
    o.put("indexedDocumentCount", count);
    if (!ctx.errors.isEmpty()) {
      JSONArray errors = new JSONArray();
      o.put("errors", errors);
      for(int i=0;i<ctx.errors.size();i++) {
        JSONObject err = new JSONObject();
        errors.add(err);
        err.put("index", ctx.errorIndex.get(i));
        err.put("exception", ctx.errors.get(i));
      }
    }
    
    return o.toString();
  }
}
