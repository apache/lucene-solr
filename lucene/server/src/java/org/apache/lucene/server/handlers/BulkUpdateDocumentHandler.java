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
import org.apache.lucene.index.Term;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.ListType;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import net.minidev.json.JSONObject;

import static org.apache.lucene.server.IndexState.AddDocumentContext;

/** Handles {@code bulkUpdateDocument}. */
public class BulkUpdateDocumentHandler extends Handler {

  /** {@link StructType} for update document. */
  public final static StructType UPDATE_DOCUMENT_TYPE =  new StructType(
                                                 new Param("term", "Identifies which document to replace", 
                                                     new StructType(
                                                         new Param("field", "Field", new StringType()),
                                                         new Param("term", "Text", new StringType()))));

  static {
    UPDATE_DOCUMENT_TYPE.params.putAll(AddDocumentHandler.DOCUMENT_TYPE.params);
  }

  private StructType TYPE = new StructType(
                                     new Param("indexName", "Index name", new StringType()),
                                     new Param("documents", "List of documents",
                                         new ListType(UPDATE_DOCUMENT_TYPE)));

  /** Sole constructor. */
  public BulkUpdateDocumentHandler(GlobalState state) {
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
    return "Update more than one document in a single HTTP connection.  Each document should be its own JSON struct, matching @updateDocument, and then there must one whitespace character separating each document.  Returns the index generation (indexGen) that contains all updated documents.";
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

    // Parse any number of documents to update:
    int count = 0;

    while (true) {
      JsonToken token = parser.nextToken();
      if (token == JsonToken.END_ARRAY) {
        break;
      }
      if (token != JsonToken.START_OBJECT) {
        throw new IllegalArgumentException("missing object");
      }

      // Parse term: and fields:
      Term updateTerm = null;

      final Document doc = new Document();

      while (true) {
        token = parser.nextToken();
        if (token == JsonToken.END_OBJECT) {
          break;
        }
        if (token != JsonToken.FIELD_NAME) {
          throw new IllegalArgumentException("missing field name");
        }
        String f = parser.getText();
        if (f.equals("term")) {
          if (parser.nextToken() != JsonToken.START_OBJECT) {
            throw new IllegalArgumentException("missing object");
          }

          // TODO: allow field to be specified only once, then
          // only text per document

          String field=null, term=null;

          while (parser.nextToken() != JsonToken.END_OBJECT) {
            String f2 = parser.getText();
            if (f2.equals("field")) {
              if (parser.nextToken() != JsonToken.VALUE_STRING) {
                throw new IllegalArgumentException("missing string value");
              }
              field = parser.getText();
              // Ensure field is valid:
              state.getField(field);
            } else if (f2.equals("term")) {
              if (parser.nextToken() != JsonToken.VALUE_STRING) {
                throw new IllegalArgumentException("missing string value");
              }
              term = parser.getText();
            } else {
              throw new IllegalArgumentException("unexpected field " + f);
            }
          }
          updateTerm = new Term(field, term);
        } else if (f.equals("fields")) {
          addDocHandler.parseFields(state, doc, parser);
        } else {
          boolean handled = false;
          for(AddDocumentHandler.PostHandle postHandle : addDocHandler.postHandlers) {
            if (postHandle.invoke(state, f, parser, doc)) {
              handled = true;
              break;
            }
          }
          if (!handled) {
            throw new IllegalArgumentException("unrecognized field " + parser.getText());
          }
        }
      }

      if (doc == null) {
        throw new IllegalArgumentException("missing fields");
      }
      if (updateTerm == null) {
        throw new IllegalArgumentException("missing term");
      }

      // TODO: this is dup'd code ... share better w/ AddDocHandler
      globalState.indexService.submit(state.getAddDocumentJob(count, updateTerm, doc, ctx));
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
    return o.toString();
  }
}


