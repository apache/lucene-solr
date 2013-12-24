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

import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.handlers.Handler;
import org.apache.lucene.server.params.IntType;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import org.apache.lucene.server.params.Type;
import org.apache.lucene.server.plugins.BinaryDocumentPlugin;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.PasswordProvider;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class CrackDocumentHandler extends Handler {
  private final static StructType TYPE =
    new StructType(new Param("indexName", "Index name", new StringType()),
                   new Param("content", "Base64 encoded binary content", new StringType()),
                   new Param("contentType", "Mime type, if known (for example, application/pdf); otherwise the mime type will be autodetected", new StringType()),
                   new Param("fileName", "File name, if known; otherwise the mime type will be autodetected", new StringType()),
                   new Param("password", "Password, if necessary", new StringType()),
                   new Param("maxLength", "Maximum length in bytes of extracted text, or -1 to disable the limit", new IntType(), 1024*1024));

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Parses the provided binary document and extracts and returns all available text and metadata; this does not alter the index, and is useful for diagnostic purposes.";
  }

  public CrackDocumentHandler(GlobalState state) {
    super(state);
  }

  @Override
  public FinishRequest handle(final IndexState state, Request r, Map<String,List<String>> params) throws Exception {
    byte[] decodedBytes = Base64.decodeBase64(r.getString("content"));
    ParseContext parseContext = new ParseContext();
    parseContext.set(Parser.class, BinaryDocumentPlugin.tikaAutoDetectParser);
    Metadata md = new Metadata();
    if (r.hasParam("fileName")) {
      md.add(Metadata.RESOURCE_NAME_KEY, r.getString("fileName"));
    }
    if (r.hasParam("contentType")) {
      md.add(Metadata.CONTENT_TYPE, r.getString("contentType"));
    }
    if (r.hasParam("password")) {
      final String password = r.getString("password");
      parseContext.set(PasswordProvider.class, new PasswordProvider() {
        @Override
        public String getPassword(Metadata metadata) {
          return password;
        }
      });
    }

    final JSONObject result = new JSONObject();

    ContentHandler handler = new BodyContentHandler(r.getInt("maxLength"));
    BinaryDocumentPlugin.tikaAutoDetectParser.parse(TikaInputStream.get(decodedBytes),
                                                    handler, md, parseContext);
    result.put("body", handler.toString());
    for(String name : md.names()) {
      if (md.isMultiValued(name)) {
        JSONArray arr = new JSONArray();
        result.put(name, arr);
        for(String value : md.getValues(name)) {
          arr.add(value);
        }
      } else {
        result.put(name, md.get(name));
      }
    }

    return new FinishRequest() {
      @Override
      public String finish() throws IOException {
        return result.toString();
      }
    };
  }
}
