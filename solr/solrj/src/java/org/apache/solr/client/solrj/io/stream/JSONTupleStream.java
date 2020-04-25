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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;


/*
  Queries a Solr instance, and maps SolrDocs to Tuples.
  Initial version works with the json format and only SolrDocs are handled.
*/

public class JSONTupleStream implements TupleStreamParser {
  private List<String> path;  // future... for more general stream handling
  private Reader reader;
  private JSONParser parser;
  private boolean atDocs;

  public JSONTupleStream(Reader reader) {
    this.reader = reader;
    this.parser = new JSONParser(reader);
  }

  // temporary...
  public static JSONTupleStream create(SolrClient server, SolrParams requestParams) throws IOException, SolrServerException {
    String p = requestParams.get("qt");
    if(p != null) {
      ModifiableSolrParams modifiableSolrParams = (ModifiableSolrParams) requestParams;
      modifiableSolrParams.remove("qt");
    }

    QueryRequest query = new QueryRequest( requestParams );
    query.setPath(p);
    query.setResponseParser(new InputStreamResponseParser("json"));
    query.setMethod(SolrRequest.METHOD.POST);
    NamedList<Object> genericResponse = server.request(query);
    InputStream stream = (InputStream)genericResponse.get("stream");
    InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
    return new JSONTupleStream(reader);
  }


  /** returns the next Tuple or null */
  @Override
  public Map<String,Object> next() throws IOException {
    if (!atDocs) {
      boolean found = advanceToDocs();
      atDocs = true;
      if (!found) return null;
    }
    // advance past ARRAY_START (in the case that we just advanced to docs, or OBJECT_END left over from the last call.
    int event = parser.nextEvent();
    if (event == JSONParser.ARRAY_END) return null;

    Object o = ObjectBuilder.getVal(parser);
    // right now, getVal will leave the last event read as OBJECT_END

    return (Map<String,Object>)o;
  }

  public void close() throws IOException {
    reader.close();
  }


  private void expect(int parserEventType) throws IOException {
    int event = parser.nextEvent();
    if (event != parserEventType) {
      throw new IOException("JSONTupleStream: expected " + JSONParser.getEventString(parserEventType) + " but got " + JSONParser.getEventString(event) );
    }
  }

  private void expect(String mapKey) {


  }

  private boolean advanceToMapKey(String key, boolean deepSearch) throws IOException {
    for (;;) {
      int event = parser.nextEvent();
      switch (event) {
        case JSONParser.STRING:
          if (key != null) {
            String val = parser.getString();
            if (key.equals(val)) {
              return true;
            } else if("error".equals(val)) {
              handleError();
            }
          }
          break;
        case JSONParser.OBJECT_END:
          return false;
        case JSONParser.OBJECT_START:
          if (deepSearch) {
            boolean found = advanceToMapKey(key, true);
            if (found) {
              return true;
            }
          } else {
            advanceToMapKey(null, false);
          }
          break;
        case JSONParser.ARRAY_START:
          skipArray(key, deepSearch);
          break;
      }
    }
  }

  private void handleError() throws IOException {
    for (;;) {
      int event = parser.nextEvent();
      if(event == JSONParser.STRING) {
        String val = parser.getString();
        if("msg".equals(val)) {
          event = parser.nextEvent();
          if(event == JSONParser.STRING) {
            String msg = parser.getString();
            if(msg != null) {
              throw new SolrStream.HandledException(msg);
            }
          }
        }
      } else if (event == JSONParser.OBJECT_END) {
        throw new IOException("");
      }
    }
  }

  private void skipArray(String key, boolean deepSearch) throws IOException {
    for (;;) {
      int event = parser.nextEvent();
      switch (event) {
        case JSONParser.OBJECT_START:
          advanceToMapKey(key, deepSearch);
          break;
        case JSONParser.ARRAY_START:
          skipArray(key, deepSearch);
          break;
        case JSONParser.ARRAY_END:
          return;
      }
    }
  }


  private boolean advanceToDocs() throws IOException {
    expect(JSONParser.OBJECT_START);
    boolean found = advanceToMapKey("docs", true);
    expect(JSONParser.ARRAY_START);
    return found;
  }



}