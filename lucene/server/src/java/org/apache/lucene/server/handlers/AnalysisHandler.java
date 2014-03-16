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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/** Useful for debugging analyzers. */
public class AnalysisHandler extends Handler {
  private final static StructType TYPE = new StructType(
                                                        new Param("indexName", "Index Name", new StringType()),
                                                        new Param("text", "Text to analyze", new StringType()),
                                                        new Param("analyzer", "Analyzer", RegisterFieldHandler.ANALYZER_TYPE));

  /** Sole constructor. */
  public AnalysisHandler(GlobalState state) {
    super(state);
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Run an analyzer on text and see the resulting tokens.";
  }

  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {
    final String text = r.getString("text");

    // TODO: allow passing field name in, and we use its analyzer?
    final Analyzer a = RegisterFieldHandler.getAnalyzer(state, r, "analyzer");

    return new FinishRequest() {
      @Override
      public String finish() throws Exception {
        TokenStream ts = a.tokenStream("field", new StringReader(text));
        CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
        PositionIncrementAttribute posIncAtt = ts.addAttribute(PositionIncrementAttribute.class);
        PositionLengthAttribute posLenAtt = ts.addAttribute(PositionLengthAttribute.class);
        OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
        ts.reset();

        JSONArray tokens = new JSONArray();
        int pos = -1;
        while(ts.incrementToken()) {
          JSONObject o = new JSONObject();
          tokens.add(o);
          o.put("token", termAtt.toString());
          pos += posIncAtt.getPositionIncrement();
          o.put("position", pos);
          o.put("positionLength", posLenAtt.getPositionLength());
          o.put("startOffset", offsetAtt.startOffset());
          o.put("endOffset", offsetAtt.endOffset());
        }
        ts.end();
        ts.close();
        JSONObject result = new JSONObject();
        result.put("tokens", tokens);
        return result.toString();
      }
    };
  }
}
