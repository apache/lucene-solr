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

package org.apache.solr.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilterFactory;
import org.apache.lucene.analysis.payloads.NumericPayloadTokenFilterFactory;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.payloads.AveragePayloadFunction;
import org.apache.lucene.queries.payloads.MaxPayloadFunction;
import org.apache.lucene.queries.payloads.MinPayloadFunction;
import org.apache.lucene.queries.payloads.PayloadDecoder;
import org.apache.lucene.queries.payloads.PayloadFunction;
import org.apache.lucene.queries.payloads.SumPayloadFunction;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.PayloadScoreQParserPlugin;

public class PayloadUtils {

  public static String getPayloadEncoder(FieldType fieldType) {
    // TODO: support custom payload encoding fields too somehow - maybe someone has a custom component that encodes payloads as floats
    String encoder = null;
    Analyzer a = fieldType.getIndexAnalyzer();
    if (a instanceof TokenizerChain) {
      // examine the indexing analysis chain for DelimitedPayloadTokenFilterFactory or NumericPayloadTokenFilterFactory
      TokenizerChain tc = (TokenizerChain)a;
      TokenFilterFactory[] factories = tc.getTokenFilterFactories();
      for (TokenFilterFactory factory : factories) {
        if (factory instanceof DelimitedPayloadTokenFilterFactory) {
          encoder = factory.getOriginalArgs().get(DelimitedPayloadTokenFilterFactory.ENCODER_ATTR);
          break;
        }

        if (factory instanceof NumericPayloadTokenFilterFactory) {
          // encodes using `PayloadHelper.encodeFloat(payload)`
          encoder = "float";
          break;
        }
      }
    }

    return encoder;
  }

  public static PayloadDecoder getPayloadDecoder(FieldType fieldType) {
    PayloadDecoder decoder = null;

    String encoder = getPayloadEncoder(fieldType);

    if ("integer".equals(encoder)) {
      decoder = (BytesRef payload) -> payload == null ? 1 : PayloadHelper.decodeInt(payload.bytes, payload.offset);
    }
    if ("float".equals(encoder)) {
      decoder = (BytesRef payload) -> payload == null ? 1 : PayloadHelper.decodeFloat(payload.bytes, payload.offset);
    }
    // encoder could be "identity" at this point, in the case of DelimitedTokenFilterFactory encoder="identity"

    // TODO: support pluggable payload decoders?

    return decoder;
  }

  public static PayloadFunction getPayloadFunction(String func) {
    PayloadFunction payloadFunction = null;
    if ("min".equals(func)) {
      payloadFunction = new MinPayloadFunction();
    }
    if ("max".equals(func)) {
      payloadFunction = new MaxPayloadFunction();
    }
    if ("average".equals(func)) {
      payloadFunction = new AveragePayloadFunction();
    }
    if ("sum".equals(func)) {
      payloadFunction = new SumPayloadFunction();
    }
    return payloadFunction;
  }

  public static SpanQuery createSpanQuery(String field, String value, Analyzer analyzer) throws IOException {
    return createSpanQuery(field, value, analyzer, PayloadScoreQParserPlugin.DEFAULT_OPERATOR);
  }


  /**
   * The generated SpanQuery will be either a SpanTermQuery or an ordered, zero slop SpanNearQuery, depending
   * on how many tokens are emitted.
   */
  public static SpanQuery createSpanQuery(String field, String value, Analyzer analyzer, String operator) throws IOException {
    // adapted this from QueryBuilder.createSpanQuery (which isn't currently public) and added reset(), end(), and close() calls
    List<SpanTermQuery> terms = new ArrayList<>();
    try (TokenStream in = analyzer.tokenStream(field, value)) {
      in.reset();

      TermToBytesRefAttribute termAtt = in.getAttribute(TermToBytesRefAttribute.class);
      while (in.incrementToken()) {
        terms.add(new SpanTermQuery(new Term(field, termAtt.getBytesRef())));
      }
      in.end();
    }

    SpanQuery query;
    if (terms.isEmpty()) {
      query = null;
    } else if (terms.size() == 1) {
      query = terms.get(0);
    } else if (operator != null && operator.equalsIgnoreCase("or")) {
        query = new SpanOrQuery(terms.toArray(new SpanTermQuery[terms.size()]));
    } else {
        query = new SpanNearQuery(terms.toArray(new SpanTermQuery[terms.size()]), 0, true);
      }
    return query;
  }
}
