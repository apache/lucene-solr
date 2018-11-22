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
package org.apache.solr.spelling;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;


/**
 *
 * @since solr 1.3
 **/
class SimpleQueryConverter extends SpellingQueryConverter {

  @Override
  public Collection<Token> convert(String origQuery) {
    Collection<Token> result = new HashSet<>();

    try (WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(); TokenStream ts = analyzer.tokenStream("", origQuery)) {
      // TODO: support custom attributes
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
      TypeAttribute typeAtt = ts.addAttribute(TypeAttribute.class);
      FlagsAttribute flagsAtt = ts.addAttribute(FlagsAttribute.class);
      PayloadAttribute payloadAtt = ts.addAttribute(PayloadAttribute.class);
      PositionIncrementAttribute posIncAtt = ts.addAttribute(PositionIncrementAttribute.class);

      ts.reset();

      while (ts.incrementToken()) {
        Token tok = new Token();
        tok.copyBuffer(termAtt.buffer(), 0, termAtt.length());
        tok.setOffset(offsetAtt.startOffset(), offsetAtt.endOffset());
        tok.setFlags(flagsAtt.getFlags());
        tok.setPayload(payloadAtt.getPayload());
        tok.setPositionIncrement(posIncAtt.getPositionIncrement());
        tok.setType(typeAtt.type());
        result.add(tok);
      }
      ts.end();      
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
