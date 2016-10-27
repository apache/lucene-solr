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

package org.apache.lucene.concordance.charoffsets;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;

/**
 * TokenCharOffsetsReader that captures character offsets by reanalyzing a
 * field.
 */
public class ReanalyzingTokenCharOffsetsReader implements
    TokenCharOffsetsReader {

  private final static int GOT_ALL_REQUESTS = -2;
  private Analyzer baseAnalyzer;

  /**
   * Constructor
   *
   * @param analyzer to use to get character offsets
   */
  public ReanalyzingTokenCharOffsetsReader(Analyzer analyzer) {
    this.baseAnalyzer = analyzer;
  }

  @Override
  public void getTokenCharOffsetResults(final Document d,
                                        final String fieldName, final TokenCharOffsetRequests requests,
                                        final RandomAccessCharOffsetContainer results) throws IOException {

    int fieldIndex = 0;
    int currPosInc = -1;
    int posIncrementGap = baseAnalyzer.getPositionIncrementGap(fieldName);
    int charOffsetGap = baseAnalyzer.getOffsetGap(fieldName);
    int charBase = 0;
    for (String fieldValue : d.getValues(fieldName)) {

      currPosInc = addFieldValue(fieldName, currPosInc, charBase, fieldValue, requests,
          results);

      if (currPosInc == GOT_ALL_REQUESTS) {
        break;
      }
      charBase += fieldValue.length() + charOffsetGap;
      currPosInc += posIncrementGap;
      fieldIndex++;
    }

  }

  private int addFieldValue(String fieldName, int currInd, int charBase, String fieldValue,
                            TokenCharOffsetRequests requests, RandomAccessCharOffsetContainer results)
      throws IOException {
    //Analyzer limitAnalyzer = new LimitTokenCountAnalyzer(baseAnalyzer, 10, true);
    TokenStream stream = baseAnalyzer.tokenStream(fieldName, fieldValue);
    stream.reset();

    int defaultInc = 1;

    CharTermAttribute termAtt = stream
        .getAttribute(org.apache.lucene.analysis.tokenattributes.CharTermAttribute.class);
    OffsetAttribute offsetAtt = stream
        .getAttribute(org.apache.lucene.analysis.tokenattributes.OffsetAttribute.class);
    PositionIncrementAttribute incAtt = null;
    if (stream
        .hasAttribute(org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute.class)) {
      incAtt = stream
          .getAttribute(org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute.class);
    }

    while (stream.incrementToken()) {

      currInd += (incAtt != null) ? incAtt.getPositionIncrement() : defaultInc;
      if (requests.contains(currInd)) {
        results.add(currInd, offsetAtt.startOffset() + charBase,
            offsetAtt.endOffset() + charBase, termAtt.toString());
      }
      if (currInd > requests.getLast()) {
        // TODO: Is there a way to avoid this? Or, is this
        // an imaginary performance hit?
        while (stream.incrementToken()) {
          //NO-OP
        }
        stream.end();
        stream.close();
        return GOT_ALL_REQUESTS;
      }
    }
    stream.end();
    stream.close();
    return currInd;
  }

}
