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

package org.apache.lucene.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.AttributeSource;

/** adds synonym of "dog" for "dogs", and synonym of "cavy" for "guinea pig". */
public class MockSynonymFilter extends TokenFilter {
  CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
  List<AttributeSource> tokenQueue = new ArrayList<>();
  boolean endOfInput = false;

  public MockSynonymFilter(TokenStream input) {
    super(input);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    tokenQueue.clear();
    endOfInput = false;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (tokenQueue.size() > 0) {
      tokenQueue.remove(0).copyTo(this);
      return true;
    }
    if (endOfInput == false && input.incrementToken()) {
      if (termAtt.toString().equals("dogs")) {
        addSynonymAndRestoreOrigToken("dog", 1, offsetAtt.endOffset());
      } else if (termAtt.toString().equals("guinea")) {
        AttributeSource firstSavedToken = cloneAttributes();
        if (input.incrementToken()) {
          if (termAtt.toString().equals("pig")) {
            AttributeSource secondSavedToken = cloneAttributes();
            int secondEndOffset = offsetAtt.endOffset();
            firstSavedToken.copyTo(this);
            addSynonym("cavy", 2, secondEndOffset);
            tokenQueue.add(secondSavedToken);
          } else if (termAtt.toString().equals("dogs")) {
            tokenQueue.add(cloneAttributes());
            addSynonym("dog", 1, offsetAtt.endOffset());
          }
        } else {
          endOfInput = true;
        }
        firstSavedToken.copyTo(this);
      }
      return true;
    } else {
      endOfInput = true;
      return false;
    }
  }
  private void addSynonym(String synonymText, int posLen, int endOffset) {
    termAtt.setEmpty().append(synonymText);
    posIncAtt.setPositionIncrement(0);
    posLenAtt.setPositionLength(posLen);
    offsetAtt.setOffset(offsetAtt.startOffset(), endOffset);
    tokenQueue.add(cloneAttributes());
  }
  private void addSynonymAndRestoreOrigToken(String synonymText, int posLen, int endOffset) {
    AttributeSource origToken = cloneAttributes();
    addSynonym(synonymText, posLen, endOffset);
    origToken.copyTo(this);
  }
}


