package org.apache.lucene.analysis.stages;

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
import java.io.Reader;

import org.apache.lucene.analysis.tokenattributes.ArcAttribute;
import org.apache.lucene.analysis.tokenattributes.ArcAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttributeImpl;
import org.apache.lucene.analysis.util.CharacterUtils.CharacterBuffer;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.Version;

public abstract class CharTokenizerStage extends Stage {
  private static final int MAX_WORD_LEN = 255;
  private static final int IO_BUFFER_SIZE = 4096;

  private Reader input;
  private final CharTermAttribute termAtt;
  private final OffsetAttribute offsetAtt;
  private final ArcAttribute arcAtt;

  private final CharacterUtils charUtils = CharacterUtils.getInstance(Version.LUCENE_50);
  private final CharacterBuffer ioBuffer = CharacterUtils.newCharacterBuffer(IO_BUFFER_SIZE);

  // Where we are in the current chunk we are working on:
  private int bufferIndex;

  // Net char offset so far:
  private int offset;

  // How many chars currently in the "chunk" we are working on:
  private int dataLen;

  private int lastNode;

  public CharTokenizerStage() {
    super(null);
    termAtt = create(CharTermAttribute.class);
    offsetAtt = create(OffsetAttribute.class);
    arcAtt = create(ArcAttribute.class);
  }

  @Override
  public void reset(Reader input) {
    super.reset(input);
    this.input = input;
    bufferIndex = 0;
    offset = 0;
    dataLen = 0;
    ioBuffer.reset();
    lastNode = nodes.newNode();
  }

  @Override
  public boolean next() throws IOException {
    int length = 0;
    int start = bufferIndex;
    char[] buffer = termAtt.buffer();
    while (true) {
      if (bufferIndex >= dataLen) {
        offset += dataLen;
        if (!charUtils.fill(ioBuffer, input)) { // read supplementary char aware with CharacterUtils
          dataLen = 0; // so next offset += dataLen won't decrement offset
          if (length > 0) {
            break;
          } else {
            // set final offset
            // nocommit -- get charfilter working:
            //final int finalOffset = correctOffset(offset);
            final int finalOffset = offset;
            offsetAtt.setOffset(finalOffset, finalOffset);
            return false;
          }
        }
        dataLen = ioBuffer.getLength();
        bufferIndex = 0;
      }
      // use CharacterUtils here to support < 3.1 UTF-16 code unit behavior if the char based methods are gone
      final int c = charUtils.codePointAt(ioBuffer.getBuffer(), bufferIndex);
      bufferIndex += Character.charCount(c);

      if (isTokenChar(c)) {               // if it's a token char
        if (length == 0) {                // start of token
          start = offset + bufferIndex - 1;
        } else if (length >= buffer.length-1) { // check if a supplementary could run out of bounds
          buffer = termAtt.resizeBuffer(2+length); // make sure a supplementary fits in the buffer
        }
        length += Character.toChars(normalize(c), buffer, length); // buffer it, normalized
        if (length >= MAX_WORD_LEN) { // buffer overflow! make sure to check for >= surrogate pair could break == test
          break;
        }
      } else if (length > 0) {             // at non-Letter w/ chars
        break;                           // return 'em
      }
    }

    termAtt.setLength(length);

    // nocommit -- get charfilter working:
    //offsetAtt.setOffset(correctOffset(start), correctOffset(start+length));
    offsetAtt.setOffset(start, start+length);

    int node = nodes.newNode();
    arcAtt.set(lastNode, node);
    lastNode = node;

    return true;
  }

  protected abstract boolean isTokenChar(int c);

  protected int normalize(int c) {
    return c;
  }
}
