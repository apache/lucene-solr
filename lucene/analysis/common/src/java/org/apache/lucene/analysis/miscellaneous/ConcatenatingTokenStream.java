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

package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.IOUtils;

/**
 * A TokenStream that takes an array of input TokenStreams as sources, and
 * concatenates them together.
 *
 * Offsets from the second and subsequent sources are incremented to behave
 * as if all the inputs were from a single source.
 *
 * All of the input TokenStreams must have the same attribute implementations
 */
public final class ConcatenatingTokenStream extends TokenStream {

  private final TokenStream[] sources;
  private final OffsetAttribute[] sourceOffsets;
  private final PositionIncrementAttribute[] sourceIncrements;
  private final OffsetAttribute offsetAtt;
  private final PositionIncrementAttribute posIncAtt;

  private int currentSource;
  private int offsetIncrement;
  private int initialPositionIncrement = 1;

  /**
   * Create a new ConcatenatingTokenStream from a set of inputs
   * @param sources an array of TokenStream inputs to concatenate
   */
  public ConcatenatingTokenStream(TokenStream... sources) {
    super(combineSources(sources));
    this.sources = sources;
    this.offsetAtt = addAttribute(OffsetAttribute.class);
    this.posIncAtt = addAttribute(PositionIncrementAttribute.class);
    this.sourceOffsets = new OffsetAttribute[sources.length];
    this.sourceIncrements = new PositionIncrementAttribute[sources.length];
    for (int i = 0; i < sources.length; i++) {
      this.sourceOffsets[i] = sources[i].addAttribute(OffsetAttribute.class);
      this.sourceIncrements[i] = sources[i].addAttribute(PositionIncrementAttribute.class);
    }
  }

  private static AttributeSource combineSources(TokenStream... sources) {
    AttributeSource base = sources[0].cloneAttributes();
    try {
      for (int i = 1; i < sources.length; i++) {
        Iterator<Class<? extends Attribute>> it = sources[i].getAttributeClassesIterator();
        while (it.hasNext()) {
          base.addAttribute(it.next());
        }
        // check attributes can be captured
        sources[i].copyTo(base);
      }
      return base;
    }
    catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Attempted to concatenate TokenStreams with different attribute types", e);
    }
  }

  @Override
  public boolean incrementToken() throws IOException {
    boolean newSource = false;
    while (sources[currentSource].incrementToken() == false) {
      if (currentSource >= sources.length - 1)
        return false;
      sources[currentSource].end();
      initialPositionIncrement = sourceIncrements[currentSource].getPositionIncrement();
      OffsetAttribute att = sourceOffsets[currentSource];
      if (att != null)
        offsetIncrement += att.endOffset();
      currentSource++;
      newSource = true;
    }

    clearAttributes();
    sources[currentSource].copyTo(this);
    offsetAtt.setOffset(offsetAtt.startOffset() + offsetIncrement, offsetAtt.endOffset() + offsetIncrement);
    if (newSource) {
      int posInc = posIncAtt.getPositionIncrement();
      posIncAtt.setPositionIncrement(posInc + initialPositionIncrement);
    }

    return true;
  }

  @Override
  public void end() throws IOException {
    sources[currentSource].end();
    int finalOffset = sourceOffsets[currentSource].endOffset() + offsetIncrement;
    int finalPosInc = sourceIncrements[currentSource].getPositionIncrement();
    super.end();
    offsetAtt.setOffset(finalOffset, finalOffset);
    posIncAtt.setPositionIncrement(finalPosInc);
  }

  @Override
  public void reset() throws IOException {
    for (TokenStream source : sources) {
      source.reset();
    }
    super.reset();
    currentSource = 0;
    offsetIncrement = 0;
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(sources);
    }
    finally {
      super.close();
    }
  }
}
