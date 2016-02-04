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
package org.apache.lucene.analysis.path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.AttributeFactory;

/**
 * Tokenizer for domain-like hierarchies.
 * <p>
 * Take something like:
 *
 * <pre>
 * www.site.co.uk
 * </pre>
 *
 * and make:
 *
 * <pre>
 * www.site.co.uk
 * site.co.uk
 * co.uk
 * uk
 * </pre>
 *
 */
public class ReversePathHierarchyTokenizer extends Tokenizer {

  public ReversePathHierarchyTokenizer() {
    this(DEFAULT_BUFFER_SIZE, DEFAULT_DELIMITER, DEFAULT_DELIMITER, DEFAULT_SKIP);
  }

  public ReversePathHierarchyTokenizer(int skip) {
    this(DEFAULT_BUFFER_SIZE, DEFAULT_DELIMITER, DEFAULT_DELIMITER, skip);
  }

  public ReversePathHierarchyTokenizer(int bufferSize, char delimiter) {
    this(bufferSize, delimiter, delimiter, DEFAULT_SKIP);
  }

  public ReversePathHierarchyTokenizer(char delimiter, char replacement) {
    this(DEFAULT_BUFFER_SIZE, delimiter, replacement, DEFAULT_SKIP);
  }

  public ReversePathHierarchyTokenizer(int bufferSize, char delimiter, char replacement) {
    this(bufferSize, delimiter, replacement, DEFAULT_SKIP);
  }

  public ReversePathHierarchyTokenizer(char delimiter, int skip) {
    this( DEFAULT_BUFFER_SIZE, delimiter, delimiter, skip);
  }

  public ReversePathHierarchyTokenizer(char delimiter, char replacement, int skip) {
    this(DEFAULT_BUFFER_SIZE, delimiter, replacement, skip);
  }

  public ReversePathHierarchyTokenizer
      (AttributeFactory factory, char delimiter, char replacement, int skip) {
    this(factory, DEFAULT_BUFFER_SIZE, delimiter, replacement, skip);
  }

  public ReversePathHierarchyTokenizer( int bufferSize, char delimiter, char replacement, int skip) {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, bufferSize, delimiter, replacement, skip);
  }
  public ReversePathHierarchyTokenizer
      (AttributeFactory factory, int bufferSize, char delimiter, char replacement, int skip) {
    super(factory);
    if (bufferSize < 0) {
      throw new IllegalArgumentException("bufferSize cannot be negative");
    }
    if (skip < 0) {
      throw new IllegalArgumentException("skip cannot be negative");
    }
    termAtt.resizeBuffer(bufferSize);
    this.delimiter = delimiter;
    this.replacement = replacement;
    this.skip = skip;
    resultToken = new StringBuilder(bufferSize);
    resultTokenBuffer = new char[bufferSize];
    delimiterPositions = new ArrayList<>(bufferSize/10);
  }

  private static final int DEFAULT_BUFFER_SIZE = 1024;
  public static final char DEFAULT_DELIMITER = '/';
  public static final int DEFAULT_SKIP = 0;

  private final char delimiter;
  private final char replacement;
  private final int skip;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posAtt = addAttribute(PositionIncrementAttribute.class);
  
  private int endPosition = 0;
  private int finalOffset = 0;
  private int skipped = 0;
  private StringBuilder resultToken;

  private List<Integer> delimiterPositions;
  private int delimitersCount = -1;
  private char[] resultTokenBuffer;

  @Override
  public final boolean incrementToken() throws IOException {
    clearAttributes();
    if(delimitersCount == -1){
      int length = 0;
      delimiterPositions.add(0);
      while (true) {
        int c = input.read();
        if( c < 0 ) {
          break;
        }
        length++;
        if( c == delimiter ) {
          delimiterPositions.add(length);
          resultToken.append(replacement);
        }
        else{
          resultToken.append((char)c);
        }
      }
      delimitersCount = delimiterPositions.size();
      if( delimiterPositions.get(delimitersCount-1) < length ){
        delimiterPositions.add(length);
        delimitersCount++;
      }
      if( resultTokenBuffer.length < resultToken.length() ){
        resultTokenBuffer = new char[resultToken.length()];
      }
      resultToken.getChars(0, resultToken.length(), resultTokenBuffer, 0);
      resultToken.setLength(0);
      int idx = delimitersCount-1 - skip;
      if (idx >= 0) {
        // otherwise it's ok, because we will skip and return false
        endPosition = delimiterPositions.get(idx);
      }
      finalOffset = correctOffset(length);
      posAtt.setPositionIncrement(1);
    }
    else{
      posAtt.setPositionIncrement(0);
    }

    while( skipped < delimitersCount-skip-1 ){
      int start = delimiterPositions.get(skipped);
      termAtt.copyBuffer(resultTokenBuffer, start, endPosition - start);
      offsetAtt.setOffset(correctOffset(start), correctOffset(endPosition));
      skipped++;
      return true;
    }

    return false;
  }

  @Override
  public final void end() throws IOException {
    super.end();
    // set final offset
    offsetAtt.setOffset(finalOffset, finalOffset);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    resultToken.setLength(0);
    finalOffset = 0;
    endPosition = 0;
    skipped = 0;
    delimitersCount = -1;
    delimiterPositions.clear();
  }
}
