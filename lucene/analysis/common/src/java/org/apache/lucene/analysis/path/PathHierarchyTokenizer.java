package org.apache.lucene.analysis.path;
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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * Tokenizer for path-like hierarchies.
 * <p>
 * Take something like:
 *
 * <pre>
 *  /something/something/else
 * </pre>
 *
 * and make:
 *
 * <pre>
 *  /something
 *  /something/something
 *  /something/something/else
 * </pre>
 */
public class PathHierarchyTokenizer extends Tokenizer {

  public PathHierarchyTokenizer(Reader input) {
    this(input, DEFAULT_BUFFER_SIZE, DEFAULT_DELIMITER, DEFAULT_DELIMITER, DEFAULT_SKIP);
  }

  public PathHierarchyTokenizer(Reader input, int skip) {
    this(input, DEFAULT_BUFFER_SIZE, DEFAULT_DELIMITER, DEFAULT_DELIMITER, skip);
  }

  public PathHierarchyTokenizer(Reader input, int bufferSize, char delimiter) {
    this(input, bufferSize, delimiter, delimiter, DEFAULT_SKIP);
  }

  public PathHierarchyTokenizer(Reader input, char delimiter, char replacement) {
    this(input, DEFAULT_BUFFER_SIZE, delimiter, replacement, DEFAULT_SKIP);
  }

  public PathHierarchyTokenizer(Reader input, char delimiter, char replacement, int skip) {
    this(input, DEFAULT_BUFFER_SIZE, delimiter, replacement, skip);
  }

  public PathHierarchyTokenizer(AttributeFactory factory, Reader input, char delimiter, char replacement, int skip) {
    this(factory, input, DEFAULT_BUFFER_SIZE, delimiter, replacement, skip);
  }

  public PathHierarchyTokenizer(Reader input, int bufferSize, char delimiter, char replacement, int skip) {
    this(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, input, bufferSize, delimiter, replacement, skip);
  }

  public PathHierarchyTokenizer
      (AttributeFactory factory, Reader input, int bufferSize, char delimiter, char replacement, int skip) {
    super(factory, input);
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
  private int startPosition = 0;
  private int skipped = 0;
  private boolean endDelimiter = false;
  private StringBuilder resultToken;
  
  private int charsRead = 0;


  @Override
  public final boolean incrementToken() throws IOException {
    clearAttributes();
    termAtt.append( resultToken );
    if(resultToken.length() == 0){
      posAtt.setPositionIncrement(1);
    }
    else{
      posAtt.setPositionIncrement(0);
    }
    int length = 0;
    boolean added = false;
    if( endDelimiter ){
      termAtt.append(replacement);
      length++;
      endDelimiter = false;
      added = true;
    }

    while (true) {
      int c = input.read();
      if (c >= 0) {
        charsRead++;
      } else {
        if( skipped > skip ) {
          length += resultToken.length();
          termAtt.setLength(length);
           offsetAtt.setOffset(correctOffset(startPosition), correctOffset(startPosition + length));
          if( added ){
            resultToken.setLength(0);
            resultToken.append(termAtt.buffer(), 0, length);
          }
          return added;
        }
        else{
          return false;
        }
      }
      if( !added ){
        added = true;
        skipped++;
        if( skipped > skip ){
          termAtt.append(c == delimiter ? replacement : (char)c);
          length++;
        }
        else {
          startPosition++;
        }
      }
      else {
        if( c == delimiter ){
          if( skipped > skip ){
            endDelimiter = true;
            break;
          }
          skipped++;
          if( skipped > skip ){
            termAtt.append(replacement);
            length++;
          }
          else {
            startPosition++;
          }
        }
        else {
          if( skipped > skip ){
            termAtt.append((char)c);
            length++;
          }
          else {
            startPosition++;
          }
        }
      }
    }
    length += resultToken.length();
    termAtt.setLength(length);
    offsetAtt.setOffset(correctOffset(startPosition), correctOffset(startPosition+length));
    resultToken.setLength(0);
    resultToken.append(termAtt.buffer(), 0, length);
    return true;
  }

  @Override
  public final void end() {
    // set final offset
    int finalOffset = correctOffset(charsRead);
    offsetAtt.setOffset(finalOffset, finalOffset);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    resultToken.setLength(0);
    charsRead = 0;
    endDelimiter = false;
    skipped = 0;
    startPosition = 0;
  }
}
