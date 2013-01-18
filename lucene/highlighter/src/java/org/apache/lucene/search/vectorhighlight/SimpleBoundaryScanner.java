package org.apache.lucene.search.vectorhighlight;

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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple boundary scanner implementation that divides fragments
 * based on a set of separator characters.
 */
public class SimpleBoundaryScanner implements BoundaryScanner {
  
  public static final int DEFAULT_MAX_SCAN = 20;
  public static final Character[] DEFAULT_BOUNDARY_CHARS = {'.', ',', '!', '?', ' ', '\t', '\n'};

  protected int maxScan;
  protected Set<Character> boundaryChars;
  
  public SimpleBoundaryScanner(){
    this( DEFAULT_MAX_SCAN, DEFAULT_BOUNDARY_CHARS );
  }
  
  public SimpleBoundaryScanner( int maxScan ){
    this( maxScan, DEFAULT_BOUNDARY_CHARS );
  }
  
  public SimpleBoundaryScanner( Character[] boundaryChars ){
    this( DEFAULT_MAX_SCAN, boundaryChars );
  }
  
  public SimpleBoundaryScanner( int maxScan, Character[] boundaryChars ){
    this.maxScan = maxScan;
    this.boundaryChars = new HashSet<Character>();
    this.boundaryChars.addAll(Arrays.asList(boundaryChars));
  }
  
  public SimpleBoundaryScanner( int maxScan, Set<Character> boundaryChars ){
    this.maxScan = maxScan;
    this.boundaryChars = boundaryChars;
  }

  @Override
  public int findStartOffset(StringBuilder buffer, int start) {
    // avoid illegal start offset
    if( start > buffer.length() || start < 1 ) return start;
    int offset, count = maxScan;
    for( offset = start; offset > 0 && count > 0; count-- ){
      // found?
      if( boundaryChars.contains( buffer.charAt( offset - 1 ) ) ) return offset;
      offset--;
    }
    // if we scanned up to the start of the text, return it, its a "boundary"
    if (offset == 0) {
      return 0;
    }
    // not found
    return start;
  }

  @Override
  public int findEndOffset(StringBuilder buffer, int start) {
    // avoid illegal start offset
    if( start > buffer.length() || start < 0 ) return start;
    int offset, count = maxScan;
    //for( offset = start; offset <= buffer.length() && count > 0; count-- ){
    for( offset = start; offset < buffer.length() && count > 0; count-- ){
      // found?
      if( boundaryChars.contains( buffer.charAt( offset ) ) ) return offset;
      offset++;
    }
    // not found
    return start;
  }
}
