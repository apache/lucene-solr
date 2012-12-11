package org.apache.lucene.search.highlight;

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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

/**
 * {@link Fragmenter} implementation which breaks text up into same-size
 * fragments with no concerns over spotting sentence boundaries.
 */
public class SimpleFragmenter implements Fragmenter {
  private static final int DEFAULT_FRAGMENT_SIZE = 100;
  private int currentNumFrags;
  private int fragmentSize;
  private OffsetAttribute offsetAtt;

  public SimpleFragmenter() {
    this(DEFAULT_FRAGMENT_SIZE);
  }

  /**
   * 
   * @param fragmentSize size in number of characters of each fragment
   */
  public SimpleFragmenter(int fragmentSize) {
    this.fragmentSize = fragmentSize;
  }


  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.Fragmenter#start(java.lang.String, org.apache.lucene.analysis.TokenStream)
   */
  @Override
  public void start(String originalText, TokenStream stream) {
    offsetAtt = stream.addAttribute(OffsetAttribute.class);
    currentNumFrags = 1;
  }


  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.Fragmenter#isNewFragment()
   */
  @Override
  public boolean isNewFragment() {
    boolean isNewFrag = offsetAtt.endOffset() >= (fragmentSize * currentNumFrags);
    if (isNewFrag) {
      currentNumFrags++;
    }
    return isNewFrag;
  }

  /**
   * @return size in number of characters of each fragment
   */
  public int getFragmentSize() {
    return fragmentSize;
  }

  /**
   * @param size size in characters of each fragment
   */
  public void setFragmentSize(int size) {
    fragmentSize = size;
  }

}
