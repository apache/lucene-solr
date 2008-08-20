package org.apache.lucene.analysis;

/**
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This class can be used if the Tokens of a TokenStream
 * are intended to be consumed more than once. It caches
 * all Tokens locally in a List.
 * 
 * CachingTokenFilter implements the optional method
 * {@link TokenStream#reset()}, which repositions the
 * stream to the first Token. 
 *
 */
public class CachingTokenFilter extends TokenFilter {
  private List cache;
  private Iterator iterator;
  
  public CachingTokenFilter(TokenStream input) {
    super(input);
  }
  
  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    if (cache == null) {
      // fill cache lazily
      cache = new LinkedList();
      fillCache(reusableToken);
      iterator = cache.iterator();
    }
    
    if (!iterator.hasNext()) {
      // the cache is exhausted, return null
      return null;
    }
    // Since the TokenFilter can be reset, the tokens need to be preserved as immutable.
    Token nextToken = (Token) iterator.next();
    return (Token) nextToken.clone();
  }
  
  public void reset() throws IOException {
    if(cache != null) {
    	iterator = cache.iterator();
    }
  }
  
  private void fillCache(final Token reusableToken) throws IOException {
    for (Token nextToken = input.next(reusableToken); nextToken != null; nextToken = input.next(reusableToken)) {
      cache.add(nextToken.clone());
    }
  }

}
