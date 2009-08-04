package org.apache.lucene.search.highlight;

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

import org.apache.lucene.analysis.TokenStream;

/**
 * Adds to the score for a fragment based on its tokens
 */
public interface Scorer {

  /**
   * Called to init the Scorer with a TokenStream. You can grab references to
   * the attributes you are interested in here and access them from
   * getTokenScore().
   * 
   * @param tokenStream
   * @throws IOException 
   */
  public TokenStream init(TokenStream tokenStream) throws IOException;

  /**
   * called when a new fragment is started for consideration
   * 
   * @param newFragment
   */
  public void startFragment(TextFragment newFragment);

  /**
   * Called for each token in the current fragment. The Highlighter will
   * increment the TokenStream passed to init on every call.
   * 
   * @return a score which is passed to the Highlighter class to influence the
   *         mark-up of the text (this return value is NOT used to score the
   *         fragment)
   */
  public float getTokenScore();

  /**
   * Called when the highlighter has no more tokens for the current fragment -
   * the scorer returns the weighting it has derived for the most recent
   * fragment, typically based on the tokens passed to getTokenScore().
   * 
   */
  public float getFragmentScore();

}
