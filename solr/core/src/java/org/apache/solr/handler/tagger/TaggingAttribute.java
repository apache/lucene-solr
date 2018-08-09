/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.tagger;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Attribute;

/**
 * Attribute used by the {@link Tagger} to decide if a token can start a
 * new {@link TagLL tag}.
 * <p>
 * By default this Attribute will return <code>true</code>, but it might be
 * reset by some {@link TokenFilter} added to the {@link TokenStream} used
 * to analyze the parsed text. Typically this will be done based on NLP
 * processing results (e.g. to only lookup Named Entities).
 * <p>
 * NOTE: that all Tokens are used to advance existing {@link TagLL tags}.
 */
public interface TaggingAttribute extends Attribute {

  /**
   * By default this Attribute will be initialised with <code>true</code>.
   * This ensures that all tokens are taggable by default (especially if
   * the {@link TaggingAttribute} is not set by any component in the configured
   * {@link TokenStream}
   */
  public static final boolean DEFAULT_TAGGABLE = true;

  /**
   * Getter for the taggable state of the current Token
   *
   * @return the state
   */
  public boolean isTaggable();

  /**
   * Setter for the taggable state. Typically called by code within
   * {@link TokenFilter#incrementToken()}.
   *
   * @param lookup the state
   */
  public void setTaggable(boolean lookup);

}
