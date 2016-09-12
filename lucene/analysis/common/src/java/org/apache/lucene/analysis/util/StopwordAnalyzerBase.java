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
package org.apache.lucene.analysis.util;

/**
 * Base class for Analyzers that need to make use of stopword sets. 
 * @deprecated This class moved to Lucene-Core module:
 *  {@link org.apache.lucene.analysis.StopwordAnalyzerBase}
 */
@Deprecated
public abstract class StopwordAnalyzerBase extends org.apache.lucene.analysis.StopwordAnalyzerBase {

  /**
   * Creates a new instance initialized with the given stopword set
   * 
   * @param stopwords
   *          the analyzer's stopword set
   */
  protected StopwordAnalyzerBase(final CharArraySet stopwords) {
    super(stopwords);
  }

  /**
   * Creates a new Analyzer with an empty stopword set
   */
  protected StopwordAnalyzerBase() {
    super();
  }

  @Override
  public CharArraySet getStopwordSet() {
    // this cast should always work, because the stop set is final!
    return (CharArraySet) super.getStopwordSet();
  }

}
