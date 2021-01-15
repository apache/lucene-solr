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
package org.apache.lucene.search.matchhighlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;

/** An analyzer for tests that has a predefined offset and position gap. */
class AnalyzerWithGaps extends DelegatingAnalyzerWrapper {
  private final Analyzer delegate;
  private final int offsetGap;
  private final int positionGap;

  AnalyzerWithGaps(int offsetGap, int positionGap, Analyzer delegate) {
    super(delegate.getReuseStrategy());
    this.delegate = delegate;
    this.offsetGap = offsetGap;
    this.positionGap = positionGap;
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    return delegate;
  }

  @Override
  public int getOffsetGap(String fieldName) {
    return offsetGap;
  }

  @Override
  public int getPositionIncrementGap(String fieldName) {
    return positionGap;
  }
}
