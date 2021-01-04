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

import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;

/** An {@link Analyzer} that throws a runtime exception when used for anything. */
final class MissingAnalyzer extends Analyzer {
  @Override
  protected Reader initReader(String fieldName, Reader reader) {
    throw new RuntimeException("Field must have an explicit Analyzer: " + fieldName);
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    throw new RuntimeException("Field must have an explicit Analyzer: " + fieldName);
  }

  @Override
  public int getOffsetGap(String fieldName) {
    return 0;
  }
}
