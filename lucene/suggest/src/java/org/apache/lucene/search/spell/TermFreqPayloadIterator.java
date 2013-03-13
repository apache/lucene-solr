package org.apache.lucene.search.spell;

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

import org.apache.lucene.search.suggest.Lookup.LookupResult; // javadocs
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester; // javadocs
import org.apache.lucene.search.suggest.analyzing.FuzzySuggester; // javadocs
import org.apache.lucene.util.BytesRef;

/**
 * Interface for enumerating term,weight,payload triples;
 * currently only {@link AnalyzingSuggester} and {@link
 * FuzzySuggester} support payloads.
 */
public interface TermFreqPayloadIterator extends TermFreqIterator {

  /** An arbitrary byte[] to record per suggestion.  See
   *  {@link LookupResult#payload} to retrieve the payload
   *  for each suggestion. */
  public BytesRef payload();
}
