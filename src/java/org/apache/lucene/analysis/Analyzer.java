package org.apache.lucene.analysis;

/**
 * Copyright 2004 The Apache Software Foundation
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

import java.io.Reader;

/** An Analyzer builds TokenStreams, which analyze text.  It thus represents a
 *  policy for extracting index terms from text.
 *  <p>
 *  Typical implementations first build a Tokenizer, which breaks the stream of
 *  characters from the Reader into raw Tokens.  One or more TokenFilters may
 *  then be applied to the output of the Tokenizer.
 *  <p>
 *  WARNING: You must override one of the methods defined by this class in your
 *  subclass or the Analyzer will enter an infinite loop.
 */
public abstract class Analyzer {
  /** Creates a TokenStream which tokenizes all the text in the provided
    Reader.  Default implementation forwards to tokenStream(Reader) for 
    compatibility with older version.  Override to allow Analyzer to choose 
    strategy based on document and/or field.  Must be able to handle null
    field name for backward compatibility. */
  public abstract TokenStream tokenStream(String fieldName, Reader reader);


  /**
   * Invoked before indexing a Fieldable instance if
   * terms have already been added to that field.  This allows custom
   * analyzers to place an automatic position increment gap between
   * Fieldable instances using the same field name.  The default value
   * position increment gap is 0.  With a 0 position increment gap and
   * the typical default token position increment of 1, all terms in a field,
   * including across Fieldable instances, are in successive positions, allowing
   * exact PhraseQuery matches, for instance, across Fieldable instance boundaries.
   *
   * @param fieldName Fieldable name being indexed.
   * @return position increment gap, added to the next token emitted from {@link #tokenStream(String,Reader)}
   */
  public int getPositionIncrementGap(String fieldName)
  {
    return 0;
  }
}

