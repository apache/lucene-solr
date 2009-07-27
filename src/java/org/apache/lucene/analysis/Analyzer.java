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

import java.io.Reader;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.store.AlreadyClosedException;

import org.apache.lucene.document.Fieldable;

/** An Analyzer builds TokenStreams, which analyze text.  It thus represents a
 *  policy for extracting index terms from text.
 *  <p>
 *  Typical implementations first build a Tokenizer, which breaks the stream of
 *  characters from the Reader into raw Tokens.  One or more TokenFilters may
 *  then be applied to the output of the Tokenizer.
 */
public abstract class Analyzer {
  /** Creates a TokenStream which tokenizes all the text in the provided
   * Reader.  Must be able to handle null field name for
   * backward compatibility.
   */
  public abstract TokenStream tokenStream(String fieldName, Reader reader);

  /** Creates a TokenStream that is allowed to be re-used
   *  from the previous time that the same thread called
   *  this method.  Callers that do not need to use more
   *  than one TokenStream at the same time from this
   *  analyzer should use this method for better
   *  performance.
   */
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    return tokenStream(fieldName, reader);
  }

  private CloseableThreadLocal tokenStreams = new CloseableThreadLocal();

  /** Used by Analyzers that implement reusableTokenStream
   *  to retrieve previously saved TokenStreams for re-use
   *  by the same thread. */
  protected Object getPreviousTokenStream() {
    try {
      return tokenStreams.get();
    } catch (NullPointerException npe) {
      if (tokenStreams == null) {
        throw new AlreadyClosedException("this Analyzer is closed");
      } else {
        throw npe;
      }
    }
  }

  /** Used by Analyzers that implement reusableTokenStream
   *  to save a TokenStream for later re-use by the same
   *  thread. */
  protected void setPreviousTokenStream(Object obj) {
    try {
      tokenStreams.set(obj);
    } catch (NullPointerException npe) {
      if (tokenStreams == null) {
        throw new AlreadyClosedException("this Analyzer is closed");
      } else {
        throw npe;
      }
    }
  }

  protected boolean overridesTokenStreamMethod;

  /** @deprecated This is only present to preserve
   *  back-compat of classes that subclass a core analyzer
   *  and override tokenStream but not reusableTokenStream */
  protected void setOverridesTokenStreamMethod(Class baseClass) {

    final Class[] params = new Class[2];
    params[0] = String.class;
    params[1] = Reader.class;
    
    try {
      Method m = this.getClass().getMethod("tokenStream", params);
      if (m != null) {
        overridesTokenStreamMethod = m.getDeclaringClass() != baseClass;
      } else {
        overridesTokenStreamMethod = false;
      }
    } catch (NoSuchMethodException nsme) {
      overridesTokenStreamMethod = false;
    }
  }


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

  /**
   * Just like {@link #getPositionIncrementGap}, except for
   * Token offsets instead.  By default this returns 1 for
   * tokenized fields and, as if the fields were joined
   * with an extra space character, and 0 for un-tokenized
   * fields.  This method is only called if the field
   * produced at least one token for indexing.
   *
   * @param field the field just indexed
   * @return offset gap, added to the next token emitted from {@link #tokenStream(String,Reader)}
   */
  public int getOffsetGap(Fieldable field) {
    if (field.isTokenized())
      return 1;
    else
      return 0;
  }

  /** Frees persistent resources used by this Analyzer */
  public void close() {
    tokenStreams.close();
    tokenStreams = null;
  }
}
