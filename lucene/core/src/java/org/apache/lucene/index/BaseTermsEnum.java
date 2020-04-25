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

package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

/**
 * A base TermsEnum that adds default implementations for
 * <ul>
 *   <li>{@link #attributes()}</li>
 *   <li>{@link #termState()}</li>
 *   <li>{@link #seekExact(BytesRef)}</li>
 *   <li>{@link #seekExact(BytesRef, TermState)}</li>
 * </ul>
 *
 * In some cases, the default implementation may be slow and consume huge memory, so subclass SHOULD have its own
 * implementation if possible.
 */
public abstract class BaseTermsEnum extends TermsEnum {

  private AttributeSource atts = null;
  
  /** Sole constructor. (For invocation by subclass
   *  constructors, typically implicit.) */
  protected BaseTermsEnum() {
    super();
  }

  @Override
  public TermState termState() throws IOException {
    return new TermState() {
      @Override
      public void copyFrom(TermState other) {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public boolean seekExact(BytesRef text) throws IOException {
    return seekCeil(text) == SeekStatus.FOUND;
  }

  @Override
  public void seekExact(BytesRef term, TermState state) throws IOException {
    if (!seekExact(term)) {
      throw new IllegalArgumentException("term=" + term + " does not exist");
    }
  }

  public AttributeSource attributes() {
    if (atts == null) {
      atts = new AttributeSource();
    }
    return atts;
  }
}
