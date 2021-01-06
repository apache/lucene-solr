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
package org.apache.lucene.spatial.prefix;

import java.io.IOException;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/**
 * A TokenStream used internally by {@link org.apache.lucene.spatial.prefix.PrefixTreeStrategy}.
 *
 * @lucene.internal
 */
public class BytesRefIteratorTokenStream extends TokenStream {

  public BytesRefIterator getBytesRefIterator() {
    return bytesIter;
  }

  public BytesRefIteratorTokenStream setBytesRefIterator(BytesRefIterator iter) {
    this.bytesIter = iter;
    return this;
  }

  @Override
  public void reset() throws IOException {
    if (bytesIter == null)
      throw new IllegalStateException("call setBytesRefIterator() before usage");
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (bytesIter == null)
      throw new IllegalStateException("call setBytesRefIterator() before usage");

    // get next
    BytesRef bytes = bytesIter.next();
    if (bytes == null) {
      return false;
    } else {
      clearAttributes();
      bytesAtt.setBytesRef(bytes);
      // note: we don't bother setting posInc or type attributes.  There's no point to it.
      return true;
    }
  }

  // members
  private final BytesTermAttribute bytesAtt = addAttribute(BytesTermAttribute.class);

  private BytesRefIterator bytesIter = null; // null means not initialized
}
