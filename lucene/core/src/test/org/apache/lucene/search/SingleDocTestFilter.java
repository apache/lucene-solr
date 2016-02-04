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
package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;

@Deprecated
public class SingleDocTestFilter extends Filter {
  private int doc;

  public SingleDocTestFilter(int doc) {
    this.doc = doc;
  }

  @Override
  public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
    FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
    bits.set(doc);
    if (acceptDocs != null && !acceptDocs.get(doc)) bits.clear(doc);
    return new BitDocIdSet(bits);
  }

  @Override
  public String toString(String field) {
    return "SingleDocTestFilter(" + doc + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (super.equals(obj) == false) {
      return false;
    }
    return doc == ((SingleDocTestFilter) obj).doc;
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + doc;
  }
}
