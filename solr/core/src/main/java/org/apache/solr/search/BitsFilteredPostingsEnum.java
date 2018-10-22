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
package org.apache.solr.search;

import java.io.IOException;

import org.apache.lucene.index.FilterLeafReader.FilterPostingsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.Bits;

public class BitsFilteredPostingsEnum extends FilterPostingsEnum {

  public static PostingsEnum wrap(PostingsEnum in, Bits acceptDocs) {
    if (in == null || acceptDocs == null) {
      return in;
    }
    return new BitsFilteredPostingsEnum(in, acceptDocs);
  }

  private final Bits acceptDocs;

  private BitsFilteredPostingsEnum(PostingsEnum in, Bits acceptDocs) {
    super(in);
    this.acceptDocs = acceptDocs;
  }

  private int doNext(int doc) throws IOException {
    while (doc != NO_MORE_DOCS && acceptDocs.get(doc) == false) {
      doc = super.nextDoc();
    }
    return doc;
  }

  @Override
  public int nextDoc() throws IOException {
    return doNext(super.nextDoc());
  }

  @Override
  public int advance(int target) throws IOException {
    return doNext(super.advance(target));
  }
}
