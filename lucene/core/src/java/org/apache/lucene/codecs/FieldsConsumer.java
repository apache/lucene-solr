package org.apache.lucene.codecs;

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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Terms;

/** Abstract API that consumes terms, doc, freq, prox, offset and
 *  payloads postings.  Concrete implementations of this
 *  actually do "something" with the postings (write it into
 *  the index in a specific format).
 *
 * @lucene.experimental
 */
public abstract class FieldsConsumer implements Closeable {

  /** Add a new field */
  public abstract TermsConsumer addField(FieldInfo field) throws IOException;
  
  /** Called when we are done adding everything. */
  public abstract void close() throws IOException;

  public void merge(MergeState mergeState, Fields fields) throws IOException {
    FieldsEnum fieldsEnum = fields.iterator();
    assert fieldsEnum != null;
    String field;
    while((field = fieldsEnum.next()) != null) {
      mergeState.fieldInfo = mergeState.fieldInfos.fieldInfo(field);
      assert mergeState.fieldInfo != null : "FieldInfo for field is null: "+ field;
      Terms terms = fieldsEnum.terms();
      if (terms != null) {
        final TermsConsumer termsConsumer = addField(mergeState.fieldInfo);
        termsConsumer.merge(mergeState, terms.iterator(null));
      }
    }
  }
}
