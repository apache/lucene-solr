package org.apache.lucene.codecs;

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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState; // javadocs
import org.apache.lucene.index.Terms;

/** 
 * Abstract API that consumes terms, doc, freq, prox, offset and
 * payloads postings.  Concrete implementations of this
 * actually do "something" with the postings (write it into
 * the index in a specific format).
 * <p>
 * The lifecycle is:
 * <ol>
 *   <li>FieldsConsumer is created by 
 *       {@link PostingsFormat#fieldsConsumer(SegmentWriteState)}.
 *   <li>For each field, {@link #addField(FieldInfo)} is called,
 *       returning a {@link TermsConsumer} for the field.
 *   <li>After all fields are added, the consumer is {@link #close}d.
 * </ol>
 *
 * @lucene.experimental
 */
public abstract class FieldsConsumer implements Closeable {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected FieldsConsumer() {
  }

  /** Add a new field */
  public abstract TermsConsumer addField(FieldInfo field) throws IOException;
  
  /** Called when we are done adding everything. */
  @Override
  public abstract void close() throws IOException;

  /** Called during merging to merge all {@link Fields} from
   *  sub-readers.  This must recurse to merge all postings
   *  (terms, docs, positions, etc.).  A {@link
   *  PostingsFormat} can override this default
   *  implementation to do its own merging. */
  public void merge(MergeState mergeState, Fields fields) throws IOException {
    for (String field : fields) {
      FieldInfo info = mergeState.fieldInfos.fieldInfo(field);
      assert info != null : "FieldInfo for field is null: "+ field;
      Terms terms = fields.terms(field);
      if (terms != null) {
        final TermsConsumer termsConsumer = addField(info);
        termsConsumer.merge(mergeState, info.getIndexOptions(), terms.iterator(null));
      }
    }
  }
}
