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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.ReaderSlice;
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
  public void merge(MergeState mergeState) throws IOException {
    final List<Fields> fields = new ArrayList<>();
    final List<ReaderSlice> slices = new ArrayList<>();

    int docBase = 0;

    for(int readerIndex=0;readerIndex<mergeState.readers.size();readerIndex++) {
      final AtomicReader reader = mergeState.readers.get(readerIndex);
      final Fields f = reader.fields();
      final int maxDoc = reader.maxDoc();
      if (f != null) {
        slices.add(new ReaderSlice(docBase, maxDoc, readerIndex));
        fields.add(f);
      }
      docBase += maxDoc;
    }
    
    final Fields mergedFields = new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
                                                slices.toArray(ReaderSlice.EMPTY_ARRAY));
    
    for (String field : mergedFields) {
      FieldInfo info = mergeState.fieldInfos.fieldInfo(field);
      assert info != null : "FieldInfo for field is null: "+ field;
      Terms terms = mergedFields.terms(field);
      if (terms != null) {
        final TermsConsumer termsConsumer = addField(info);
        termsConsumer.merge(mergeState, info.getIndexOptions(), terms.iterator(null));
      }
    }
  }
}
