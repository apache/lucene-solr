package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.util.Bits;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * Codec API for writing stored fields:
 * <p>
 * <ol>
 *   <li>For every document, {@link #startDocument(int)} is called,
 *       informing the Codec how many fields will be written.
 *   <li>{@link #writeField(FieldInfo, IndexableField)} is called for 
 *       each field in the document.
 *   <li>After all documents have been written, {@link #finish(int)} 
 *       is called for verification/sanity-checks.
 *   <li>Finally the writer is closed ({@link #close()})
 * </ol>
 * 
 * @lucene.experimental
 */
public abstract class StoredFieldsWriter implements Closeable {
  
  /** Called before writing the stored fields of the document.
   *  {@link #writeField(FieldInfo, IndexableField)} will be called
   *  <code>numStoredFields</code> times. Note that this is
   *  called even if the document has no stored fields, in
   *  this case <code>numStoredFields</code> will be zero. */
  public abstract void startDocument(int numStoredFields) throws IOException;
  
  /** Writes a single stored field. */
  public abstract void writeField(FieldInfo info, IndexableField field) throws IOException;

  /** Aborts writing entirely, implementation should remove
   *  any partially-written files, etc. */
  public abstract void abort();
  
  /** Called before {@link #close()}, passing in the number
   *  of documents that were written. Note that this is 
   *  intentionally redundant (equivalent to the number of
   *  calls to {@link #startDocument(int)}, but a Codec should
   *  check that this is the case to detect the JRE bug described 
   *  in LUCENE-1282. */
  public abstract void finish(int numDocs) throws IOException;
  
  /** Merges in the stored fields from the readers in 
   *  <code>mergeState</code>. The default implementation skips
   *  over deleted documents, and uses {@link #startDocument(int)},
   *  {@link #writeField(FieldInfo, IndexableField)}, and {@link #finish(int)},
   *  returning the number of documents that were written.
   *  Implementations can override this method for more sophisticated
   *  merging (bulk-byte copying, etc). */
  public int merge(MergeState mergeState) throws IOException {
    int docCount = 0;
    for (MergeState.IndexReaderAndLiveDocs reader : mergeState.readers) {
      final int maxDoc = reader.reader.maxDoc();
      final Bits liveDocs = reader.liveDocs;
      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs != null && !liveDocs.get(i)) {
          // skip deleted docs
          continue;
        }
        // TODO: this could be more efficient using
        // FieldVisitor instead of loading/writing entire
        // doc; ie we just have to renumber the field number
        // on the fly?
        // NOTE: it's very important to first assign to doc then pass it to
        // fieldsWriter.addDocument; see LUCENE-1282
        Document doc = reader.reader.document(i);
        addDocument(doc, mergeState.fieldInfos);
        docCount++;
        mergeState.checkAbort.work(300);
      }
    }
    finish(docCount);
    return docCount;
  }
  
  /** sugar method for startDocument() + writeField() for every stored field in the document */
  protected final void addDocument(Iterable<? extends IndexableField> doc, FieldInfos fieldInfos) throws IOException {
    int storedCount = 0;
    for (IndexableField field : doc) {
      if (field.fieldType().stored()) {
        storedCount++;
      }
    }
    
    startDocument(storedCount);

    for (IndexableField field : doc) {
      if (field.fieldType().stored()) {
        writeField(fieldInfos.fieldInfo(field.name()), field);
      }
    }
  }
}
