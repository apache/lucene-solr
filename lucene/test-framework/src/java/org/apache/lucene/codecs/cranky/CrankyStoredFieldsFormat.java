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
package org.apache.lucene.codecs.cranky;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

class CrankyStoredFieldsFormat extends StoredFieldsFormat {
  final StoredFieldsFormat delegate;
  final Random random;
  
  CrankyStoredFieldsFormat(StoredFieldsFormat delegate, Random random) {
    this.delegate = delegate;
    this.random = random;
  }

  @Override
  public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
    return delegate.fieldsReader(directory, si, fn, context);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
    if (random.nextInt(100) == 0) {
      throw new IOException("Fake IOException from StoredFieldsFormat.fieldsWriter()");
    }
    return new CrankyStoredFieldsWriter(delegate.fieldsWriter(directory, si, context), random);
  }
  
  static class CrankyStoredFieldsWriter extends StoredFieldsWriter {
    
    final StoredFieldsWriter delegate;
    final Random random;
    
    CrankyStoredFieldsWriter(StoredFieldsWriter delegate, Random random) {
      this.delegate = delegate;
      this.random = random;
    }

    @Override
    public void finish(FieldInfos fis, int numDocs) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from StoredFieldsWriter.finish()");
      }
      delegate.finish(fis, numDocs);
    }

    @Override
    public int merge(MergeState mergeState) throws IOException {
      if (random.nextInt(100) == 0) {
        throw new IOException("Fake IOException from StoredFieldsWriter.merge()");
      }
      return super.merge(mergeState);
    }
    
    @Override
    public void close() throws IOException {
      delegate.close();
      if (random.nextInt(1000) == 0) {
        throw new IOException("Fake IOException from StoredFieldsWriter.close()");
      }
    }
    
    // per doc/field methods: lower probability since they are invoked so many times.

    @Override
    public void startDocument() throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from StoredFieldsWriter.startDocument()");
      }
      delegate.startDocument();
    }
    
    @Override
    public void finishDocument() throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from StoredFieldsWriter.finishDocument()");
      }
      delegate.finishDocument();
    }

    @Override
    public void writeField(FieldInfo info, IndexableField field) throws IOException {
      if (random.nextInt(10000) == 0) {
        throw new IOException("Fake IOException from StoredFieldsWriter.writeField()");
      }
      delegate.writeField(info, field);
    }
  }
}
