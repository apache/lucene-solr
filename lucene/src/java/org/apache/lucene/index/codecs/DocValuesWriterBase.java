package org.apache.lucene.index.codecs;

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

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.values.Writer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

/**
 * Abstract base class for PerDocConsumer implementations
 * @lucene.experimental
 */
public abstract class DocValuesWriterBase extends PerDocConsumer {
  private final String segmentName;
  private final int codecId;
  private final Counter bytesUsed;
  private final IOContext context;
  
  protected DocValuesWriterBase(PerDocWriteState state) {
    this.segmentName = state.segmentName;
    this.codecId = state.codecId;
    this.bytesUsed = state.bytesUsed;
    this.context = state.context;
  }

  protected abstract Directory getDirectory();
  
  @Override
  public void close() throws IOException {   
  }

  @Override
  public DocValuesConsumer addValuesField(FieldInfo field) throws IOException {
    return Writer.create(field.getDocValues(),
        docValuesId(segmentName, codecId, field.number), 
        getDirectory(), getComparator(), bytesUsed, context);
  }

  public static String docValuesId(String segmentsName, int codecID, int fieldId) {
    return segmentsName + "_" + codecID + "-" + fieldId;
  }
  
  
  public Comparator<BytesRef> getComparator() throws IOException {
    return BytesRef.getUTF8SortedAsUnicodeComparator();
  }
}
