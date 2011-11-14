package org.apache.lucene.index.codecs.lucene3x;

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
import java.util.Set;

import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.DefaultStoredFieldsFormat;
import org.apache.lucene.index.codecs.DefaultSegmentInfosFormat;
import org.apache.lucene.index.codecs.DocValuesFormat;
import org.apache.lucene.index.codecs.StoredFieldsFormat;
import org.apache.lucene.index.codecs.PerDocConsumer;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.SegmentInfosFormat;
import org.apache.lucene.store.Directory;

/**
 * Supports the Lucene 3.x index format (readonly)
 */
public class Lucene3xCodec extends Codec {
  public Lucene3xCodec() {
    super("Lucene3x");
  }

  private final PostingsFormat postingsFormat = new Lucene3xPostingsFormat();
  
  // TODO: this should really be a different impl
  private final StoredFieldsFormat fieldsFormat = new DefaultStoredFieldsFormat();
  
  // TODO: this should really be a different impl
  // also if we want preflex to *really* be read-only it should throw exception for the writer?
  // this way IR.commit fails on delete/undelete/setNorm/etc ?
  private final SegmentInfosFormat infosFormat = new DefaultSegmentInfosFormat();
  
  // 3.x doesn't support docvalues
  private final DocValuesFormat docValuesFormat = new DocValuesFormat() {
    @Override
    public PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException {
      return null;
    }

    @Override
    public PerDocValues docsProducer(SegmentReadState state) throws IOException {
      return null;
    }

    @Override
    public void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException {}
  };
  
  @Override
  public PostingsFormat postingsFormat() {
    return postingsFormat;
  }
  
  @Override
  public DocValuesFormat docValuesFormat() {
    return docValuesFormat;
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return fieldsFormat;
  }

  @Override
  public SegmentInfosFormat segmentInfosFormat() {
    return infosFormat;
  }
}
