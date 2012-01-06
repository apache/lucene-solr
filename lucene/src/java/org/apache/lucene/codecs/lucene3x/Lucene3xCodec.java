package org.apache.lucene.codecs.lucene3x;

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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfosFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40FieldInfosFormat;
import org.apache.lucene.codecs.lucene40.Lucene40SegmentInfosFormat;
import org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40TermVectorsFormat;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
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
  private final StoredFieldsFormat fieldsFormat = new Lucene40StoredFieldsFormat();
  
  // TODO: this should really be a different impl
  private final TermVectorsFormat vectorsFormat = new Lucene40TermVectorsFormat();
  
  // TODO: this should really be a different impl
  private final FieldInfosFormat fieldInfosFormat = new Lucene40FieldInfosFormat();

  // TODO: this should really be a different impl
  // also if we want preflex to *really* be read-only it should throw exception for the writer?
  // this way IR.commit fails on delete/undelete/setNorm/etc ?
  private final SegmentInfosFormat infosFormat = new Lucene40SegmentInfosFormat();
  
  // TODO: this should really be a different impl
  private final NormsFormat normsFormat = new Lucene3xNormsFormat();
  
  // 3.x doesn't support docvalues
  private final DocValuesFormat docValuesFormat = new DocValuesFormat() {
    @Override
    public PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException {
      return null;
    }

    @Override
    public PerDocProducer docsProducer(SegmentReadState state) throws IOException {
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
  public TermVectorsFormat termVectorsFormat() {
    return vectorsFormat;
  }
  
  @Override
  public FieldInfosFormat fieldInfosFormat() {
    return fieldInfosFormat;
  }

  @Override
  public SegmentInfosFormat segmentInfosFormat() {
    return infosFormat;
  }

  @Override
  public NormsFormat normsFormat() {
    return normsFormat;
  }
}
