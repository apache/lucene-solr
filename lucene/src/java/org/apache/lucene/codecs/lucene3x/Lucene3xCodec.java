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
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.codecs.PerDocProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfosFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40LiveDocsFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.MutableBits;

/**
 * Supports the Lucene 3.x index format (readonly)
 * @deprecated
 */
@Deprecated
public class Lucene3xCodec extends Codec {
  public Lucene3xCodec() {
    super("Lucene3x");
  }

  private final PostingsFormat postingsFormat = new Lucene3xPostingsFormat();
  
  private final StoredFieldsFormat fieldsFormat = new Lucene3xStoredFieldsFormat();
  
  private final TermVectorsFormat vectorsFormat = new Lucene3xTermVectorsFormat();
  
  private final FieldInfosFormat fieldInfosFormat = new Lucene3xFieldInfosFormat();

  private final SegmentInfosFormat infosFormat = new Lucene3xSegmentInfosFormat();
  
  private final Lucene3xNormsFormat normsFormat = new Lucene3xNormsFormat();
  
  /** Extension of compound file for doc store files*/
  static final String COMPOUND_FILE_STORE_EXTENSION = "cfx";
  
  // TODO: this should really be a different impl
  private final LiveDocsFormat liveDocsFormat = new Lucene40LiveDocsFormat() {
    @Override
    public void writeLiveDocs(MutableBits bits, Directory dir, SegmentInfo info, IOContext context) throws IOException {
      throw new UnsupportedOperationException("this codec can only be used for reading");
    }
  };
  
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
    public void files(SegmentInfo info, Set<String> files) throws IOException {}
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
  public Lucene3xNormsFormat normsFormat() {
    return normsFormat;
  }
  
  @Override
  public LiveDocsFormat liveDocsFormat() {
    return liveDocsFormat;
  }
  
  // overrides the default implementation in codec.java to handle CFS without CFE, 
  // shared doc stores, compound doc stores, separate norms, etc
  @Override
  public void files(SegmentInfo info, Set<String> files) throws IOException {
    if (info.getUseCompoundFile()) {
      files.add(IndexFileNames.segmentFileName(info.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION));
    } else {
      postingsFormat().files(info, "", files);
      storedFieldsFormat().files(info, files);
      termVectorsFormat().files(info, files);
      fieldInfosFormat().files(info, files);
      // TODO: segmentInfosFormat should be allowed to declare additional files
      // if it wants, in addition to segments_N
      docValuesFormat().files(info, files);
      normsFormat().files(info, files);
    }
    // never inside CFS
    liveDocsFormat().files(info, files);
    normsFormat().separateFiles(info, files);
    
    // shared docstores: these guys check the hair
    if (info.getDocStoreOffset() != -1) {
      storedFieldsFormat().files(info, files);
      termVectorsFormat().files(info, files);
    }
  }  
}
