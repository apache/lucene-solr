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
package org.apache.lucene.index;

import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.util.CloseableThreadLocal;

/**
 * {@link CodecReader} wrapper that performs all reads using the merging
 * instance of the index formats.
 */
public class MergingCodecReader extends FilterCodecReader {

  private final CloseableThreadLocal<StoredFieldsReader> fieldsReader = new CloseableThreadLocal<StoredFieldsReader>() {
    @Override
    protected StoredFieldsReader initialValue() {
      return in.getFieldsReader().getMergeInstance();
    }
  };
  private final CloseableThreadLocal<NormsProducer> normsReader = new CloseableThreadLocal<NormsProducer>() {
    @Override
    protected NormsProducer initialValue() {
      NormsProducer norms = in.getNormsReader();
      if (norms == null) {
        return null;
      } else {
        return norms.getMergeInstance();
      }
    }
  };
  // TODO: other formats too

  /** Wrap the given instance. */
  public MergingCodecReader(CodecReader in) {
    super(in);
  }

  @Override
  public StoredFieldsReader getFieldsReader() {
    return fieldsReader.get();
  }

  @Override
  public NormsProducer getNormsReader() {
    return normsReader.get();
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    // same content, we can delegate
    return in.getCoreCacheHelper();
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    // same content, we can delegate
    return in.getReaderCacheHelper();
  }

}
