package org.apache.lucene.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RefCount;

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

/**
 * Manages the {@link DocValuesProducer} held by {@link SegmentReader} and
 * keeps track of their reference counting.
 */
final class SegmentDocValues {

  private final Map<Long,RefCount<DocValuesProducer>> genDVProducers = new HashMap<>();

  private RefCount<DocValuesProducer> newDocValuesProducer(SegmentCommitInfo si, IOContext context, Directory dir,
      DocValuesFormat dvFormat, final Long gen, FieldInfos infos, int termsIndexDivisor) throws IOException {
    Directory dvDir = dir;
    String segmentSuffix = "";
    if (gen.longValue() != -1) {
      dvDir = si.info.dir; // gen'd files are written outside CFS, so use SegInfo directory
      segmentSuffix = Long.toString(gen.longValue(), Character.MAX_RADIX);
    }

    // set SegmentReadState to list only the fields that are relevant to that gen
    SegmentReadState srs = new SegmentReadState(dvDir, si.info, infos, context, termsIndexDivisor, segmentSuffix);
    return new RefCount<DocValuesProducer>(dvFormat.fieldsProducer(srs)) {
      @SuppressWarnings("synthetic-access")
      @Override
      protected void release() throws IOException {
        object.close();
        synchronized (SegmentDocValues.this) {
          genDVProducers.remove(gen);
        }
      }
    };
  }

  /** Returns the {@link DocValuesProducer} for the given generation. */
  synchronized DocValuesProducer getDocValuesProducer(long gen, SegmentCommitInfo si, IOContext context, Directory dir, 
      DocValuesFormat dvFormat, FieldInfos infos, int termsIndexDivisor) throws IOException {
    RefCount<DocValuesProducer> dvp = genDVProducers.get(gen);
    if (dvp == null) {
      dvp = newDocValuesProducer(si, context, dir, dvFormat, gen, infos, termsIndexDivisor);
      assert dvp != null;
      genDVProducers.put(gen, dvp);
    } else {
      dvp.incRef();
    }
    return dvp.get();
  }
  
  /**
   * Decrement the reference count of the given {@link DocValuesProducer}
   * generations. 
   */
  synchronized void decRef(List<Long> dvProducersGens) throws IOException {
    Throwable t = null;
    for (Long gen : dvProducersGens) {
      RefCount<DocValuesProducer> dvp = genDVProducers.get(gen);
      assert dvp != null : "gen=" + gen;
      try {
        dvp.decRef();
      } catch (Throwable th) {
        if (t != null) {
          t = th;
        }
      }
    }
    if (t != null) {
      IOUtils.reThrow(t);
    }
  }
}
