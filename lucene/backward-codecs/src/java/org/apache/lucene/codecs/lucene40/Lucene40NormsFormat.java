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
package org.apache.lucene.codecs.lucene40;

import java.io.IOException;

import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.UndeadNormsProducer;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Lucene 4.0 Norms Format.
 * @deprecated Only for reading old 4.0 and 4.1 segments
 */
@Deprecated
public class Lucene40NormsFormat extends NormsFormat {

  /** Sole constructor. */
  public Lucene40NormsFormat() {}
  
  @Override
  public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }

  @Override
  public NormsProducer normsProducer(SegmentReadState state) throws IOException {
    String filename = IndexFileNames.segmentFileName(state.segmentInfo.name, 
                                                     "nrm", 
                                                     Lucene40CompoundFormat.COMPOUND_FILE_EXTENSION);
    if (UndeadNormsProducer.isUndeadArmy(state.fieldInfos)) {
      return UndeadNormsProducer.INSTANCE;
    } else {
      return new Lucene40NormsReader(state, filename);
    }
  }
}
