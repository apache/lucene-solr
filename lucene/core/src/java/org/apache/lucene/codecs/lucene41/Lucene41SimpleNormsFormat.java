package org.apache.lucene.codecs.lucene41;

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

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

public class Lucene41SimpleNormsFormat extends NormsFormat {

  @Override
  public DocValuesConsumer normsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene41SimpleDocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
  }
  
  @Override
  public DocValuesProducer normsProducer(SegmentReadState state) throws IOException {
    return new Lucene41SimpleDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
  }
  
  private static final String DATA_CODEC = "Lucene41NormsData";
  private static final String DATA_EXTENSION = "nvd";
  private static final String METADATA_CODEC = "Lucene41NormsMetadata";
  private static final String METADATA_EXTENSION = "nvm";
}
