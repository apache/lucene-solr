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

import org.apache.lucene.codecs.SimpleDVConsumer;
import org.apache.lucene.codecs.SimpleDVProducer;
import org.apache.lucene.codecs.SimpleDocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

public class Lucene41SimpleDocValuesFormat extends SimpleDocValuesFormat {

  public Lucene41SimpleDocValuesFormat() {
    super("Lucene41");
  }

  @Override
  public SimpleDVConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene41SimpleDocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
  }
  
  @Override
  public SimpleDVProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new Lucene41SimpleDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
  }
  
  private static final String DATA_CODEC = "Lucene41DocValuesData";
  private static final String DATA_EXTENSION = "dvd";
  private static final String METADATA_CODEC = "Lucene41DocValuesMetadata";
  private static final String METADATA_EXTENSION = "dvm";
}
