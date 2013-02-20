package org.apache.lucene.codecs.diskdv;

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
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * DocValues format that keeps most things on disk.
 * <p>
 * Things like ordinals and disk offsets are loaded into ram,
 * for single-seek access to all the types.
 * <p>
 * @lucene.experimental
 */
public final class DiskDocValuesFormat extends DocValuesFormat {

  public DiskDocValuesFormat() {
    super("Disk");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new DiskDocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new DiskDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }
  
  public static final String DATA_CODEC = "DiskDocValuesData";
  public static final String DATA_EXTENSION = "dvdd";
  public static final String META_CODEC = "DiskDocValuesMetadata";
  public static final String META_EXTENSION = "dvdm";
  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;
  public static final byte NUMERIC = 0;
  public static final byte BINARY = 1;
  public static final byte SORTED = 2;
  public static final byte SORTED_SET = 3;
}
