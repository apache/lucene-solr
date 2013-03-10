package org.apache.lucene.facet.codecs.facet42;

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
 * DocValues format that only handles binary doc values and
 * is optimized for usage with facets.  It uses more RAM than other
 * formats in exchange for faster lookups.
 *
 * <p>
 * <b>NOTE</b>: this format cannot handle more than 2 GB
 * of facet data in a single segment.  If your usage may hit
 * this limit, you can either use Lucene's default
 * DocValuesFormat, limit the maximum segment size in your
 * MergePolicy, or send us a patch fixing the limitation.
 *
 * @lucene.experimental
 */
public final class Facet42DocValuesFormat extends DocValuesFormat {
  
  public static final String CODEC = "FacetsDocValues";
  public static final String EXTENSION = "fdv";
  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;

  public Facet42DocValuesFormat() {
    super("Facet42");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new Facet42DocValuesConsumer(state);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new Facet42DocValuesProducer(state);
  }
  
}
