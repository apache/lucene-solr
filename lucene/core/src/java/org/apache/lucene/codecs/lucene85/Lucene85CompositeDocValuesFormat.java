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

package org.apache.lucene.codecs.lucene85;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.composite.CompositeDocValuesConsumer;
import org.apache.lucene.codecs.composite.CompositeDocValuesProducer;
import org.apache.lucene.codecs.lucene80.Lucene80BinaryConsumer;
import org.apache.lucene.codecs.lucene80.Lucene80BinaryProducer;
import org.apache.lucene.codecs.lucene80.Lucene80NumericConsumer;
import org.apache.lucene.codecs.lucene80.Lucene80NumericProducer;
import org.apache.lucene.codecs.lucene80.Lucene80SortedSetConsumer;
import org.apache.lucene.codecs.lucene80.Lucene80SortedSetProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import static org.apache.lucene.codecs.composite.CompositeDocValuesConsumer.BinaryConsumer;
import static org.apache.lucene.codecs.composite.CompositeDocValuesConsumer.NumericConsumer;
import static org.apache.lucene.codecs.composite.CompositeDocValuesConsumer.SortedConsumer;
import static org.apache.lucene.codecs.composite.CompositeDocValuesConsumer.SortedNumericConsumer;
import static org.apache.lucene.codecs.composite.CompositeDocValuesConsumer.SortedSetConsumer;

public class Lucene85CompositeDocValuesFormat extends DocValuesFormat {

  public static final String NAME = "Lucene85";
  private static final String EXTENSION = "cdvd";
  private static final int VERSION_START = 0;
  private static final int VERSION_CURRENT = 0;

  public Lucene85CompositeDocValuesFormat() {
    super(NAME);
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new CompositeDocValuesConsumer(state, DocValuesConsumerSupplier.DEFAULT, NAME, EXTENSION, VERSION_CURRENT);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new CompositeDocValuesProducer(state, DocValuesProducerSupplier.DEFAULT, NAME, EXTENSION, VERSION_START, VERSION_CURRENT);
  }

  public static class DocValuesConsumerSupplier implements CompositeDocValuesConsumer.DocValuesConsumerSupplier {

    public static final DocValuesConsumerSupplier DEFAULT = new DocValuesConsumerSupplier();

    @Override
    public BinaryConsumer createBinaryConsumer(SegmentWriteState state) {
      return new Lucene80BinaryConsumer(state);
    }

    @Override
    public SortedConsumer createSortedConsumer(SegmentWriteState state) {
      return new Lucene80SortedSetConsumer(state);
    }

    @Override
    public SortedSetConsumer createSortedSetConsumer(SegmentWriteState state) {
      return new Lucene80SortedSetConsumer(state);
    }

    @Override
    public NumericConsumer createNumericConsumer(SegmentWriteState state) {
      return new Lucene80NumericConsumer(state);
    }

    @Override
    public SortedNumericConsumer createSortedNumericConsumer(SegmentWriteState state) {
      return new Lucene80NumericConsumer(state);
    }
  }

  public static class DocValuesProducerSupplier implements CompositeDocValuesProducer.DocValuesProducerSupplier {

    public static final DocValuesProducerSupplier DEFAULT = new DocValuesProducerSupplier();

    @Override
    public CompositeDocValuesProducer.NumericProducer createNumericProducer(SegmentReadState state) {
      return new Lucene80NumericProducer(state.segmentInfo.maxDoc());
    }

    @Override
    public CompositeDocValuesProducer.BinaryProducer createBinaryProducer(SegmentReadState state) {
      return new Lucene80BinaryProducer(state.segmentInfo.maxDoc());
    }

    @Override
    public CompositeDocValuesProducer.SortedNumericProducer createSortedNumericProducer(SegmentReadState state) {
      return new Lucene80NumericProducer(state.segmentInfo.maxDoc());
    }

    @Override
    public CompositeDocValuesProducer.SortedProducer createSortedProducer(SegmentReadState state) {
      return new Lucene80SortedSetProducer(state.segmentInfo.maxDoc());
    }

    @Override
    public CompositeDocValuesProducer.SortedSetProducer createSortedSetProducer(SegmentReadState state) {
      return new Lucene80SortedSetProducer(state.segmentInfo.maxDoc());
    }
  }
}
