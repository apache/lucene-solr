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
package org.apache.lucene.backward_codecs.lucene84;

import java.util.Objects;
import org.apache.lucene.backward_codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.backward_codecs.lucene60.Lucene60FieldInfosFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60PointsFormat;
import org.apache.lucene.backward_codecs.lucene70.Lucene70SegmentInfoFormat;
import org.apache.lucene.backward_codecs.lucene80.Lucene80NormsFormat;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.VectorFormat;
import org.apache.lucene.codecs.lucene50.Lucene50TermVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/**
 * Implements the Lucene 8.4 index format, with configurable per-field postings and docvalues
 * formats.
 *
 * <p>If you want to reuse functionality of this codec in another codec, extend {@link FilterCodec}.
 *
 * @see org.apache.lucene.backward_codecs.lucene84 package documentation for file format details.
 * @lucene.experimental
 */
public class Lucene84Codec extends Codec {
  private final TermVectorsFormat vectorsFormat = new Lucene50TermVectorsFormat();
  private final FieldInfosFormat fieldInfosFormat = new Lucene60FieldInfosFormat();
  private final SegmentInfoFormat segmentInfosFormat = new Lucene70SegmentInfoFormat();
  private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
  private final CompoundFormat compoundFormat = new Lucene50CompoundFormat();
  private final PostingsFormat defaultFormat;

  private final PostingsFormat postingsFormat =
      new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
          return Lucene84Codec.this.getPostingsFormatForField(field);
        }
      };

  private final DocValuesFormat docValuesFormat =
      new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
          return Lucene84Codec.this.getDocValuesFormatForField(field);
        }
      };

  private final StoredFieldsFormat storedFieldsFormat;

  /** Instantiates a new codec. */
  public Lucene84Codec() {
    this(Mode.BEST_SPEED);
  }

  /**
   * Instantiates a new codec, specifying the stored fields compression mode to use.
   *
   * @param mode stored fields compression mode to use for newly flushed/merged segments.
   */
  public Lucene84Codec(Mode mode) {
    super("Lucene84");
    this.storedFieldsFormat = new Lucene50StoredFieldsFormat(Objects.requireNonNull(mode));
    this.defaultFormat = new Lucene84PostingsFormat();
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return storedFieldsFormat;
  }

  @Override
  public final TermVectorsFormat termVectorsFormat() {
    return vectorsFormat;
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postingsFormat;
  }

  @Override
  public final FieldInfosFormat fieldInfosFormat() {
    return fieldInfosFormat;
  }

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return segmentInfosFormat;
  }

  @Override
  public final LiveDocsFormat liveDocsFormat() {
    return liveDocsFormat;
  }

  @Override
  public CompoundFormat compoundFormat() {
    return compoundFormat;
  }

  @Override
  public PointsFormat pointsFormat() {
    return new Lucene60PointsFormat();
  }

  @Override
  public VectorFormat vectorFormat() {
    return VectorFormat.EMPTY;
  }

  /**
   * Returns the postings format that should be used for writing new segments of <code>field</code>.
   *
   * <p>The default implementation always returns "Lucene84".
   *
   * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
   * future version of Lucene are only guaranteed to be able to read the default implementation.
   */
  public PostingsFormat getPostingsFormatForField(String field) {
    return defaultFormat;
  }

  /**
   * Returns the docvalues format that should be used for writing new segments of <code>field</code>
   * .
   *
   * <p>The default implementation always returns "Lucene80".
   *
   * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
   * future version of Lucene are only guaranteed to be able to read the default implementation.
   */
  public DocValuesFormat getDocValuesFormatForField(String field) {
    return defaultDVFormat;
  }

  @Override
  public final DocValuesFormat docValuesFormat() {
    return docValuesFormat;
  }

  private final DocValuesFormat defaultDVFormat = DocValuesFormat.forName("Lucene80");

  private final NormsFormat normsFormat = new Lucene80NormsFormat();

  @Override
  public NormsFormat normsFormat() {
    return normsFormat;
  }
}
