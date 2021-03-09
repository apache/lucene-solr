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
package org.apache.lucene.codecs.lucene90;

import java.util.Objects;
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
import org.apache.lucene.codecs.lucene86.Lucene86PointsFormat;
import org.apache.lucene.codecs.lucene86.Lucene86SegmentInfoFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/**
 * Implements the Lucene 9.0 index format
 *
 * <p>If you want to reuse functionality of this codec in another codec, extend {@link FilterCodec}.
 *
 * @see org.apache.lucene.codecs.lucene90 package documentation for file format details.
 * @lucene.experimental
 */
public class Lucene90Codec extends Codec {

  /** Configuration option for the codec. */
  public static enum Mode {
    /** Trade compression ratio for retrieval speed. */
    BEST_SPEED(Lucene90StoredFieldsFormat.Mode.BEST_SPEED, Lucene90DocValuesFormat.Mode.BEST_SPEED),
    /** Trade retrieval speed for compression ratio. */
    BEST_COMPRESSION(
        Lucene90StoredFieldsFormat.Mode.BEST_COMPRESSION,
        Lucene90DocValuesFormat.Mode.BEST_COMPRESSION);

    private final Lucene90StoredFieldsFormat.Mode storedMode;
    private final Lucene90DocValuesFormat.Mode dvMode;

    private Mode(Lucene90StoredFieldsFormat.Mode storedMode, Lucene90DocValuesFormat.Mode dvMode) {
      this.storedMode = Objects.requireNonNull(storedMode);
      this.dvMode = Objects.requireNonNull(dvMode);
    }
  }

  private final TermVectorsFormat vectorsFormat = new Lucene90TermVectorsFormat();
  private final FieldInfosFormat fieldInfosFormat = new Lucene90FieldInfosFormat();
  private final SegmentInfoFormat segmentInfosFormat = new Lucene86SegmentInfoFormat();
  private final LiveDocsFormat liveDocsFormat = new Lucene90LiveDocsFormat();
  private final CompoundFormat compoundFormat = new Lucene90CompoundFormat();
  private final PostingsFormat defaultFormat;

  private final PostingsFormat postingsFormat =
      new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
          return Lucene90Codec.this.getPostingsFormatForField(field);
        }
      };

  private final DocValuesFormat docValuesFormat =
      new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
          return Lucene90Codec.this.getDocValuesFormatForField(field);
        }
      };

  private final VectorFormat vectorFormat = new Lucene90VectorFormat();

  private final StoredFieldsFormat storedFieldsFormat;

  /** Instantiates a new codec. */
  public Lucene90Codec() {
    this(Mode.BEST_SPEED);
  }

  /**
   * Instantiates a new codec, specifying the stored fields compression mode to use.
   *
   * @param mode stored fields compression mode to use for newly flushed/merged segments.
   */
  public Lucene90Codec(Mode mode) {
    super("Lucene90");
    this.storedFieldsFormat =
        new Lucene90StoredFieldsFormat(Objects.requireNonNull(mode).storedMode);
    this.defaultFormat = new Lucene90PostingsFormat();
    this.defaultDVFormat = new Lucene90DocValuesFormat(mode.dvMode);
  }

  @Override
  public final StoredFieldsFormat storedFieldsFormat() {
    return storedFieldsFormat;
  }

  @Override
  public final TermVectorsFormat termVectorsFormat() {
    return vectorsFormat;
  }

  @Override
  public final PostingsFormat postingsFormat() {
    return postingsFormat;
  }

  @Override
  public final FieldInfosFormat fieldInfosFormat() {
    return fieldInfosFormat;
  }

  @Override
  public final SegmentInfoFormat segmentInfoFormat() {
    return segmentInfosFormat;
  }

  @Override
  public final LiveDocsFormat liveDocsFormat() {
    return liveDocsFormat;
  }

  @Override
  public final CompoundFormat compoundFormat() {
    return compoundFormat;
  }

  @Override
  public final PointsFormat pointsFormat() {
    return new Lucene86PointsFormat();
  }

  @Override
  public final VectorFormat vectorFormat() {
    return vectorFormat;
  }

  /**
   * Returns the postings format that should be used for writing new segments of <code>field</code>.
   *
   * <p>The default implementation always returns "Lucene90".
   *
   * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
   * future version of Lucene are only guaranteed to be able to read the default implementation,
   */
  public PostingsFormat getPostingsFormatForField(String field) {
    return defaultFormat;
  }

  /**
   * Returns the docvalues format that should be used for writing new segments of <code>field</code>
   * .
   *
   * <p>The default implementation always returns "Lucene90".
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

  private final DocValuesFormat defaultDVFormat;

  private final NormsFormat normsFormat = new Lucene90NormsFormat();

  @Override
  public final NormsFormat normsFormat() {
    return normsFormat;
  }
}
