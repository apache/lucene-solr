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

import org.apache.lucene.backward_codecs.lucene50.Lucene50RWCompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50RWStoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60RWPointsFormat;
import org.apache.lucene.backward_codecs.lucene70.Lucene70RWSegmentInfoFormat;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/** RW impersonation of {@link Lucene84Codec}. */
public class Lucene84RWCodec extends Lucene84Codec {

  private final PostingsFormat defaultPF = new Lucene84RWPostingsFormat();
  private final PostingsFormat postingsFormat =
      new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
          return defaultPF;
        }
      };

  @Override
  public PointsFormat pointsFormat() {
    return new Lucene60RWPointsFormat();
  }

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return new Lucene70RWSegmentInfoFormat();
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return new Lucene50RWStoredFieldsFormat();
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postingsFormat;
  }

  @Override
  public final CompoundFormat compoundFormat() {
    return new Lucene50RWCompoundFormat();
  }
}
