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
package org.apache.lucene.backward_codecs.lucene86;

import org.apache.lucene.backward_codecs.lucene50.Lucene50RWCompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50RWStoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene80.Lucene80RWNormsFormat;
import org.apache.lucene.backward_codecs.lucene84.Lucene84RWPostingsFormat;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/** RW impersonation of {@link Lucene86Codec}. */
public class Lucene86RWCodec extends Lucene86Codec {

  private final StoredFieldsFormat storedFieldsFormat;
  private final PostingsFormat defaultPF = new Lucene84RWPostingsFormat();
  private final PostingsFormat postingsFormat =
      new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
          return defaultPF;
        }
      };

  /** No arguments constructor. */
  public Lucene86RWCodec() {
    storedFieldsFormat = new Lucene50RWStoredFieldsFormat();
  }

  /** Constructor that takes a mode. */
  public Lucene86RWCodec(Lucene50StoredFieldsFormat.Mode mode) {
    storedFieldsFormat = new Lucene50RWStoredFieldsFormat(mode);
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return storedFieldsFormat;
  }

  @Override
  public NormsFormat normsFormat() {
    return new Lucene80RWNormsFormat();
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
