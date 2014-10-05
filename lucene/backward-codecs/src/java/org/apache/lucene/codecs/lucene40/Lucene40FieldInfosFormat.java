package org.apache.lucene.codecs.lucene40;

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

import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.codecs.FieldInfosWriter;

/**
 * Lucene 4.0 Field Infos format.
 * @deprecated Only for reading old 4.0 and 4.1 segments
 */
@Deprecated
public class Lucene40FieldInfosFormat extends FieldInfosFormat {
  private final FieldInfosReader reader = new Lucene40FieldInfosReader();
  
  /** Sole constructor. */
  public Lucene40FieldInfosFormat() {
  }

  @Override
  public final FieldInfosReader getFieldInfosReader() throws IOException {
    return reader;
  }

  @Override
  public FieldInfosWriter getFieldInfosWriter() throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }
  
  /** Extension of field infos */
  static final String FIELD_INFOS_EXTENSION = "fnm";
  
  static final String CODEC_NAME = "Lucene40FieldInfos";
  static final int FORMAT_START = 0;
  static final int FORMAT_CURRENT = FORMAT_START;
  
  static final byte IS_INDEXED = 0x1;
  static final byte STORE_TERMVECTOR = 0x2;
  static final byte STORE_OFFSETS_IN_POSTINGS = 0x4;
  static final byte OMIT_NORMS = 0x10;
  static final byte STORE_PAYLOADS = 0x20;
  static final byte OMIT_TERM_FREQ_AND_POSITIONS = 0x40;
  static final byte OMIT_POSITIONS = -128;
}
