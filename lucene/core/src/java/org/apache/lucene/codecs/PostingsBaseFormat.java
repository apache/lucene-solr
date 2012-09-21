package org.apache.lucene.codecs;

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

import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;

/** 
 * Provides a {@link PostingsReaderBase} and {@link
 * PostingsWriterBase}.
 *
 * @lucene.experimental */

// TODO: find a better name; this defines the API that the
// terms dict impls use to talk to a postings impl.
// TermsDict + PostingsReader/WriterBase == PostingsConsumer/Producer

// can we clean this up and do this some other way? 
// refactor some of these classes and use covariant return?
public abstract class PostingsBaseFormat {

  /** Unique name that's used to retrieve this codec when
   *  reading the index */
  public final String name;
  
  /** Sole constructor. */
  protected PostingsBaseFormat(String name) {
    this.name = name;
  }

  /** Creates the {@link PostingsReaderBase} for this
   *  format. */
  public abstract PostingsReaderBase postingsReaderBase(SegmentReadState state) throws IOException;

  /** Creates the {@link PostingsWriterBase} for this
   *  format. */
  public abstract PostingsWriterBase postingsWriterBase(SegmentWriteState state) throws IOException;
}
