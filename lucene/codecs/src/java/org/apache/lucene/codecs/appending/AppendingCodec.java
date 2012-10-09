package org.apache.lucene.codecs.appending;

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

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;

/**
 * This codec uses an index format that is very similar to
 * {@link Lucene40Codec} but works on append-only outputs, such as plain output
 * streams and append-only filesystems.
 *
 * @lucene.experimental
 * @deprecated This codec is read-only: as the functionality has been folded
 * into the default codec. Its only for convenience to read old segments.
 */
@Deprecated
public class AppendingCodec extends FilterCodec {

  public AppendingCodec() {
    super("Appending", new Lucene40Codec());
  }

  private final PostingsFormat postings = new AppendingPostingsFormat();

  @Override
  public PostingsFormat postingsFormat() {
    return postings;
  }

}
