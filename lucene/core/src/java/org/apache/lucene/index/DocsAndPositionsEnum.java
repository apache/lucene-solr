package org.apache.lucene.index;

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

import org.apache.lucene.util.Bits; // javadocs
import org.apache.lucene.util.BytesRef;

/** Also iterates through positions. */
public abstract class DocsAndPositionsEnum extends DocsEnum {
  
  /** Flag to pass to {@link TermsEnum#docsAndPositions(Bits,DocsAndPositionsEnum,int)}
   *  if you require offsets in the returned enum. */
  public static final int FLAG_OFFSETS = 0x1;

  /** Flag to pass to  {@link TermsEnum#docsAndPositions(Bits,DocsAndPositionsEnum,int)}
   *  if you require payloads in the returned enum. */
  public static final int FLAG_PAYLOADS = 0x2;

  /** Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.) */
  protected DocsAndPositionsEnum() {
  }

  /** Returns the next position.  You should only call this
   *  up to {@link DocsEnum#freq()} times else
   *  the behavior is not defined.  If positions were not
   *  indexed this will return -1; this only happens if
   *  offsets were indexed and you passed needsOffset=true
   *  when pulling the enum.  */
  public abstract int nextPosition() throws IOException;

  /** Returns start offset for the current position, or -1
   *  if offsets were not indexed. */
  public abstract int startOffset() throws IOException;

  /** Returns end offset for the current position, or -1 if
   *  offsets were not indexed. */
  public abstract int endOffset() throws IOException;

  /** Returns the payload at this position, or null if no
   *  payload was indexed. You should not modify anything 
   *  (neither members of the returned BytesRef nor bytes 
   *  in the byte[]). */
  public abstract BytesRef getPayload() throws IOException;
}
