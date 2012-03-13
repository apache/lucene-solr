package org.apache.lucene.codecs.lucene40.values;

/**
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
import java.util.Comparator;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

/**
 * Abstract API for per-document stored primitive values of type <tt>byte[]</tt>
 * , <tt>long</tt> or <tt>double</tt>. The API accepts a single value for each
 * document. The underlying storage mechanism, file formats, data-structures and
 * representations depend on the actual implementation.
 * <p>
 * Document IDs passed to this API must always be increasing unless stated
 * otherwise.
 * </p>
 * 
 * @lucene.experimental
 */
abstract class Writer extends DocValuesConsumer {
  protected final Counter bytesUsed;
  protected Type type;

  /**
   * Creates a new {@link Writer}.
   * 
   * @param bytesUsed
   *          bytes-usage tracking reference used by implementation to track
   *          internally allocated memory. All tracked bytes must be released
   *          once {@link #finish(int)} has been called.
   */
  protected Writer(Counter bytesUsed, Type type) {
    this.bytesUsed = bytesUsed;
    this.type = type;
  }
  
  

  @Override
  protected Type getType() {
    return type;
  }



  /**
   * Factory method to create a {@link Writer} instance for a given type. This
   * method returns default implementations for each of the different types
   * defined in the {@link Type} enumeration.
   * 
   * @param type
   *          the {@link Type} to create the {@link Writer} for
   * @param id
   *          the file name id used to create files within the writer.
   * @param directory
   *          the {@link Directory} to create the files from.
   * @param bytesUsed
   *          a byte-usage tracking reference
   * @param fasterButMoreRam Whether the space used for packed ints should be rounded up for higher lookup performance.
   *                         Currently this parameter only applies for types {@link Type#BYTES_VAR_SORTED}
   *                         and {@link Type#BYTES_FIXED_SORTED}.
   * @return a new {@link Writer} instance for the given {@link Type}
   * @throws IOException
   */
  public static DocValuesConsumer create(Type type, String id, Directory directory,
      Comparator<BytesRef> comp, Counter bytesUsed, IOContext context, boolean fasterButMoreRam) throws IOException {
    if (comp == null) {
      comp = BytesRef.getUTF8SortedAsUnicodeComparator();
    }
    switch (type) {
    case FIXED_INTS_16:
    case FIXED_INTS_32:
    case FIXED_INTS_64:
    case FIXED_INTS_8:
    case VAR_INTS:
      return Ints.getWriter(directory, id, bytesUsed, type, context);
    case FLOAT_32:
      return Floats.getWriter(directory, id, bytesUsed, context, type);
    case FLOAT_64:
      return Floats.getWriter(directory, id, bytesUsed, context, type);
    case BYTES_FIXED_STRAIGHT:
      return Bytes.getWriter(directory, id, Bytes.Mode.STRAIGHT, true, comp,
          bytesUsed, context, fasterButMoreRam);
    case BYTES_FIXED_DEREF:
      return Bytes.getWriter(directory, id, Bytes.Mode.DEREF, true, comp,
          bytesUsed, context, fasterButMoreRam);
    case BYTES_FIXED_SORTED:
      return Bytes.getWriter(directory, id, Bytes.Mode.SORTED, true, comp,
          bytesUsed, context, fasterButMoreRam);
    case BYTES_VAR_STRAIGHT:
      return Bytes.getWriter(directory, id, Bytes.Mode.STRAIGHT, false, comp,
          bytesUsed, context, fasterButMoreRam);
    case BYTES_VAR_DEREF:
      return Bytes.getWriter(directory, id, Bytes.Mode.DEREF, false, comp,
          bytesUsed, context, fasterButMoreRam);
    case BYTES_VAR_SORTED:
      return Bytes.getWriter(directory, id, Bytes.Mode.SORTED, false, comp,
          bytesUsed, context, fasterButMoreRam);
    default:
      throw new IllegalArgumentException("Unknown Values: " + type);
    }
  }
}
