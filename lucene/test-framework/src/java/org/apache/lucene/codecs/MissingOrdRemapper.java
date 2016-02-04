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
package org.apache.lucene.codecs;

import java.util.Iterator;

import org.apache.lucene.util.BytesRef;

/** 
 * a utility class to write missing values for SORTED as if they were the empty string
 * (to simulate pre-Lucene4.5 dv behavior for testing old codecs)
 */
public class MissingOrdRemapper {
  
  /** insert an empty byte[] to the front of this iterable */
  public static Iterable<BytesRef> insertEmptyValue(final Iterable<BytesRef> iterable) {
    return new Iterable<BytesRef>() {
      @Override
      public Iterator<BytesRef> iterator() {
        return new Iterator<BytesRef>() {
          boolean seenEmpty = false;
          Iterator<BytesRef> in = iterable.iterator();
          
          @Override
          public boolean hasNext() {
            return !seenEmpty || in.hasNext();
          }

          @Override
          public BytesRef next() {
            if (!seenEmpty) {
              seenEmpty = true;
              return new BytesRef();
            } else {
              return in.next();
            }
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
  
  /** remaps ord -1 to ord 0 on this iterable. */
  public static Iterable<Number> mapMissingToOrd0(final Iterable<Number> iterable) {
    return new Iterable<Number>() {
      @Override
      public Iterator<Number> iterator() {
        return new Iterator<Number>() {
          Iterator<Number> in = iterable.iterator();
          
          @Override
          public boolean hasNext() {
            return in.hasNext();
          }

          @Override
          public Number next() {
            Number n = in.next();
            if (n.longValue() == -1) {
              return 0;
            } else {
              return n;
            }
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
  
  /** remaps every ord+1 on this iterable */
  public static Iterable<Number> mapAllOrds(final Iterable<Number> iterable) {
    return new Iterable<Number>() {
      @Override
      public Iterator<Number> iterator() {
        return new Iterator<Number>() {
          Iterator<Number> in = iterable.iterator();
          
          @Override
          public boolean hasNext() {
            return in.hasNext();
          }

          @Override
          public Number next() {
            Number n = in.next();
            return n.longValue()+1;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
}
