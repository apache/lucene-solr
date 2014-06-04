package org.apache.lucene.util.packed;

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

import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;

/** 
 * Retrieves an instance previously written by {@link DirectWriter} 
 * <p>
 * Example usage:
 * <pre class="prettyprint">
 *   int bitsPerValue = 100;
 *   IndexInput in = dir.openInput("packed", IOContext.DEFAULT);
 *   LongValues values = DirectReader.getInstance(in.randomAccessSlice(start, end), bitsPerValue);
 *   for (int i = 0; i < numValues; i++) {
 *     long value = values.get(i);
 *   }
 * </pre>
 * @see DirectWriter
 */
public class DirectReader {
  
  /** 
   * Retrieves an instance from the specified slice written decoding
   * {@code bitsPerValue} for each value 
   */
  public static LongValues getInstance(RandomAccessInput slice, int bitsPerValue) {
    switch (bitsPerValue) {
      case 1: return new DirectPackedReader1(slice);
      case 2: return new DirectPackedReader2(slice);
      case 4: return new DirectPackedReader4(slice);
      case 8: return new DirectPackedReader8(slice);
      case 12: return new DirectPackedReader12(slice);
      case 16: return new DirectPackedReader16(slice);
      case 20: return new DirectPackedReader20(slice);
      case 24: return new DirectPackedReader24(slice);
      case 28: return new DirectPackedReader28(slice);
      case 32: return new DirectPackedReader32(slice);
      case 40: return new DirectPackedReader40(slice);
      case 48: return new DirectPackedReader48(slice);
      case 56: return new DirectPackedReader56(slice);
      case 64: return new DirectPackedReader64(slice);
      default: throw new IllegalArgumentException("unsupported bitsPerValue: " + bitsPerValue);
    }
  }
  
  static final class DirectPackedReader1 extends LongValues {
    final RandomAccessInput in;
    
    DirectPackedReader1(RandomAccessInput in) {
      this.in = in;
    }

    @Override
    public long get(long index) {
      try {
        int shift = 7 - (int) (index & 7);
        return (in.readByte(index >>> 3) >>> shift) & 0x1;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static final class DirectPackedReader2 extends LongValues {
    final RandomAccessInput in;
    
    DirectPackedReader2(RandomAccessInput in) {
      this.in = in;
    }

    @Override
    public long get(long index) {
      try {
        int shift = (3 - (int)(index & 3)) << 1;
        return (in.readByte(index >>> 2) >>> shift) & 0x3;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static final class DirectPackedReader4 extends LongValues {
    final RandomAccessInput in;

    DirectPackedReader4(RandomAccessInput in) {
      this.in = in;
    }

    @Override
    public long get(long index) {
      try {
        int shift = (int) ((index + 1) & 1) << 2;
        return (in.readByte(index >>> 1) >>> shift) & 0xF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
    
  static final class DirectPackedReader8 extends LongValues {
    final RandomAccessInput in;

    DirectPackedReader8(RandomAccessInput in) {
      this.in = in;
    }

    @Override
    public long get(long index) {
      try {
        return in.readByte(index) & 0xFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static final class DirectPackedReader12 extends LongValues {
    final RandomAccessInput in;
    
    DirectPackedReader12(RandomAccessInput in) {
      this.in = in;
    }

    @Override
    public long get(long index) {
      try {
        long offset = (index * 12) >>> 3;
        int shift = (int) ((index + 1) & 1) << 2;
        return (in.readShort(offset) >>> shift) & 0xFFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static final class DirectPackedReader16 extends LongValues {
    final RandomAccessInput in;
    
    DirectPackedReader16(RandomAccessInput in) {
      this.in = in;
    }

    @Override
    public long get(long index) {
      try {
        return in.readShort(index << 1) & 0xFFFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  static final class DirectPackedReader20 extends LongValues {
    final RandomAccessInput in;

    DirectPackedReader20(RandomAccessInput in) {
      this.in = in;
    }

    @Override
    public long get(long index) {
      try {
        long offset = (index * 20) >>> 3;
        // TODO: clean this up...
        int v = in.readInt(offset) >>> 8;
        int shift = (int) ((index + 1) & 1) << 2;
        return (v >>> shift) & 0xFFFFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  static final class DirectPackedReader24 extends LongValues {
    final RandomAccessInput in;
    
    DirectPackedReader24(RandomAccessInput in) {
      this.in = in;
    }

    @Override
    public long get(long index) {
      try {
        return in.readInt(index * 3) >>> 8;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  static final class DirectPackedReader28 extends LongValues {
    final RandomAccessInput in;
    
    DirectPackedReader28(RandomAccessInput in) {
      this.in = in;
    }
    
    @Override
    public long get(long index) {
      try {
        long offset = (index * 28) >>> 3;
        int shift = (int) ((index + 1) & 1) << 2;
        return (in.readInt(offset) >>> shift) & 0xFFFFFFFL;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static final class DirectPackedReader32 extends LongValues {
    final RandomAccessInput in;
    
    DirectPackedReader32(RandomAccessInput in) {
      this.in = in;
    }
    
    @Override
    public long get(long index) {
      try {
        return in.readInt(index << 2) & 0xFFFFFFFFL;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static final class DirectPackedReader40 extends LongValues {
    final RandomAccessInput in;
    
    DirectPackedReader40(RandomAccessInput in) {
      this.in = in;
    }
    
    @Override
    public long get(long index) {
      try {
        return in.readLong(index * 5) >>> 24;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static final class DirectPackedReader48 extends LongValues {
    final RandomAccessInput in;
    
    DirectPackedReader48(RandomAccessInput in) {
      this.in = in;
    }
    
    @Override
    public long get(long index) {
      try {
        return in.readLong(index * 6) >>> 16;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static final class DirectPackedReader56 extends LongValues {
    final RandomAccessInput in;
    
    DirectPackedReader56(RandomAccessInput in) {
      this.in = in;
    }
    
    @Override
    public long get(long index) {
      try {
        return in.readLong(index * 7) >>> 8;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static final class DirectPackedReader64 extends LongValues {
    final RandomAccessInput in;
    
    DirectPackedReader64(RandomAccessInput in) {
      this.in = in;
    }
    
    @Override
    public long get(long index) {
      try {
        return in.readLong(index << 3);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
}
