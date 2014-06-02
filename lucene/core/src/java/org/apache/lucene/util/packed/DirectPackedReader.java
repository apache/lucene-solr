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

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/* Reads directly from disk on each get */
class DirectPackedReader extends PackedInts.ReaderImpl {
  final IndexInput in;
  final long startPointer;
  final long valueMask;

  DirectPackedReader(int bitsPerValue, int valueCount, IndexInput in) {
    super(valueCount, bitsPerValue);
    this.in = in;

    startPointer = in.getFilePointer();
    if (bitsPerValue == 64) {
      valueMask = -1L;
    } else {
      valueMask = (1L << bitsPerValue) - 1;
    }
  }

  @Override
  public long get(int index) {
    final long majorBitPos = (long)index * bitsPerValue;
    final long elementPos = majorBitPos >>> 3;
    try {
      in.seek(startPointer + elementPos);

      final int bitPos = (int) (majorBitPos & 7);
      // round up bits to a multiple of 8 to find total bytes needed to read
      final int roundedBits = ((bitPos + bitsPerValue + 7) & ~7);
      // the number of extra bits read at the end to shift out
      int shiftRightBits = roundedBits - bitPos - bitsPerValue;

      long rawValue;
      switch (roundedBits >>> 3) {
        case 1:
          rawValue = in.readByte();
          break;
        case 2:
          rawValue = in.readShort();
          break;
        case 3:
          rawValue = ((long)in.readShort() << 8) | (in.readByte() & 0xFFL);
          break;
        case 4:
          rawValue = in.readInt();
          break;
        case 5:
          rawValue = ((long)in.readInt() << 8) | (in.readByte() & 0xFFL);
          break;
        case 6:
          rawValue = ((long)in.readInt() << 16) | (in.readShort() & 0xFFFFL);
          break;
        case 7:
          rawValue = ((long)in.readInt() << 24) | ((in.readShort() & 0xFFFFL) << 8) | (in.readByte() & 0xFFL);
          break;
        case 8:
          rawValue = in.readLong();
          break;
        case 9:
          // We must be very careful not to shift out relevant bits. So we account for right shift
          // we would normally do on return here, and reset it.
          rawValue = (in.readLong() << (8 - shiftRightBits)) | ((in.readByte() & 0xFFL) >>> shiftRightBits);
          shiftRightBits = 0;
          break;
        default:
          throw new AssertionError("bitsPerValue too large: " + bitsPerValue);
      }
      return (rawValue >>> shiftRightBits) & valueMask;

    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
  
  static class DirectPackedReader1 extends DirectPackedReader {
    DirectPackedReader1(int valueCount, IndexInput in) {
      super(1, valueCount, in);
    }

    @Override
    public long get(int index) {
      try {
        in.seek(startPointer + (index >>> 3));
        int shift = 7 - (index & 7);
        return (in.readByte() >>> shift) & 0x1;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static class DirectPackedReader2 extends DirectPackedReader {
    DirectPackedReader2(int valueCount, IndexInput in) {
      super(2, valueCount, in);
    }

    @Override
    public long get(int index) {
      try {
        in.seek(startPointer + (index >>> 2));
        int shift = (3 - (index & 3)) << 1;
        return (in.readByte() >>> shift) & 0x3;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static class DirectPackedReader4 extends DirectPackedReader {
    DirectPackedReader4(int valueCount, IndexInput in) {
      super(4, valueCount, in);
    }

    @Override
    public long get(int index) {
      try {
        in.seek(startPointer + (index >>> 1));
        int shift = ((index + 1) & 1) << 2;
        return (in.readByte() >>> shift) & 0xF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
    
  static class DirectPackedReader8 extends DirectPackedReader {
    DirectPackedReader8(int valueCount, IndexInput in) {
      super(8, valueCount, in);
    }

    @Override
    public long get(int index) {
      try {
        in.seek(startPointer + index);
        return in.readByte() & 0xFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static class DirectPackedReader12 extends DirectPackedReader {
    DirectPackedReader12(int valueCount, IndexInput in) {
      super(12, valueCount, in);
    }

    @Override
    public long get(int index) {
      try {
        long offset = (index * 12L) >>> 3;
        in.seek(startPointer + offset);
        int shift = ((index + 1) & 1) << 2;
        return (in.readShort() >>> shift) & 0xFFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static class DirectPackedReader16 extends DirectPackedReader {
    DirectPackedReader16(int valueCount, IndexInput in) {
      super(16, valueCount, in);
    }

    @Override
    public long get(int index) {
      try {
        in.seek(startPointer + (index<<1));
        return in.readShort() & 0xFFFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  static class DirectPackedReader20 extends DirectPackedReader {
    DirectPackedReader20(int valueCount, IndexInput in) {
      super(20, valueCount, in);
    }

    @Override
    public long get(int index) {
      try {
        long offset = (index * 20L) >>> 3;
        in.seek(startPointer + offset);
        int v = in.readShort() << 8 | (in.readByte() & 0xFF);
        int shift = ((index + 1) & 1) << 2;
        return (v >>> shift) & 0xFFFFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  static class DirectPackedReader24 extends DirectPackedReader {
    DirectPackedReader24(int valueCount, IndexInput in) {
      super(24, valueCount, in);
    }

    @Override
    public long get(int index) {
      try {
        in.seek(startPointer + (index*3));
        return (in.readShort() & 0xFFFF) << 8 | (in.readByte() & 0xFF);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  static class DirectPackedReader28 extends DirectPackedReader {
    DirectPackedReader28(int valueCount, IndexInput in) {
      super(28, valueCount, in);
    }
    
    @Override
    public long get(int index) {
      try {
        long offset = (index * 28L) >>> 3;
        in.seek(startPointer + offset);
        int shift = ((index + 1) & 1) << 2;
        return (in.readInt() >>> shift) & 0xFFFFFFFL;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static class DirectPackedReader32 extends DirectPackedReader {
    DirectPackedReader32(int valueCount, IndexInput in) {
      super(32, valueCount, in);
    }
    
    @Override
    public long get(int index) {
      try {
        in.seek(startPointer + (index<<2));
        return in.readInt() & 0xFFFFFFFFL;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static class DirectPackedReader40 extends DirectPackedReader {
    DirectPackedReader40(int valueCount, IndexInput in) {
      super(40, valueCount, in);
    }
    
    @Override
    public long get(int index) {
      try {
        in.seek(startPointer + (index*5));
        return (in.readInt() & 0xFFFFFFFFL) << 8 | (in.readByte() & 0xFF);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static class DirectPackedReader48 extends DirectPackedReader {
    DirectPackedReader48(int valueCount, IndexInput in) {
      super(48, valueCount, in);
    }
    
    @Override
    public long get(int index) {
      try {
        in.seek(startPointer + (index*6));
        return (in.readInt() & 0xFFFFFFFFL) << 16 | (in.readShort() & 0xFFFF);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static class DirectPackedReader56 extends DirectPackedReader {
    DirectPackedReader56(int valueCount, IndexInput in) {
      super(56, valueCount, in);
    }
    
    @Override
    public long get(int index) {
      try {
        in.seek(startPointer + (index*7));
        return (in.readInt() & 0xFFFFFFFFL) << 24 | (in.readShort() & 0xFFFF) << 8 | (in.readByte() & 0xFF);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
  
  static class DirectPackedReader64 extends DirectPackedReader {
    DirectPackedReader64(int valueCount, IndexInput in) {
      super(64, valueCount, in);
    }
    
    @Override
    public long get(int index) {
      try {
        in.seek(startPointer + (index<<3));
        return in.readLong();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }    
  }
     
  static DirectPackedReader getInstance(int bitsPerValue, int valueCount, IndexInput in) {
    switch(bitsPerValue) {
      case 1: return new DirectPackedReader1(valueCount, in);
      case 2: return new DirectPackedReader2(valueCount, in);
      case 4: return new DirectPackedReader4(valueCount, in);
      case 8: return new DirectPackedReader8(valueCount, in);
      case 12: return new DirectPackedReader12(valueCount, in);
      case 16: return new DirectPackedReader16(valueCount, in);
      case 20: return new DirectPackedReader20(valueCount, in);
      case 24: return new DirectPackedReader24(valueCount, in);
      case 28: return new DirectPackedReader28(valueCount, in);
      case 32: return new DirectPackedReader32(valueCount, in);
      case 40: return new DirectPackedReader40(valueCount, in);
      case 48: return new DirectPackedReader48(valueCount, in);
      case 56: return new DirectPackedReader56(valueCount, in);
      case 64: return new DirectPackedReader64(valueCount, in);
      default: return new DirectPackedReader(bitsPerValue, valueCount, in);
    }
  }
}
