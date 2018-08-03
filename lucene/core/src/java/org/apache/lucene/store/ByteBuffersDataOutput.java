package org.apache.lucene.store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.UnicodeUtil;

public final class ByteBuffersDataOutput extends DataOutput implements Accountable {
  private final static ByteBuffer EMPTY = ByteBuffer.allocate(0);
  private final static List<ByteBuffer> EMPTY_LIST = Arrays.asList(EMPTY);
  private final static byte [] EMPTY_BYTE_ARRAY = {};

  private final static IntFunction<ByteBuffer> ALLOCATE_BB_ON_HEAP = (size) -> {
    return ByteBuffer.allocate(size);
  };

  final static int DEFAULT_MIN_BITS_PER_BLOCK = 10; // 1024 B
  final static int DEFAULT_MAX_BITS_PER_BLOCK = 25; //   32 MB

  /**
   * Maximum number of blocks at the current {@link #blockBits} block size
   * before we increase block sizes.
   */
  final static int MAX_BLOCKS_BEFORE_BLOCK_EXPANSION = 100;

  /**
   * Maximum block size: {@code 2^bits}.
   */
  private final int maxBitsPerBlock;

  /**
   * {@link ByteBuffer} supplier.
   */
  private final IntFunction<ByteBuffer> blockAllocate;

  /**
   * Current block size: {@code 2^bits}.
   */
  private int blockBits;

  /**
   * Blocks storing data.
   */
  private final ArrayDeque<ByteBuffer> blocks = new ArrayDeque<>();

  /**
   * The current-or-next write block.
   */
  private ByteBuffer currentBlock = EMPTY;

  public ByteBuffersDataOutput(long expectedSize) {
    this(computeBlockSizeBitsFor(expectedSize), DEFAULT_MAX_BITS_PER_BLOCK, ALLOCATE_BB_ON_HEAP);
  }

  public ByteBuffersDataOutput() {
    this(DEFAULT_MIN_BITS_PER_BLOCK, DEFAULT_MAX_BITS_PER_BLOCK, ALLOCATE_BB_ON_HEAP);
  }

  public ByteBuffersDataOutput(int minBitsPerBlock, 
                               int maxBitsPerBlock,
                               IntFunction<ByteBuffer> blockAllocate) {
    if (minBitsPerBlock < 10 ||
        minBitsPerBlock > maxBitsPerBlock ||
        maxBitsPerBlock > 31) {
      throw new IllegalArgumentException(String.format(Locale.ROOT,
          "Invalid arguments: %s %s",
          minBitsPerBlock,
          maxBitsPerBlock));
    }
    this.blockAllocate = Objects.requireNonNull(blockAllocate, "Block allocator must not be null.");
    this.maxBitsPerBlock = maxBitsPerBlock;
    this.blockBits = minBitsPerBlock;
  }

  @Override
  public void writeByte(byte b) {
    if (!currentBlock.hasRemaining()) {
      appendBlock();
    }
    currentBlock.put(b);
  }

  @Override
  public void writeBytes(byte[] src, int offset, int length) {
    assert length >= 0;
    while (length > 0) {
      if (!currentBlock.hasRemaining()) {
        appendBlock();
      }

      int chunk = Math.min(currentBlock.remaining(), length);
      currentBlock.put(src, offset, chunk);
      length -= chunk;
      offset += chunk;
    }
  }

  @Override
  public void writeBytes(byte[] b, int length) {
    writeBytes(b, 0, length);
  }

  public void writeBytes(byte[] b) {
    writeBytes(b, 0, b.length);
  }

  public void writeBytes(ByteBuffer buffer) {
    buffer = buffer.duplicate();
    int length = buffer.remaining();
    while (length > 0) {
      if (!currentBlock.hasRemaining()) {
        appendBlock();
      }

      int chunk = Math.min(currentBlock.remaining(), length);
      buffer.limit(buffer.position() + chunk);
      currentBlock.put(buffer);

      length -= chunk;
    }
  }

  /**
   * Return a contiguous array with the current content written to the output.
   */
  public byte[] toArray() {
    if (blocks.size() == 0) {
      return EMPTY_BYTE_ARRAY;
    }

    // We could try to detect single-block, array-based ByteBuffer here
    // and use Arrays.copyOfRange, but I don't think it's worth the extra
    // instance checks.

    byte [] arr = new byte[Math.toIntExact(size())];
    int offset = 0;
    for (ByteBuffer bb : toBufferList()) {
      int len = bb.remaining();
      bb.get(arr, offset, len);
      offset += len;
    }
    return arr;
  }  

  /**
   * Return a list of read-only {@link ByteBuffer} blocks over the 
   * current content written to the output.
   */
  public ArrayList<ByteBuffer> toBufferList() {
    if (blocks.isEmpty()) {
      return new ArrayList<>(EMPTY_LIST);
    } else {
      ArrayList<ByteBuffer> result = new ArrayList<>(Math.max(blocks.size(), 1));
      for (ByteBuffer bb : blocks) {
        bb = (ByteBuffer) bb.asReadOnlyBuffer().flip(); // cast for jdk8 (covariant in jdk9+) 
        result.add(bb);
      }
      return result;
    }
  }

  /**
   * Return a {@link ByteBuffersDataInput} for the set of current buffers ({@link #toBufferList()}). 
   */
  public ByteBuffersDataInput toDataInput() {
    return new ByteBuffersDataInput(toBufferList());
  }

  /**
   * @return The number of bytes written to this output so far.
   */
  public long size() {
    long size = 0;
    int blockCount = blocks.size();
    if (blockCount >= 1) {
      int fullBlockSize = (blockCount - 1) * blockSize();
      int lastBlockSize = blocks.getLast().position();
      size = fullBlockSize + lastBlockSize;
    }
    return size;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT,
        "%,d bytes, block size: %,d, blocks: %,d",
        size(),
        blockSize(),
        blocks.size());
  }

  // Specialized versions of writeXXX methods that break execution into
  // fast/ slow path if the result would fall on the current block's 
  // boundary.
  // 
  // We also remove the IOException from methods because it (theoretically)
  // cannot be thrown from byte buffers.

  @Override
  public void writeShort(short v) {
    try {
      if (currentBlock.remaining() >= Short.BYTES) {
        currentBlock.putShort(v);
      } else {
        super.writeShort(v);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void writeInt(int v) {
    try {
      if (currentBlock.remaining() >= Integer.BYTES) {
        currentBlock.putInt(v);
      } else {
        super.writeInt(v);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }    
  }

  @Override
  public void writeLong(long v) {
    try {
      if (currentBlock.remaining() >= Long.BYTES) {
        currentBlock.putLong(v);
      } else {
        super.writeLong(v);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }    
  }

  @Override
  public void writeString(String v) {
    try {
      final int MAX_CHARS_PER_WINDOW = 1024;
      if (v.length() <= MAX_CHARS_PER_WINDOW) {
        final BytesRef utf8 = new BytesRef(v);
        writeVInt(utf8.length);
        writeBytes(utf8.bytes, utf8.offset, utf8.length);
      } else {
        writeVInt(UnicodeUtil.calcUTF16toUTF8Length(v, 0, v.length()));
        final byte [] buf = new byte [UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * MAX_CHARS_PER_WINDOW];
        UTF16toUTF8(v, 0, v.length(), buf, (len) -> {
          writeBytes(buf, 0, len);
        });
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }    
  }
  
  @Override
  public void writeMapOfStrings(Map<String, String> map) {
    try {
      super.writeMapOfStrings(map);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
  
  @Override
  public void writeSetOfStrings(Set<String> set) {
    try {
      super.writeSetOfStrings(set);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }      
  }

  @Override
  public long ramBytesUsed() {
    // Return a rough estimation for allocated blocks. Note that we do not make
    // any special distinction for direct memory buffers.
    return RamUsageEstimator.NUM_BYTES_OBJECT_REF * blocks.size() + 
           blocks.stream().mapToLong(buf -> buf.capacity()).sum();
  }

  private int blockSize() {
    return 1 << blockBits;
  }

  private void appendBlock() {
    if (blocks.size() >= MAX_BLOCKS_BEFORE_BLOCK_EXPANSION && blockBits < maxBitsPerBlock) {
      rewriteToBlockSize(blockBits + 1);
      if (blocks.getLast().hasRemaining()) {
        return;
      }
    }

    final int requiredBlockSize = 1 << blockBits;
    currentBlock = blockAllocate.apply(requiredBlockSize);
    assert currentBlock.capacity() == requiredBlockSize;
    blocks.add(currentBlock);
  }

  private void rewriteToBlockSize(int blockBits) {
    assert blockBits <= maxBitsPerBlock;

    // We copy over data blocks to an output with one-larger block bit size.
    // We also discard references to blocks as we're copying to allow GC to
    // clean up partial results in case of memory pressure.
    ByteBuffersDataOutput cloned = new ByteBuffersDataOutput(blockBits, blockBits, blockAllocate);
    ByteBuffer block;
    while ((block = blocks.pollFirst()) != null) {
      block.flip();
      cloned.writeBytes(block);
    }

    assert blocks.isEmpty();
    this.blockBits = blockBits;
    blocks.addAll(cloned.blocks);
  }

  private static int computeBlockSizeBitsFor(long bytes) {
    long powerOfTwo = BitUtil.nextHighestPowerOfTwo(bytes / MAX_BLOCKS_BEFORE_BLOCK_EXPANSION);
    if (powerOfTwo == 0) {
      return DEFAULT_MIN_BITS_PER_BLOCK;
    }
    
    int blockBits = (int) Long.numberOfTrailingZeros(powerOfTwo);
    blockBits = Math.min(blockBits, DEFAULT_MAX_BITS_PER_BLOCK);
    blockBits = Math.max(blockBits, DEFAULT_MIN_BITS_PER_BLOCK);
    return blockBits;
  }

  private static final long HALF_SHIFT = 10;
  private static final int SURROGATE_OFFSET = 
      Character.MIN_SUPPLEMENTARY_CODE_POINT - 
      (UnicodeUtil.UNI_SUR_HIGH_START << HALF_SHIFT) - UnicodeUtil.UNI_SUR_LOW_START;

  /**
   * A consumer-based UTF16-UTF8 encoder (writes the input string in smaller buffers.).
   */
  private static int UTF16toUTF8(final CharSequence s, 
                                final int offset, 
                                final int length, 
                                byte[] buf, 
                                IntConsumer bufferFlusher) {
    int utf8Len = 0;
    int j = 0;    
    for (int i = offset, end = offset + length; i < end; i++) {
      final int chr = (int) s.charAt(i);

      if (j + 4 >= buf.length) {
        bufferFlusher.accept(j);
        utf8Len += j;
        j = 0;
      }

      if (chr < 0x80)
        buf[j++] = (byte) chr;
      else if (chr < 0x800) {
        buf[j++] = (byte) (0xC0 | (chr >> 6));
        buf[j++] = (byte) (0x80 | (chr & 0x3F));
      } else if (chr < 0xD800 || chr > 0xDFFF) {
        buf[j++] = (byte) (0xE0 | (chr >> 12));
        buf[j++] = (byte) (0x80 | ((chr >> 6) & 0x3F));
        buf[j++] = (byte) (0x80 | (chr & 0x3F));
      } else {
        // A surrogate pair. Confirm valid high surrogate.
        if (chr < 0xDC00 && (i < end - 1)) {
          int utf32 = (int) s.charAt(i + 1);
          // Confirm valid low surrogate and write pair.
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
            utf32 = (chr << 10) + utf32 + SURROGATE_OFFSET;
            i++;
            buf[j++] = (byte) (0xF0 | (utf32 >> 18));
            buf[j++] = (byte) (0x80 | ((utf32 >> 12) & 0x3F));
            buf[j++] = (byte) (0x80 | ((utf32 >> 6) & 0x3F));
            buf[j++] = (byte) (0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // Replace unpaired surrogate or out-of-order low surrogate
        // with substitution character.
        buf[j++] = (byte) 0xEF;
        buf[j++] = (byte) 0xBF;
        buf[j++] = (byte) 0xBD;
      }
    }

    bufferFlusher.accept(j);
    utf8Len += j;

    return utf8Len;
  }
}
