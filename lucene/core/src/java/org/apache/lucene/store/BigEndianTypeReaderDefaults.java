package org.apache.lucene.store;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Implements all {@link TypeReader} methods with big-endian defaults.
 * @param <T>
 */
public interface BigEndianTypeReaderDefaults<T extends DataInput> extends TypeReader<T>, TypeReaderDefaults<T> {
  @Override
  default short readShort() throws IOException {
    return (short) (((readByte() & 0xFF) <<  8) |  (readByte() & 0xFF));
  }

  /** Reads four bytes and returns an int.
   * @see DataOutput#writeInt(int)
   */
  @Override
  default int readInt() throws IOException {
    return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16)
        | ((readByte() & 0xFF) <<  8) |  (readByte() & 0xFF);
  }

  /** Reads eight bytes and returns a long.
   * @see DataOutput#writeLong(long)
   */
  @Override
  default long readLong() throws IOException {
    return (((long)readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
  }

  /**
   * Read a specified number of longs with the little endian byte order.
   * <p>This method can be used to read longs whose bytes have been
   * {@link Long#reverseBytes reversed} at write time:
   * <pre class="prettyprint">
   * for (long l : longs) {
   *   output.writeLong(Long.reverseBytes(l));
   * }
   * </pre>
   * @lucene.experimental
   */
  // TODO: LUCENE-9047: Make the entire DataInput/DataOutput API little endian
  // Then this would just be `readLongs`?
  @Override
  default void readLELongs(long[] dst, int offset, int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, dst.length);
    for (int i = 0; i < length; ++i) {
      dst[offset + i] = Long.reverseBytes(readLong());
    }
  }

  @Override
  default ByteOrder getByteOrder() {
    return ByteOrder.BIG_ENDIAN;
  }
}
