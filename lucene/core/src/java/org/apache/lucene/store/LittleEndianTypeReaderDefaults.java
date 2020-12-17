package org.apache.lucene.store;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Implements all {@link TypeReader} methods with little-endian defaults.
 * @param <T>
 */
public interface LittleEndianTypeReaderDefaults<T extends DataInput> extends TypeReader<T> {
  // I was just lazy here, obviously this should be implemented.
  @Override
  default short readShort() throws IOException {
    throw new IOException("Implement me");
  }

  /** Reads four bytes and returns an int.
   * @see DataOutput#writeInt(int)
   */
  @Override
  default int readInt() throws IOException {
    throw new IOException("Implement me");
  }

  /** Reads eight bytes and returns a long.
   * @see DataOutput#writeLong(long)
   */
  @Override
  default long readLong() throws IOException {
    throw new IOException("Implement me");
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
      dst[offset + i] = readLong();
    }
  }

  @Override
  default ByteOrder getByteOrder() {
    return ByteOrder.LITTLE_ENDIAN;
  }
}
