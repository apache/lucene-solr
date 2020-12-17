package org.apache.lucene.store;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Set;

/**
 * Byte-order specific (and complex type) reader.
 */
public interface TypeReader<T extends DataInput> {
  // Byte order for this reader.
  public ByteOrder getByteOrder();

  // The input to which this type reader is attached.
  T input();

  // For convenience only.
  public abstract byte readByte() throws IOException;
  public abstract void readBytes(byte[] b, int offset, int len) throws IOException;

  // Complex types.
  public short readShort() throws IOException;
  public int readInt() throws IOException;

  public long readLong() throws IOException;

  // This could be eliminated by adding readLongs() and enforcing the byte order
  // is little endian downstream where readLongs() are used (within the codec).
  public void readLELongs(long[] dst, int offset, int length) throws IOException;

  public int readVInt() throws IOException;
  public int readZInt() throws IOException;
  public long readVLong() throws IOException;
  public long readZLong() throws IOException;

  public String readString() throws IOException;
  public Map<String, String> readMapOfStrings() throws IOException;
  public Set<String> readSetOfStrings() throws IOException;
}
