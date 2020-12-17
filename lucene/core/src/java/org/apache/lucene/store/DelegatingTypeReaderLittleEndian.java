package org.apache.lucene.store;

import java.io.IOException;

public class DelegatingTypeReaderLittleEndian<T extends DataInput> extends DataInput implements LittleEndianTypeReaderDefaults<T> {
  final T in;

  public DelegatingTypeReaderLittleEndian(T in) {
    this.in = in;
  }

  @Override
  public T input() {
    return in;
  }

  @Override
  public byte readByte() throws IOException {
    return in.readByte();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    in.readBytes(b, offset, len);
  }

  @Override
  public DataInput clone() {
    return new DelegatingTypeReaderLittleEndian<>(in.clone());
  }
}
