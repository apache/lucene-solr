package org.apache.lucene.store;

import java.io.IOException;

public class DelegatingTypeReaderBigEndian<T extends DataInput> extends DataInput implements BigEndianTypeReaderDefaults<T> {
  final T in;

  public DelegatingTypeReaderBigEndian(T in) {
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
    return new DelegatingTypeReaderBigEndian<>(in.clone());
  }
}
