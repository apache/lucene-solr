package org.apache.lucene.store;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.zip.CRC32;

public final class ByteBuffersIndexOutput extends IndexOutput {
  private ByteBuffersDataOutput delegate;
  private BufferedChecksum crc;
  private Consumer<ByteBuffersDataOutput> onClose;

  public ByteBuffersIndexOutput(ByteBuffersDataOutput delegate, String resourceDescription, String name) {
    this(delegate, resourceDescription, name, null);
  }

  public ByteBuffersIndexOutput(ByteBuffersDataOutput delegate, String resourceDescription, String name, Consumer<ByteBuffersDataOutput> onClose) {
    super(resourceDescription, name);
    this.delegate = delegate;
    this.crc = new BufferedChecksum(new CRC32());
    this.onClose = onClose;
  }

  @Override
  public void close() throws IOException {
    // No special effort to be thread-safe here since IndexOutputs are not required to be thread-safe.
    ByteBuffersDataOutput local = delegate;
    delegate = null;
    if (local != null && onClose != null) {
      onClose.accept(local);
    }
  }

  @Override
  public long getFilePointer() {
    ensureOpen();
    return delegate.size();
  }

  @Override
  public long getChecksum() throws IOException {
    ensureOpen();
    return crc.getValue();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    ensureOpen();
    delegate.writeByte(b);
    crc.update(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    ensureOpen();
    delegate.writeBytes(b, offset, length);
    crc.update(b, offset, length);
  }

  @Override
  public void writeBytes(byte[] b, int length) throws IOException {
    ensureOpen();
    delegate.writeBytes(b, length);
    crc.update(b, 0, length);
  }

  private void ensureOpen() {
    if (delegate == null) {
      throw new AlreadyClosedException("Already closed.");
    }
  }  
}
