package org.apache.solr.spelling.suggest.fst;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.store.DataInput;
import com.google.common.io.ByteStreams;

/**
 * A {@link DataInput} wrapping a plain {@link InputStream}.
 */
public class InputStreamDataInput extends DataInput {
  
  private final InputStream is;

  public InputStreamDataInput(InputStream is) {
    this.is = is;
  }
  
  @Override
  public byte readByte() throws IOException {
    int v = is.read();
    if (v == -1) throw new EOFException();
    return (byte) v;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    ByteStreams.readFully(is, b, offset, len);
  }
}
