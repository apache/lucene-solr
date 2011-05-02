package org.apache.solr.spelling.suggest.fst;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.lucene.store.DataOutput;

/**
 * A {@link DataOutput} wrapping a plain {@link OutputStream}.
 */
public class OutputStreamDataOutput extends DataOutput {
  
  private final OutputStream os;
  
  public OutputStreamDataOutput(OutputStream os) {
    this.os = os;
  }
  
  @Override
  public void writeByte(byte b) throws IOException {
    os.write(b);
  }
  
  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    os.write(b, offset, length);
  }
}
