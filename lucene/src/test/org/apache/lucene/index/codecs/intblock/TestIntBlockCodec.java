package org.apache.lucene.index.codecs.intblock;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.store.*;
import org.apache.lucene.index.codecs.sep.*;

public class TestIntBlockCodec extends LuceneTestCase {

  public void testSimpleIntBlocks() throws Exception {
    Directory dir = new MockRAMDirectory();

    IntIndexOutput out = new SimpleIntBlockIndexOutput(dir, "test", 128);
    for(int i=0;i<11777;i++) {
      out.write(i);
    }
    out.close();

    IntIndexInput in = new SimpleIntBlockIndexInput(dir, "test", 128);
    IntIndexInput.Reader r = in.reader();

    for(int i=0;i<11777;i++) {
      assertEquals(i, r.next());
    }
    in.close();
    
    dir.close();
  }

  public void testEmptySimpleIntBlocks() throws Exception {
    Directory dir = new MockRAMDirectory();

    IntIndexOutput out = new SimpleIntBlockIndexOutput(dir, "test", 128);
    // write no ints
    out.close();

    IntIndexInput in = new SimpleIntBlockIndexInput(dir, "test", 128);
    IntIndexInput.Reader r = in.reader();
    // read no ints
    in.close();
    dir.close();
  }
}
