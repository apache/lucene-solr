package org.apache.lucene.index;

import junit.framework.TestCase;
import org.apache.lucene.store.InputStream;

import java.io.IOException;

public class TestInputStream extends TestCase {
    public void testRead() throws IOException {
        InputStream is = new MockInputStream(new byte[] { (byte) 0x80, 0x01,
                                                          (byte) 0xFF, 0x7F,
                                                          (byte) 0x80, (byte) 0x80, 0x01,
                                                          (byte) 0x81, (byte) 0x80, 0x01,
                                                          0x06, 'L', 'u', 'c', 'e', 'n', 'e'});
        assertEquals(128,is.readVInt());
        assertEquals(16383,is.readVInt());
        assertEquals(16384,is.readVInt());
        assertEquals(16385,is.readVInt());
        assertEquals("Lucene",is.readString());
    }
}
