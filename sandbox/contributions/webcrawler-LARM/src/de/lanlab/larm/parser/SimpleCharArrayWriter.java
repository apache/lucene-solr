package de.lanlab.larm.parser;

import java.io.CharArrayWriter;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2002</p>
 * <p>Company: </p>
 * @author unascribed
 * @version 1.0
 */

public final class SimpleCharArrayWriter extends java.io.CharArrayWriter {
    public SimpleCharArrayWriter() {
        super();
    }

    public SimpleCharArrayWriter(int size) {
        super(size);
    }

    // use only to *decrement* size
    public void setLength(int size) {
       // synchronized (lock) {
            if (size < count) count = size;
       // }
    }

    public char[] getCharArray() {
       // synchronized (lock) {
            return buf;
       // }
    }

    public int getLength()
    {
        return count;
    }


}
