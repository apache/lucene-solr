/*
 * $Id$
 *
 * Copyright 1997 Hewlett-Packard Company
 *
 * This file may be copied, modified and distributed only in
 * accordance with the terms of the limited licence contained
 * in the accompanying file LICENSE.TXT.
 */

package hplb.xml;

/**
 * A java.io.CharArrayWriter with the additional property that users can get
 * to the actual underlying storage. Hence it's very fast (and dangerous).
 * @author      Anders Kristensen
 */
public final class CharBuffer extends java.io.CharArrayWriter {
    public CharBuffer() {
        super();
    }

    public CharBuffer(int size) {
        super(size);
    }

    // use only to *decrement* size
    public void setLength(int size) {
        synchronized (lock) {
            if (size < count) count = size;
	    }
	}

    public char[] getCharArray() {
	    synchronized (lock) {
	        return buf;
	    }
    }

    public int getLength()
    {
        return count;
    }


}
