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
import java.util.Hashtable;
import java.io.*;

/**
 * A very simple entity manager.
 * @author  Anders Kristensen
 */
public class EntityManager {
    protected Hashtable entities = new Hashtable();
    private hplb.org.xml.sax.Parser tok;

    public EntityManager(hplb.org.xml.sax.Parser tok) {
        this.tok = tok;
        entities.put("amp",   "&");
        entities.put("lt",    "<");
        entities.put("gt",    ">");
        entities.put("apos",  "'");
        entities.put("quot", "\"");
    }

    /**
     * Finds entitiy and character references in the provided char array
     * and decodes them. The operation is destructive, i.e. the encoded
     * string replaces the original - this is atrightforward since the
     * new string can only get shorter.
     */
    public final CharBuffer entityDecode(CharBuffer buffer) throws Exception {
        char[] buf = buffer.getCharArray();  // avoids method calls
        int len = buffer.size();

        // not fastest but certainly simplest:
        if (indexOf(buf, '&', 0, len) == -1) return buffer;
        CharBuffer newbuf = new CharBuffer(len);

        for (int start = 0; ; ) {
            int x = indexOf(buf, '&', start, len);
            if (x == -1) {
                newbuf.write(buf, start, len - start);
                return newbuf;
            } else {
                newbuf.write(buf, start, x - start);
                start = x+1;
                x = indexOf(buf, ';', start, len);
                if (x == -1) {
                    //tok.warning("Entity reference not semicolon terminated");
                    newbuf.write('&');
                    //break; //???????????
                } else {
                    try {
                        writeEntityDef(buf, start, x-start, newbuf);
                        start = x+1;
                    } catch (Exception ex) {
                        //tok.warning("Bad entity reference");
                    }
                }
            }
        }
    }

    // character references are rare enough that we don't care about
    // creating a String object for them unnecessarily...
    public void writeEntityDef(char[] buf, int off, int len, Writer out)
        throws Exception, IOException, NumberFormatException
    {
        Integer ch;
        //System.out.println("Entity: " + new String(buf, off, len) +" "+off+" "+len);

        if (buf[off] == '#') {  // character reference
            off++;
            len--;
            if (buf[off] == 'x' || buf[off] == 'X') {
                ch = Integer.valueOf(new String(buf, off+1, len-1), 16);
            } else {
                ch = Integer.valueOf(new String(buf, off, len));
            }
            out.write(ch.intValue());
         } else {
            String ent = new String(buf, off, len);
            String val = (String) entities.get(ent);
            if (val != null) {
                out.write(val);
            } else {
                out.write("&" + ent + ";");
                //tok.warning("unknown entity reference: " + ent);
            }
        }
    }

    public String defTextEntity(String entity, String value) {
        return (String) entities.put(entity, value);
    }

    /**
     * Returns the index within this String of the first occurrence of the
     * specified character, starting the search at fromIndex. This method
     * returns -1 if the character is not found.
     * @params buf        the buffer to search
     * @params ch         the character to search for
     * @params from       the index to start the search from
     * @params to         the highest possible index returned plus 1
     * @throws IndexOutOfBoundsException  if index out of bounds...
     */
    public static final int indexOf(char[] buf, int ch, int from, int to) {
        int i;
        for (i = from; i < to && buf[i] != ch; i++)
            ;  // do nothing
        if (i < to) return i;
        else return -1;
    }

    // FOR TESTING
    /*
    public static void main(String[] args) throws Exception {
        Parser tok = new Parser();
        tst.xml.TokArgs.args(args, tok);
        CharBuffer buf1 = new CharBuffer();
        buf1.write(args[0]);
        CharBuffer buf2 = tok.entMngr.entityDecode(buf1);

        System.out.println("Changed: " + (buf1 != buf2));
        System.out.println("Result: [" + buf2 + "]");
    }
    */
}
