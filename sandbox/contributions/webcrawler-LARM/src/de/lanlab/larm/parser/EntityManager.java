/*
 *  ====================================================================
 *  The Apache Software License, Version 1.1
 *
 *  Copyright (c) 2001 The Apache Software Foundation.  All rights
 *  reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *  notice, this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the following disclaimer in
 *  the documentation and/or other materials provided with the
 *  distribution.
 *
 *  3. The end-user documentation included with the redistribution,
 *  if any, must include the following acknowledgment:
 *  "This product includes software developed by the
 *  Apache Software Foundation (http://www.apache.org/)."
 *  Alternately, this acknowledgment may appear in the software itself,
 *  if and wherever such third-party acknowledgments normally appear.
 *
 *  4. The names "Apache" and "Apache Software Foundation" and
 *  "Apache Lucene" must not be used to endorse or promote products
 *  derived from this software without prior written permission. For
 *  written permission, please contact apache@apache.org.
 *
 *  5. Products derived from this software may not be called "Apache",
 *  "Apache Lucene", nor may "Apache" appear in their name, without
 *  prior written permission of the Apache Software Foundation.
 *
 *  THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 *  OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 *  ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 *  USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 *  OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 *  SUCH DAMAGE.
 *  ====================================================================
 *
 *  This software consists of voluntary contributions made by many
 *  individuals on behalf of the Apache Software Foundation.  For more
 *  information on the Apache Software Foundation, please see
 *  <http://www.apache.org/>.
 */
package de.lanlab.larm.parser;

import java.util.Hashtable;
import java.io.*;

/**
 * A very simple entity manager. Based on HeX, the HTML enabled XML parser, by
 * Anders Kristensen, HP Labs Bristol
 *
 * @author    Administrator
 * @created   1. Juni 2002
 */
public class EntityManager
{
    /**
     * Description of the Field
     */
    protected Hashtable entities = new Hashtable();

    /**
     * Description of the Field
     */
    private Tokenizer tok;


    /**
     * Constructor for the EntityManager object
     *
     * @param tok  Description of the Parameter
     */
    public EntityManager(Tokenizer tok)
    {
        this.tok = tok;
        entities.put("amp", "&");
        entities.put("lt", "<");
        entities.put("gt", ">");
        entities.put("apos", "'");
        entities.put("quot", "\"");
        entities.put("auml", "ä");
        entities.put("ouml", "ö");
        entities.put("uuml", "ü");
        entities.put("Auml", "Ä");
        entities.put("Ouml", "Ö");
        entities.put("Uuml", "Ü");
        entities.put("szlig", "ß");

    }


    /**
     * Finds entitiy and character references in the provided char array and
     * decodes them. The operation is destructive, i.e. the encoded string
     * replaces the original - this is atrightforward since the new string can
     * only get shorter.
     *
     * @param buffer         Description of the Parameter
     * @return               Description of the Return Value
     * @exception Exception  Description of the Exception
     */
    public final SimpleCharArrayWriter entityDecode(SimpleCharArrayWriter buffer)
        throws Exception
    {
        char[] buf = buffer.getCharArray();
        // avoids method calls
        int len = buffer.size();

        // not fastest but certainly simplest:
        if (indexOf(buf, '&', 0, len) == -1)
        {
            return buffer;
        }
        SimpleCharArrayWriter newbuf = new SimpleCharArrayWriter(len);

        for (int start = 0; ; )
        {
            int x = indexOf(buf, '&', start, len);
            if (x == -1)
            {
                newbuf.write(buf, start, len - start);
                return newbuf;
            }
            else
            {
                newbuf.write(buf, start, x - start);
                start = x + 1;
                x = indexOf(buf, ';', start, len);
                if (x == -1)
                {
                    //tok.warning("Entity reference not semicolon terminated");
                    newbuf.write('&');
                    //break; //???????????
                }
                else
                {
                    try
                    {
                        writeEntityDef(buf, start, x - start, newbuf);
                        start = x + 1;
                    }
                    catch (Exception ex)
                    {
                        //tok.warning("Bad entity reference");
                    }
                }
            }
        }
    }


    // character references are rare enough that we don't care about
    // creating a String object for them unnecessarily...
    /**
     * Description of the Method
     *
     * @param buf                        Description of the Parameter
     * @param off                        Description of the Parameter
     * @param len                        Description of the Parameter
     * @param out                        Description of the Parameter
     * @exception Exception              Description of the Exception
     * @exception IOException            Description of the Exception
     * @exception NumberFormatException  Description of the Exception
     */
    public void writeEntityDef(char[] buf, int off, int len, Writer out)
        throws Exception, IOException, NumberFormatException
    {
        Integer ch;
        //System.out.println("Entity: " + new String(buf, off, len) +" "+off+" "+len);

        if (buf[off] == '#')
        {
            // character reference
            off++;
            len--;
            if (buf[off] == 'x' || buf[off] == 'X')
            {
                ch = Integer.valueOf(new String(buf, off + 1, len - 1), 16);
            }
            else
            {
                ch = Integer.valueOf(new String(buf, off, len));
            }
            out.write(ch.intValue());
        }
        else
        {
            String ent = new String(buf, off, len);
            String val = (String) entities.get(ent);
            if (val != null)
            {
                out.write(val);
            }
            else
            {
                out.write("&" + ent + ";");
                //tok.warning("unknown entity reference: " + ent);
            }
        }
    }


    /**
     * Description of the Method
     *
     * @param entity  Description of the Parameter
     * @param value   Description of the Parameter
     * @return        Description of the Return Value
     */
    public String defTextEntity(String entity, String value)
    {
        return (String) entities.put(entity, value);
    }


    /**
     * Returns the index within this String of the first occurrence of the
     * specified character, starting the search at fromIndex. This method
     * returns -1 if the character is not found.
     *
     * @param buf                         Description of the Parameter
     * @param ch                          Description of the Parameter
     * @param from                        Description of the Parameter
     * @param to                          Description of the Parameter
     * @return                            Description of the Return Value
     * @params                            buf the buffer to search
     * @params                            ch the character to search for
     * @params                            from the index to start the search
     *      from
     * @params                            to the highest possible index returned
     *      plus 1
     * @throws IndexOutOfBoundsException  if index out of bounds...
     */
    public final static int indexOf(char[] buf, int ch, int from, int to)
    {
        int i;
        for (i = from; i < to && buf[i] != ch; i++)
        {
            ;
        }
        // do nothing
        if (i < to)
        {
            return i;
        }
        else
        {
            return -1;
        }
    }

}
