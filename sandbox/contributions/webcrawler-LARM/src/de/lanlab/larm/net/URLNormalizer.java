package de.lanlab.larm.net;
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
import java.io.*;
import java.net.*;
import org.apache.oro.text.perl.*;


/**
 * Description of the Class
 *
 * @author    Administrator
 * @created   14. Juni 2002
 */
public class URLNormalizer
{
    final static int NP_SLASH = 1;
    final static int NP_CHAR = 2;
    final static int NP_PERCENT = 3;
    final static int NP_POINT = 4;
    final static int NP_HEX = 5;

    /**
     * contains hex codes for characters in lowercase uses char arrays instead
     * of strings for faster processing
     */
    protected final static char[][] charMap = {
            {'%', '0', '0'}, {'%', '0', '1'}, {'%', '0', '2'}, {'%', '0', '3'}, {'%', '0', '4'}, {'%', '0', '5'}, {'%', '0', '6'}, {'%', '0', '7'}, {'%', '0', '8'}, {'%', '0', '9'}, {'%', '0', 'A'}, {'%', '0', 'B'}, {'%', '0', 'C'}, {'%', '0', 'D'}, {'%', '0', 'E'}, {'%', '0', 'F'},
            {'%', '1', '0'}, {'%', '1', '1'}, {'%', '1', '2'}, {'%', '1', '3'}, {'%', '1', '4'}, {'%', '1', '5'}, {'%', '1', '6'}, {'%', '1', '7'}, {'%', '1', '8'}, {'%', '1', '9'}, {'%', '1', 'A'}, {'%', '1', 'B'}, {'%', '1', 'C'}, {'%', '1', 'D'}, {'%', '1', 'E'}, {'%', '1', 'F'},
            {'%', '2', '0'}, {'%', '2', '1'}, {'%', '2', '2'}, {'%', '2', '3'}, {'$'}, {'%', '2', '5'}, {'%', '2', '6'}, {'%', '2', '7'}, {'%', '2', '8'}, {'%', '2', '9'}, {'%', '2', 'A'}, {'%', '2', 'B'}, {'%', '2', 'C'}, {'-'}, {'.'}, {'%', '2', 'F'},
            {'0'}, {'1'}, {'2'}, {'3'}, {'4'}, {'5'}, {'6'}, {'7'}, {'8'}, {'9'}, {'%', '3', 'A'}, {'%', '3', 'B'}, {'%', '3', 'C'}, {'%', '3', 'D'}, {'%', '3', 'E'}, {'%', '3', 'F'},
            {'%', '4', '0'}, {'a'}, {'b'}, {'c'}, {'d'}, {'e'}, {'f'}, {'g'}, {'h'}, {'i'}, {'j'}, {'k'}, {'l'}, {'m'}, {'n'}, {'o'},
            {'p'}, {'q'}, {'r'}, {'s'}, {'t'}, {'u'}, {'v'}, {'w'}, {'x'}, {'y'}, {'z'}, {'%', '5', 'B'}, {'%', '5', 'C'}, {'%', '5', 'D'}, {'%', '5', 'E'}, {'_'},
            {'%', '6', '0'}, {'a'}, {'b'}, {'c'}, {'d'}, {'e'}, {'f'}, {'g'}, {'h'}, {'i'}, {'j'}, {'k'}, {'l'}, {'m'}, {'n'}, {'o'},
            {'p'}, {'q'}, {'r'}, {'s'}, {'t'}, {'u'}, {'v'}, {'w'}, {'x'}, {'y'}, {'z'}, {'%', '7', 'B'}, {'%', '7', 'C'}, {'%', '7', 'D'}, {'%', '7', 'E'}, {'%', '7', 'F'},
            {'%', '8', '0'}, {'%', '8', '1'}, {'%', '8', '2'}, {'%', '8', '3'}, {'%', '8', '4'}, {'%', '8', '5'}, {'%', '8', '6'}, {'%', '8', '7'}, {'%', '8', '8'}, {'%', '8', '9'}, {'%', '8', 'A'}, {'%', '8', 'B'}, {'%', '8', 'C'}, {'%', '8', 'D'}, {'%', '8', 'E'}, {'%', '8', 'F'},
            {'%', '9', '0'}, {'%', '9', '1'}, {'%', '9', '2'}, {'%', '9', '3'}, {'%', '9', '4'}, {'%', '9', '5'}, {'%', '9', '6'}, {'%', '9', '7'}, {'%', '9', '8'}, {'%', '9', '9'}, {'%', '9', 'A'}, {'%', '9', 'B'}, {'%', '9', 'C'}, {'%', '9', 'D'}, {'%', '9', 'E'}, {'%', '9', 'F'},
            {'%', 'A', '0'}, {'%', 'A', '1'}, {'%', 'A', '2'}, {'%', 'A', '3'}, {'%', 'A', '4'}, {'%', 'A', '5'}, {'%', 'A', '6'}, {'%', 'A', '7'}, {'%', 'A', '8'}, {'%', 'A', '9'}, {'%', 'A', 'A'}, {'%', 'A', 'B'}, {'%', 'A', 'C'}, {'%', 'A', 'D'}, {'%', 'A', 'E'}, {'%', 'A', 'F'},
            {'%', 'B', '0'}, {'%', 'B', '1'}, {'%', 'B', '2'}, {'%', 'B', '3'}, {'%', 'B', '4'}, {'%', 'B', '5'}, {'%', 'B', '6'}, {'%', 'B', '7'}, {'%', 'B', '8'}, {'%', 'B', '9'}, {'%', 'B', 'A'}, {'%', 'B', 'B'}, {'%', 'B', 'C'}, {'%', 'B', 'D'}, {'%', 'B', 'E'}, {'%', 'B', 'F'},
            {'%', 'E', '0'}, {'%', 'E', '1'}, {'%', 'E', '2'}, {'%', 'E', '3'}, {'%', 'E', '4'}, {'%', 'E', '5'}, {'%', 'E', '6'}, {'%', 'E', '7'}, {'%', 'E', '8'}, {'%', 'E', '9'}, {'%', 'E', 'A'}, {'%', 'E', 'B'}, {'%', 'E', 'C'}, {'%', 'E', 'D'}, {'%', 'E', 'E'}, {'%', 'E', 'F'},
            {'%', 'F', '0'}, {'%', 'F', '1'}, {'%', 'F', '2'}, {'%', 'F', '3'}, {'%', 'F', '4'}, {'%', 'F', '5'}, {'%', 'F', '6'}, {'%', 'D', '7'}, {'%', 'F', '8'}, {'%', 'F', '9'}, {'%', 'F', 'A'}, {'%', 'F', 'B'}, {'%', 'F', 'C'}, {'%', 'F', 'D'}, {'%', 'F', 'E'}, {'%', 'D', 'F'},
            {'%', 'E', '0'}, {'%', 'E', '1'}, {'%', 'E', '2'}, {'%', 'E', '3'}, {'%', 'E', '4'}, {'%', 'E', '5'}, {'%', 'E', '6'}, {'%', 'E', '7'}, {'%', 'E', '8'}, {'%', 'E', '9'}, {'%', 'E', 'A'}, {'%', 'E', 'B'}, {'%', 'E', 'C'}, {'%', 'E', 'D'}, {'%', 'E', 'E'}, {'%', 'E', 'F'},
            {'%', 'F', '0'}, {'%', 'F', '1'}, {'%', 'F', '2'}, {'%', 'F', '3'}, {'%', 'F', '4'}, {'%', 'F', '5'}, {'%', 'F', '6'}, {'%', 'F', '7'}, {'%', 'F', '8'}, {'%', 'F', '9'}, {'%', 'F', 'A'}, {'%', 'F', 'B'}, {'%', 'F', 'C'}, {'%', 'F', 'D'}, {'%', 'F', 'E'}, {'%', 'F', 'F'},
            };


    /**
     * Description of the Method
     *
     * @param path             Description of the Parameter
     * @return                 Description of the Return Value
     * @exception IOException  Description of the Exception
     */
    protected static String normalizePath(String path)
        throws IOException
    {
        // rule 1: if the path is empty, return "/"
        if (path.length() == 0)
        {
            return "/";
        }

        // Finite State Machine to convert characters to lowercase, remove "//" and "/./"
        // and make sure that all characters are escaped in a uniform way, i.e.
        // {" ", "+", "%20"} -> "%20"

        StringBuffer w = new StringBuffer((int) (path.length() * 1.5));

        int status = NP_CHAR;

        int pos = 0;
        int length = path.length();
        char savedChar = '?';
        int hexChar = '?';
        int pathPos = -1;    // position of last "/"
        int questionPos = -1; // assert length >0
        boolean isInQuery = false;  // question mark reached?

        while (pos < length)
        {
            char c = path.charAt(pos++);
            try
            {
                switch (status)
                {
                    case NP_SLASH:
                        if (c == '/')
                        {
                            // ignore subsequent slashes
                        }
                        else if (c == '.')
                        {
                            status = NP_POINT;
                        }
                        else if (c == '%')
                        {
                            status = NP_PERCENT;
                        }
                        else
                        {
                            pos--;
                            status = NP_CHAR;
                        }
                        break;
                    case NP_POINT:
                        if (c == '/')
                        {
                            // ignore
                        }
                        else if (c == '.')
                        {
                            // ignore; this shouldn't happen
                        }
                        else
                        {
                            w.append('.');
                            pos--;
                            status = NP_SLASH;
                        }
                        break;
                    case NP_PERCENT:
                        if (c >= '0' && c <= '9')
                        {
                            hexChar = (c - '0') << 4;
                        }
                        else if (c >= 'a' && c <= 'f')
                        {
                            hexChar = (c - 'a' + 10) << 4;
                        }
                        else if (c >= 'A' && c <= 'F')
                        {
                            hexChar = (c - 'A' + 10) << 4;
                        }
                        else
                        {
                            w.append(charMap['%']);
                            w.append(charMap[c]);
                            break;
                        }
                        savedChar = c;
                        status = NP_HEX;
                        break;
                    case NP_HEX:
                        if (c >= '0' && c <= '9')
                        {
                            hexChar |= (c - '0');
                        }
                        else if (c >= 'a' && c <= 'f')
                        {
                            hexChar |= (c - 'a' + 10);
                        }
                        else if (c >= 'A' && c <= 'F')
                        {
                            hexChar |= (c - 'A' + 10);
                        }
                        else
                        {
                            w.append(charMap['%']);
                            w.append(charMap[savedChar]);
                            w.append(charMap[c]);
                            break;
                        }
                        w.append(charMap[hexChar]);
                        status = NP_CHAR;
                        break;
                    case NP_CHAR:
                        switch (c)
                        {
                            case '%':
                                status = NP_PERCENT;
                                break;
                            case '/':
                                if(!isInQuery)
                                {
                                    w.append(c);
                                    pathPos = w.length(); // points to the char. after "/"
                                    status = NP_SLASH;
                                }
                                else
                                {
                                    w.append(charMap[c]);
                                }
                                break;
                            case '?':
                                if(!isInQuery)
                                {
                                    if(pathPos == -1)
                                    {
                                        w.append('/');
                                        pathPos = w.length();
                                    }
                                    questionPos = w.length(); // points to the char at "?"
                                    isInQuery = true;
                                }
                                else
                                {
                                    w.append(charMap[c]);
                                    break;
                                }
                            case '&':
                            case ';':
                            case '@':
                            //case ':':
                            case '=':
                                w.append(c);
                                break;
                            case '+':
                                w.append("%20");
                                break;
                            default:
                                w.append(charMap[c]);
                                break;
                        }
                }

            }
            catch (ArrayIndexOutOfBoundsException e)
            {
                // we encountered a unicode character >= 0x00ff
                // write UTF-8 to distinguish it from other characters
                // note that this does NOT lead to a pure UTF-8 URL since we
                // write 0x80 <= c <= 0xff as one-byte strings
                /*
                 *  if (ch <= 0x007f) {		// other ASCII
                 *  sbuf.append(hex[ch]);
                 *  } else
                 */
                // note that we ignore the case that we receive "%" + unicode + c
                // (status = NP_HEX + Exception when writing savedchar); in that case
                // only the second character is written. we consider this to be very
                // unlikely

                // see http://www.w3.org/International/O-URL-code.html
                if (c <= 0x07FF)
                {
                    // non-ASCII <= 0x7FF
                    w.append(charMap[0xc0 | (c >> 6)]);
                    w.append(charMap[0x80 | (c & 0x3F)]);
                }
                else
                {
                    // 0x7FF < c <= 0xFFFF
                    w.append(charMap[0xe0 | (c >> 12)]);
                    w.append(charMap[0x80 | ((c >> 6) & 0x3F)]);
                    w.append(charMap[0x80 | (c & 0x3F)]);
                }
            }
        }

        // rule 3: delete index.* or default.*

        if(questionPos == -1) // no query
        {
            questionPos = w.length();
        }
        else
        {
            if(questionPos == w.length()-1)
            {
                // empty query. assert questionPos > 0
                w.deleteCharAt(questionPos);
            }
        }
        if(pathPos == -1) // no query
        {
            pathPos = 0;
        }
        if(questionPos > pathPos)
        {
            String file = w.substring(pathPos, questionPos);
            {
                //System.out.println("file: " + file);
                if(file.startsWith("index.") || file.startsWith("default."))
                {
                    w.delete(pathPos, questionPos); // delete default page to avoid ambiguities
                }
            }
        }
        return w.toString();
    }


    /**
     * Description of the Method
     *
     * @param host  Description of the Parameter
     * @return      Description of the Return Value
     */
    protected static String normalizeHost(HostResolver hostResolver, String host)
    {
        return hostResolver.resolveHost(host.toLowerCase());
    }




    HostResolver hostResolver;


    /**
     * Constructor for the URLNormalizer object
     *
     * @param hostManager  Description of the Parameter
     */
    public URLNormalizer(HostResolver hostResolver)
    {
        this.hostResolver = hostResolver;
    }

    public void setHostResolver(HostResolver hostResolver)
    {
        this.hostResolver = hostResolver;
    }

    /**
     * Description of the Method
     *
     * @param u                          Description of the Parameter
     * @return                           Description of the Return Value
     * @exception IOException            Description of the Exception
     * @exception MalformedURLException  Description of the Exception
     */
    public static URL normalize(URL u, HostResolver hostResolver)
    {
        if(u == null)
        {
            return null;
        }
        if (u.getProtocol().equals("http"))
        {
            try
            {
                int port = u.getPort();
                /*URL url =*/
                return  new URL(u.getProtocol(), normalizeHost(hostResolver, u.getHost()), port == 80 ? -1 : port, normalizePath(u.getFile()));
                /*if(!u.equals(url))
                {
                    System.out.println(u.toExternalForm() + " -> " + url.toExternalForm());
                }
                return url;*/
            }
            catch(MalformedURLException e)
            {
                System.out.println("assertion failed: MalformedURLException in URLNormalizer.normalize()");
                throw new java.lang.InternalError("assertion failed: MalformedURLException in URLNormalizer.normalize()");
            }
            catch(IOException e)
            {
                System.out.println("assertion failed: IOException in URLNormalizer.normalize()");
                throw new java.lang.InternalError("assertion failed: MalformedURLException in URLNormalizer.normalize()");
            }

            //return url
        }
        else
        {
            return u;
        }
    }


    public URL normalize(URL u)
    {
        return this.normalize(u, hostResolver);
    }

}
