package de.lanlab.larm.util;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.*;
import java.util.*;
import de.lanlab.larm.parser.*;
import java.net.*;
import de.lanlab.larm.fetcher.*;
import de.lanlab.larm.net.*;

/**
 * Utility class for accessing page files through the store.log file.
 * Works like an iterator
 */
public class StoreLogFile implements Iterator
{

    public void remove()
    {
        throw new UnsupportedOperationException();
    }


    /**
     * @author Clemens Marschner
     * @version 1.0
     */
    public class PageFileEntry
    {
        String url;
        int pageFileNo;
        int resultCode;
        String mimeType;
        int size;
        String title;
        int pageFileOffset;
        File pageFileDirectory;
        boolean hasPageFileEntry;
        int isFrame;

        class PageFileInputStream extends InputStream
        {
            InputStream pageFileIS;
            long offset;

            public PageFileInputStream() throws IOException
            {
                pageFileIS = new FileInputStream(new File(pageFileDirectory, "pagefile_" + pageFileNo + ".pfl"));
                offset = 0;
                pageFileIS.skip(pageFileOffset);
            }
            public int available() throws IOException
            {
                return Math.min(pageFileIS.available(), (int)(size - offset));
            }
            public void close() throws IOException
            {
                pageFileIS.close();
            }
            public void mark(int readLimit)
            {
                throw new UnsupportedOperationException();
            }
            public boolean markSupported()
            {
                return false;
            }
            public int read() throws IOException
            {
                if(offset >= size)
                {
                    return -1;
                }
                int c = pageFileIS.read();
                if(c != -1)
                {
                    offset ++;
                }
                return c;
            }

            public int read(byte[] b) throws IOException
            {
                int len = Math.min((int)(size-offset), b.length);
                if(len > 0)
                {
                    len = pageFileIS.read(b, 0, len);
                    if(len != -1)
                    {
                        offset += len;
                    }
                    return len;
                }
                return -1;
            }
            public int read(byte[] b, int off, int maxLen) throws IOException
            {
                int len = Math.min(Math.min((int)(size-offset), b.length), maxLen);
                if(len > 0)
                {
                    len = pageFileIS.read(b, off, maxLen);
                    if(len != -1)
                    {
                        offset += len;
                    }
                    return len;
                }
                return -1;
            }
            public long skip(long n) throws IOException
            {
                n = Math.min(n, size-offset);
                n = pageFileIS.skip(n);
                if(n > 0)
                {
                    offset+=n;
                }
                return n;
            }



        }

        public PageFileEntry(String storeLogLine, File pageFileDirectory)
        {
            String column=null;
            SimpleStringTokenizer t = new SimpleStringTokenizer(storeLogLine, '\t');
            try
            {

                hasPageFileEntry = false;
                t.nextToken();
                url = t.nextToken();
                column = "isFrame";
                isFrame = Integer.parseInt(t.nextToken());
                t.nextToken(); // anchor
                column = "resultCode";
                resultCode = Integer.parseInt(t.nextToken());
                mimeType = t.nextToken();
                column = "size";
                size = Integer.parseInt(t.nextToken());
                title = t.nextToken();
                if(size > 0)
                {
                    column = "pageFileNo";
                    pageFileNo = Integer.parseInt(t.nextToken());
                    column = "pageFileOffset";
                    pageFileOffset = Integer.parseInt(t.nextToken());
                    this.pageFileDirectory = pageFileDirectory;
                    hasPageFileEntry = true;
                }
            }
            catch(NumberFormatException e) // possibly tab characters in title. ignore
            {
                //System.out.println(e + " at " + url + " in column " + column);
            }
        }

        public InputStream getInputStream()  throws IOException
        {
            if(hasPageFileEntry)
            {
                return new PageFileInputStream();
            }
            else return null;
        }

    }

    BufferedReader reader;
    boolean isOpen = false;
    File storeLog;

    /**
     *
     * @param storeLog location of store.log from LogStorage. pagefile_xy.pfl
     * must be in the same directory
     * @throws IOException
     */
    public StoreLogFile(File storeLog) throws IOException
    {
        this.storeLog = storeLog;
         reader = new BufferedReader(new FileReader(storeLog));
         isOpen = true; // unless exception

    }

    public boolean hasNext()
    {
        try
        {
            reader.mark(1000);
            if(reader.readLine() != null)
            {
                reader.reset();
                return true;
            }
            else
            {
                return false;
            }
        }
        catch(IOException e)
        {
            throw new RuntimeException("IOException occured");
        }
    }

    /**
     * @return a StoreLogFile.PageFileEntry with the current file
     * @throws IOException
     */
    public Object next()
    {
        try
        {
            return new PageFileEntry(reader.readLine(), storeLog.getParentFile());
        }
        catch(IOException e)
        {
            throw new RuntimeException("IOException occured");
        }
    }




//    static SimpleLogger log;
//    static PageFileEntry entry;
//    static ArrayList foundURLs;
//    static URL base;
//    static URL contextUrl;
//
//    static void test1(StoreLogFile store) throws IOException
//    {
//        while(store.hasNext())
//        {
//            PageFileEntry entry = store.next();
//            if(entry.mimeType.equals("text/plain") && entry.hasPageFileEntry)
//            {
//                BufferedReader r = new BufferedReader(new InputStreamReader(entry.getInputStream()));
//                String l;
//                while((l = r.readLine()) != null)
//                {
//                    System.out.println(entry.url + " >> " + l);
//                }
//                r.close();
//            }
//            //System.out.println(entry.title);
//        }
//    }
//    static void test2(StoreLogFile store) throws Exception
//    {
//        MessageHandler msgH = new MessageHandler();
//        log = new SimpleLogger("errors.log");
//        msgH.addListener(new URLVisitedFilter(log, 100000));
//        final de.lanlab.larm.net.HostManager hm = new de.lanlab.larm.net.HostManager(1000);
//        hm.setHostResolver(new HostResolver());
//
//        while(store.hasNext())
//        {
//            entry = store.next();
//            foundURLs = new ArrayList();
//            if(entry.mimeType.startsWith("text/html") && entry.hasPageFileEntry)
//            {
//                Tokenizer t = new Tokenizer();
//                base = new URL(entry.url);
//                contextUrl = new URL(entry.url);
//
//                t.setLinkHandler(new LinkHandler()
//                {
//
//                    public void handleLink(String link, String anchor, boolean isFrame)
//                    {
//                        try
//                        {
//                            // cut out Ref part
//
//
//                            int refPart = link.indexOf("#");
//                            //System.out.println(link);
//                            if (refPart == 0)
//                            {
//                                return;
//                            }
//                            else if (refPart > 0)
//                            {
//                                link = link.substring(0, refPart);
//                            }
//
//                            URL url = null;
//                            if (link.startsWith("http:"))
//                            {
//                                // distinguish between absolute and relative URLs
//
//                                url = new URL(link);
//                            }
//                            else
//                            {
//                                // relative url
//                                url = new URL(base, link);
//                            }
//
//                            URLMessage urlMessage =  new URLMessage(url, contextUrl, isFrame ? URLMessage.LINKTYPE_FRAME : URLMessage.LINKTYPE_ANCHOR, anchor, hm.getHostResolver());
//
//                            String urlString = urlMessage.getURLString();
//
//                            foundURLs.add(urlMessage);
//                            //messageHandler.putMessage(new actURLMessage(url)); // put them in the very end
//                        }
//                        catch (MalformedURLException e)
//                        {
//                            //log.log("malformed url: base:" + base + " -+- link:" + link);
//                            log.log("warning: " + e.getClass().getName() + ": " + e.getMessage());
//                        }
//                        catch (Exception e)
//                        {
//                            log.log("warning: " + e.getClass().getName() + ": " + e.getMessage());
//                            // e.printStackTrace();
//                        }
//
//                    }
//
//
//                    /**
//                     * called when a BASE tag was found
//                     *
//                     * @param base  the HREF attribute
//                     */
//                    public void handleBase(String baseString)
//                    {
//                        try
//                        {
//                            base = new URL(baseString);
//                        }
//                        catch (MalformedURLException e)
//                        {
//                            log.log("warning: " + e.getClass().getName() + ": " + e.getMessage() + " while converting '" + base + "' to URL in document " + contextUrl);
//                        }
//                    }
//
//                    public void handleTitle(String value)
//                    {}
//
//
//                });
//                t.parse(new BufferedReader(new InputStreamReader(entry.getInputStream())));
//                msgH.putMessages(foundURLs);
//            }
//
//        }
//
//    }
//
//    public static void main(String[] args) throws Exception
//    {
//        StoreLogFile store = new StoreLogFile(new File("c:/java/jakarta-lucene-sandbox/contributions/webcrawler-LARM/logs/store.log"));
//        test2(store);
//    }

}

