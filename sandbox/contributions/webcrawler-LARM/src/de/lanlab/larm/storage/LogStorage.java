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

package de.lanlab.larm.storage;

import de.lanlab.larm.util.WebDocument;
import de.lanlab.larm.util.SimpleLogger;
import java.io.*;


/**
 * this class saves the documents into page files of 50 MB and keeps a record of all
 * the positions into a Logger. the log file contains URL, page file number, and
 * index within the page file.
 *
 */

public class LogStorage implements DocumentStorage
{

    SimpleLogger log;

    File pageFile;
    FileOutputStream out;
    int pageFileCount;
    String filePrefix;
    int offset;
    boolean isValid = false;
    /**
     * Description of the Field
     */
    public final static int MAXLENGTH = 50000000;
    boolean logContents = false;
    String fileName;


    /**
     * Constructor for the LogStorage object
     *
     * @param log          the logger where index information is saved to
     * @param logContents  whether all docs are to be stored in page files or not
     * @param filePrefix   the file name where the page file number is appended
     */
    public LogStorage(SimpleLogger log, boolean logContents, String filePrefix)
    {
        this.log = log;
        pageFileCount = 0;
        this.filePrefix = filePrefix;
        this.logContents = logContents;
        if (logContents)
        {
            openPageFile();
        }
    }


    /**
     * Description of the Method
     */
    public void open() { }


    /**
     * Description of the Method
     */
    public void openPageFile()
    {
        int id = ++pageFileCount;
        fileName = filePrefix + "_" + id + ".pfl";
        try
        {
            this.offset = 0;
            out = new FileOutputStream(fileName);
            isValid = true;
        }
        catch (IOException io)
        {
            log.logThreadSafe("**ERROR: IOException while opening pageFile " + fileName + ": " + io.getClass().getName() + "; " + io.getMessage());
            isValid = false;
        }
    }


    /**
     * Gets the outputStream attribute of the LogStorage object
     *
     * @return   The outputStream value
     */
    public OutputStream getOutputStream()
    {
        if (offset > MAXLENGTH)
        {
            try
            {
                out.close();
            }
            catch (IOException io)
            {
                log.logThreadSafe("**ERROR: IOException while closing pageFile " + fileName + ": " + io.getClass().getName() + "; " + io.getMessage());
            }
            openPageFile();
        }
        return out;
    }


    /**
     * Description of the Method
     *
     * @param bytes  Description of the Parameter
     * @return       Description of the Return Value
     */
    public synchronized int writeToPageFile(byte[] bytes)
    {
        try
        {
            OutputStream out = getOutputStream();
            int oldOffset = this.offset;
            out.write(bytes);
            this.offset += bytes.length;
            return oldOffset;
        }
        catch (IOException io)
        {
            log.logThreadSafe("**ERROR: IOException while writing " + bytes.length + " bytes to pageFile " + fileName + ": " + io.getClass().getName() + "; " + io.getMessage());
        }
        return -1;
    }


    /**
     * Sets the logger attribute of the LogStorage object
     *
     * @param log  The new logger value
     */
    public void setLogger(SimpleLogger log)
    {
        this.log = log;
    }


    /**
     * writes file info to log file;
     * stores the document if storing is enabled. in that case the log line contains
     * the page file number and the index within that file
     *
     * @param doc  Description of the Parameter
     * @return the unchanged document
     */
    public WebDocument store(WebDocument doc)
    {
        String docInfo = doc.getInfo();
        if (logContents && isValid && doc.getField("content") != null)
        {
            int offset = writeToPageFile((byte[])doc.getField("content"));
            docInfo = docInfo + "\t" + pageFileCount + "\t" + offset;
        }
        log.logThreadSafe(docInfo);
        return doc;
    }
}
