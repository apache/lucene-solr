package search.contenthandler;

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

import org.apache.log4j.Category;
import search.DataSource;
import search.FSDataSource;
import search.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * Handles GZip content.
 *
 * @author <a href="mailto:kelvin@relevanz.com">Kelvin Tan</a>
 */
public class GZipHandler extends NestedFileContentHandlerAdapter
{
    private static Category cat = Category.getInstance(GZipHandler.class.getName());

    public GZipHandler(File file)
    {
        super(file);
    }

    public Reader getReader()
    {
        return null;
    }

    public List getNestedDataSource()
    {
        if (!file.exists())
            return null;
        try
        {
            File tempDir = new File(TEMP_FOLDER);
            tempDir.mkdirs();
            tempDir.deleteOnExit();
            String filename = file.getName();
            File tempFile = new File(tempDir, filename.substring(0, filename.lastIndexOf(".")));
            tempFile.deleteOnExit();
            IOUtils.extractGZip(file, tempFile);
            indexGZipDirectory(tempDir);
        }
        catch (IOException ioe)
        {
            cat.error("IOException ungzipping " + file.toString(), ioe);
        }
        return nestedDataSource;
    }

    public boolean fileContentIsReadable()
    {
        return false;
    }

    // only one file, but let's just treat it like a directory anyway
    private void indexGZipDirectory(File dir)
    {
        if (dir.isDirectory())
        {
            File[] dirContents = dir.listFiles();
            for (int i = 0; i < dirContents.length; i++)
            {
                indexGZipDirectory(dirContents[i]);
            }
        }
        else if (dir.isFile())
        {
            DataSource ds = new FSDataSource(dir);
            nestedDataSource.add(nestedDataSource);
        }
    }
}