package com.relevanz.indyo.contenthandler;

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
import com.relevanz.indyo.IndexDataSource;
import com.relevanz.indyo.FSDataSource;
import com.relevanz.indyo.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles Tar files.
 *
 * @author <a href="mailto:kelvint@apache.org">Kelvin Tan</a>
 * @version $Id$ 
 */
public class TARHandler extends NestedFileContentHandlerAdapter
{
    static Category cat = Category.getInstance(TARHandler.class.getName());

    public TARHandler(File file)
    {
        super(file);
    }

    public Reader getReader()
    {
        return null;
    }

    public boolean fileContentIsReadable()
    {
        return false;
    }

    public List getNestedDataSource()
    {
        if (!file.exists())
            return null;
        if (nestedDataSource == null)
        {
            nestedDataSource = new ArrayList();
        }
        try
        {
            File tempDir = new File(TEMP_FOLDER);
            tempDir.deleteOnExit();
            IOUtils.extractTar(file, tempDir);
            indexTarDirectory(tempDir);
        }
        catch (IOException ioe)
        {
            cat.error(ioe.getMessage(), ioe);
        }
        return nestedDataSource;
    }

    private void indexTarDirectory(File dir)
    {
        if (dir.isDirectory())
        {
            File[] dirContents = dir.listFiles();
            for (int i = 0; i < dirContents.length; i++)
            {
                indexTarDirectory(dirContents[i]);
            }
        }
        else if (dir.isFile())
        {
            // here create new DataMap for the tarred file
            IndexDataSource ds = new FSDataSource(dir);
            nestedDataSource.add(nestedDataSource);
        }
    }
}