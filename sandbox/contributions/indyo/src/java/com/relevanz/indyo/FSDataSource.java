package com.relevanz.indyo;

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

import org.apache.lucene.document.DateField;
import com.relevanz.indyo.contenthandler.FileContentHandler;
import com.relevanz.indyo.contenthandler.FileContentHandlerFactory;
import com.relevanz.indyo.util.IOUtils;

import java.io.File;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A filesystem-based datasource.
 *
 * @author <a href="mailto:kelvint@apache.org">Kelvin Tan</a>
 * @version $Id$
 */
public class FSDataSource extends AbstractDataSource
{
    public static final String FILE_PATH_FIELD = "filePath";
    public static final String FILE_NAME_FIELD = "fileName";
    public static final String FILE_SIZE_FIELD = "fileSize";
    public static final String FILE_FORMAT_FIELD = "fileFormat";
    public static final String FILE_CONTENTS_FIELD = "fileContents";
    public static final String FILE_LAST_MODIFIED_DATE_FIELD = "fileLastModifiedDate";

    private File targetFileOrDir;

    public FSDataSource(String targetFileOrDirStr)
    {
        this(new File(targetFileOrDirStr));
    }

    public FSDataSource(File targetFileOrDir)
    {
        setTargetDirectory(targetFileOrDir);
    }

    public Map[] getData()
    {
        Map[] returnData = null;
        List temp = new ArrayList();
        loadDataFromFiles(targetFileOrDir, temp);
        returnData = new Map[temp.size()];
        returnData = (Map[]) temp.toArray(returnData);
        return returnData;
    }

    public void setTargetDirectory(File targetFileOrDir)
    {
        this.targetFileOrDir = targetFileOrDir;
    }

    private void loadDataFromFiles(File f, List list)
    {
        if (f.isDirectory())
        {
            File[] directoryTree = f.listFiles();
            for (int i = 0; i < directoryTree.length; i++)
            {
                loadDataFromFiles(directoryTree[i], list);
            }
        }
        else
        {
            Map dataMap = new HashMap();
            dataMap.put(FILE_PATH_FIELD, f.getPath());
            dataMap.put(FILE_NAME_FIELD, f.getName());
            dataMap.put(FILE_LAST_MODIFIED_DATE_FIELD,
                        DateField.timeToString(f.lastModified()));
            dataMap.put(FILE_SIZE_FIELD, String.valueOf(f.length()));
            dataMap.put(FILE_FORMAT_FIELD,
                        IOUtils.getFileExtension(f));
            addFileContents(f, dataMap);
            list.add(dataMap);
        }
    }

    private void addFileContents(File targetFile, Map dataMap)
    {
        FileContentHandler cHandler =
                FileContentHandlerFactory.getContentHandler(targetFile);
        if (cHandler != null)
        {
            if (cHandler.fileContentIsReadable())
            {
                Reader r = cHandler.getReader();
                if (r != null)
                {
                    dataMap.put(FILE_CONTENTS_FIELD, r);
                }
            }
            if (cHandler.containsNestedData())
            {
                dataMap.put(NESTED_DATASOURCE, cHandler.getNestedDataSource());
            }
        }
        else
        {
            //cat.warn("ContentHandler not found for " + contentFile.getName());
        }
    }
}
