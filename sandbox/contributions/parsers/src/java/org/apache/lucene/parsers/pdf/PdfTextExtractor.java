package org.apache.lucene.parsers.pdf;

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
 * 4. The names "Apache" and "Apache Software Foundation"
 *    must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    nor may "Apache" appear in their name, without
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

import com.etymon.pj.Pdf;
import com.etymon.pj.exception.InvalidPdfObjectException;
import com.etymon.pj.exception.PjException;
import com.etymon.pj.object.PjArray;
import com.etymon.pj.object.PjObject;
import com.etymon.pj.object.PjPage;
import com.etymon.pj.object.PjStream;
import org.apache.log4j.Category;

import java.io.File;
import java.io.IOException;
import java.util.Vector;

/**
 * <p>
 * Attempts to extract text from a PDF file.
 * </p>
 * <p>
 * <a href="http://www.mail-archive.com/lucene-user@jakarta.apache.org/msg00280.html">
 * Known limitations</a>
 * </p>
 *
 * @author <a href="mailto:kelvint@apache.org">Kelvin Tan</a>
 * @version $Revision$
 */
public class PdfTextExtractor
{
    private static Category cat = Category.getInstance(PdfTextExtractor.class);

    public static void main(String[] args)
    {
        File f = new File("/usr/local/test.pdf");
        try
        {
            Pdf pdf = new Pdf(f.toString());
            int pagecount = pdf.getPageCount();
            cat.debug(f.toString() + "has " + pagecount + " pages.");
            for (int i = 1; i <= pagecount; i++)
            {
                System.out.println(getContent(pdf, i));
            }
        }
        catch (IOException ioe)
        {
            cat.error("IOException parsing PDF file:" + f.toString(), ioe);
        }
        catch (PjException pje)
        {
            cat.error("PjException parsing PDF file:" + f.toString(), pje);
        }
    }

    private static String getContent(Pdf pdf, int pageNo)
    {
        String content = null;
        PjStream stream = null;
        StringBuffer strbf = new StringBuffer();
        try
        {
            PjPage page = (PjPage) pdf.getObject(pdf.getPage(pageNo));
            PjObject pobj = (PjObject) pdf.resolve(page.getContents());
            if (pobj instanceof PjArray)
            {
                PjArray array = (PjArray) pobj;
                Vector vArray = array.getVector();
                int size = vArray.size();
                for (int j = 0; j < size; j++)
                {
                    stream = (PjStream) pdf.resolve((PjObject) vArray.get(j));
                    strbf.append(getStringFromPjStream(stream));
                }
                content = strbf.toString();
            }
            else
            {
                stream = (PjStream) pobj;
                content = getStringFromPjStream(stream);
            }
        }
        catch (InvalidPdfObjectException pdfe)
        {
            cat.error("Invalid PDF Object:" + pdfe, pdfe);
        }
        catch (Exception e)
        {
            cat.error("Exception in getContent() " + e, e);
        }
        return content;
    }

    private static String getStringFromPjStream(PjStream stream)
    {
        StringBuffer strbf = new StringBuffer();
        try
        {
            int start,end = 0;
            stream = stream.flateDecompress();
            String longString = stream.toString();
            int strlen = longString.length();
            int lastIndex = longString.lastIndexOf(')');
            while (lastIndex != -1 && end != lastIndex)
            {
                start = longString.indexOf('(', end);
                end = longString.indexOf(')', start);
                String text = longString.substring(start + 1, end);
                strbf.append(text);
            }
        }
        catch (InvalidPdfObjectException pdfe)
        {
            cat.error("InvalidObjectException:" + pdfe.getMessage(), pdfe);
        }
        return strbf.toString();
    }
}

