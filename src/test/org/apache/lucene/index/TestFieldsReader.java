package org.apache.lucene.index;

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

import junit.framework.TestCase;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.search.Similarity;

import java.util.Map;
import java.io.IOException;

public class TestFieldsReader extends TestCase {
  private RAMDirectory dir = new RAMDirectory();
  private Document testDoc = new Document();
  private FieldInfos fieldInfos = null;

  public TestFieldsReader(String s) {
    super(s);
  }

  protected void setUp() {
    fieldInfos = new FieldInfos();
    DocHelper.setupDoc(testDoc);
    fieldInfos.add(testDoc);
    DocumentWriter writer = new DocumentWriter(dir, new WhitespaceAnalyzer(),
            Similarity.getDefault(), 50);
    assertTrue(writer != null);
    try {
      writer.addDocument("test", testDoc);
    }
    catch (IOException e)
    {
      
    }
  }

  protected void tearDown() {

  }

  public void test() {
    assertTrue(dir != null);
    assertTrue(fieldInfos != null);
    try {
      FieldsReader reader = new FieldsReader(dir, "test", fieldInfos);
      assertTrue(reader != null);
      assertTrue(reader.size() == 1);
      Document doc = reader.doc(0);
      assertTrue(doc != null);
      assertTrue(doc.getField("textField1") != null);
      Field field = doc.getField("textField2");
      assertTrue(field != null);
      assertTrue(field.isTermVectorStored() == true);
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
}