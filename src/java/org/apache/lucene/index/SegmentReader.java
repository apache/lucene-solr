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

import java.io.IOException;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Vector;

import org.apache.lucene.util.BitVector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.InputStream;
import org.apache.lucene.document.Document;

final class SegmentReader extends IndexReader {
  Directory directory;
  private boolean closeDirectory = false;
  private String segment;

  FieldInfos fieldInfos;
  private FieldsReader fieldsReader;

  TermInfosReader tis;
  
  BitVector deletedDocs = null;
  private boolean deletedDocsDirty = false;

  private InputStream freqStream;
  private InputStream proxStream;


  private static class Norm {
    public Norm(InputStream in) { this.in = in; }
    public InputStream in;
    public byte[] bytes;
  }
  private Hashtable norms = new Hashtable();

  SegmentReader(SegmentInfo si, boolean closeDir)
       throws IOException {
    this(si);
    closeDirectory = closeDir;
  }

  SegmentReader(SegmentInfo si)
       throws IOException {
    directory = si.dir;
    segment = si.name;

    fieldInfos = new FieldInfos(directory, segment + ".fnm");
    fieldsReader = new FieldsReader(directory, segment, fieldInfos);

    tis = new TermInfosReader(directory, segment, fieldInfos);

    if (hasDeletions(si))
      deletedDocs = new BitVector(directory, segment + ".del");

    // make sure that all index files have been read or are kept open
    // so that if an index update removes them we'll still have them
    freqStream = directory.openFile(segment + ".frq");
    proxStream = directory.openFile(segment + ".prx");
    openNorms();
  }
  
  public final synchronized void close() throws IOException {
    if (deletedDocsDirty) {
      synchronized (directory) {
	deletedDocs.write(directory, segment + ".tmp");
	directory.renameFile(segment + ".tmp", segment + ".del");
      }
      deletedDocsDirty = false;
    }

    fieldsReader.close();
    tis.close();

    if (freqStream != null)
      freqStream.close();
    if (proxStream != null)
      proxStream.close();

    closeNorms();

    if (closeDirectory)
      directory.close();
  }

  final static boolean hasDeletions(SegmentInfo si) throws IOException {
    return si.dir.fileExists(si.name + ".del");
  }

  public final synchronized void delete(int docNum) throws IOException {
    if (deletedDocs == null)
      deletedDocs = new BitVector(maxDoc());
    deletedDocsDirty = true;
    deletedDocs.set(docNum);
  }

  final Vector files() throws IOException {
    Vector files = new Vector(16);
    files.addElement(segment + ".fnm");
    files.addElement(segment + ".fdx");
    files.addElement(segment + ".fdt");
    files.addElement(segment + ".tii");
    files.addElement(segment + ".tis");
    files.addElement(segment + ".frq");
    files.addElement(segment + ".prx");

    if (directory.fileExists(segment + ".del"))
      files.addElement(segment + ".del");

    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed)
	files.addElement(segment + ".f" + i);
    }
    return files;
  }

  public final TermEnum terms() throws IOException {
    return tis.terms();
  }

  public final TermEnum terms(Term t) throws IOException {
    return tis.terms(t);
  }

  public final synchronized Document document(int n) throws IOException {
    if (isDeleted(n))
      throw new IllegalArgumentException
	("attempt to access a deleted document");
    return fieldsReader.doc(n);
  }

  public final synchronized boolean isDeleted(int n) {
    return (deletedDocs != null && deletedDocs.get(n));
  }

  public final TermDocs termDocs(Term t) throws IOException {
    TermInfo ti = tis.get(t);
    if (ti != null)
      return new SegmentTermDocs(this, ti);
    else
      return null;
  }

  final InputStream getFreqStream () {
    return (InputStream)freqStream.clone();
  }

  public final TermPositions termPositions(Term t) throws IOException {
    TermInfo ti = tis.get(t);
    if (ti != null)
      return new SegmentTermPositions(this, ti);
    else
      return null;
  }

  final InputStream getProxStream () {
    return (InputStream)proxStream.clone();
  }

  public final int docFreq(Term t) throws IOException {
    TermInfo ti = tis.get(t);
    if (ti != null)
      return ti.docFreq;
    else
      return 0;
  }

  public final int numDocs() {
    int n = maxDoc();
    if (deletedDocs != null)
      n -= deletedDocs.count();
    return n;
  }

  public final int maxDoc() {
    return fieldsReader.size();
  }

  public final byte[] norms(String field) throws IOException {
    Norm norm = (Norm)norms.get(field);
    if (norm == null)
      return null;
    if (norm.bytes == null) {
      byte[] bytes = new byte[maxDoc()];
      norms(field, bytes, 0);
      norm.bytes = bytes;
    }
    return norm.bytes;
  }

  final void norms(String field, byte[] bytes, int offset) throws IOException {
    InputStream normStream = normStream(field);
    if (normStream == null)
      return;					  // use zeros in array
    try {
      normStream.readBytes(bytes, offset, maxDoc());
    } finally {
      normStream.close();
    }
  }

  final InputStream normStream(String field) throws IOException {
    Norm norm = (Norm)norms.get(field);
    if (norm == null)
      return null;
    InputStream result = (InputStream)norm.in.clone();
    result.seek(0);
    return result;
  }

  private final void openNorms() throws IOException {
    for (int i = 0; i < fieldInfos.size(); i++) {
      FieldInfo fi = fieldInfos.fieldInfo(i);
      if (fi.isIndexed) 
	norms.put(fi.name,
		  new Norm(directory.openFile(segment + ".f" + fi.number)));
    }
  }

  private final void closeNorms() throws IOException {
    synchronized (norms) {
      Enumeration enum  = norms.elements();
      while (enum.hasMoreElements()) {
	Norm norm = (Norm)enum.nextElement();
	norm.in.close();
      }
    }
  }
}
