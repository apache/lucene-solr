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

import java.util.Hashtable;
import java.util.Vector;
import java.util.Enumeration;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.store.InputStream;

final class FieldInfos {
  private Vector byNumber = new Vector();
  private Hashtable byName = new Hashtable();

  FieldInfos() {
    add("", false);
  }

  FieldInfos(Directory d, String name) throws IOException {
    InputStream input = d.openFile(name);
    try {
      read(input);
    } finally {
      input.close();
    }
  }

  /** Adds field info for a Document. */
  final void add(Document doc) {
    Enumeration fields  = doc.fields();
    while (fields.hasMoreElements()) {
      Field field = (Field)fields.nextElement();
      add(field.name(), field.isIndexed());
    }
  }

  /** Merges in information from another FieldInfos. */
  final void add(FieldInfos other) {
    for (int i = 0; i < other.size(); i++) {
      FieldInfo fi = other.fieldInfo(i);
      add(fi.name, fi.isIndexed);
    }
  }

  private final void add(String name, boolean isIndexed) {
    FieldInfo fi = fieldInfo(name);
    if (fi == null)
      addInternal(name, isIndexed);
    else if (fi.isIndexed != isIndexed)
      throw new IllegalStateException("field " + name +
				      (fi.isIndexed ? " must" : " cannot") +
				      " be an indexed field.");
  }

  private final void addInternal(String name, boolean isIndexed) {
    FieldInfo fi = new FieldInfo(name, isIndexed, byNumber.size());
    byNumber.addElement(fi);
    byName.put(name, fi);
  }

  final int fieldNumber(String fieldName) {
    FieldInfo fi = fieldInfo(fieldName);
    if (fi != null)
      return fi.number;
    else
      return -1;
  }

  final FieldInfo fieldInfo(String fieldName) {
    return (FieldInfo)byName.get(fieldName);
  }

  final String fieldName(int fieldNumber) {
    return fieldInfo(fieldNumber).name;
  }

  final FieldInfo fieldInfo(int fieldNumber) {
    return (FieldInfo)byNumber.elementAt(fieldNumber);
  }

  final int size() {
    return byNumber.size();
  }

  final void write(Directory d, String name) throws IOException {
    OutputStream output = d.createFile(name);
    try {
      write(output);
    } finally {
      output.close();
    }
  }

  final void write(OutputStream output) throws IOException {
    output.writeVInt(size());
    for (int i = 0; i < size(); i++) {
      FieldInfo fi = fieldInfo(i);
      output.writeString(fi.name);
      output.writeByte((byte)(fi.isIndexed ? 1 : 0));
    }
  }

  private final void read(InputStream input) throws IOException {
    int size = input.readVInt();
    for (int i = 0; i < size; i++)
      addInternal(input.readString().intern(),
		  input.readByte() != 0);
  }
}
