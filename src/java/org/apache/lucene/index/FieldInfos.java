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

import java.util.*;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.store.InputStream;

/** Access to the Field Info file that describes document fields and whether or
 *  not they are indexed. Each segment has a separate Field Info file. Objects
 *  of this class are thread-safe for multiple readers, but only one thread can
 *  be adding documents at a time, with no other reader or writer threads
 *  accessing this object.
 */
final class FieldInfos {
  private Vector byNumber = new Vector();
  private Hashtable byName = new Hashtable();

  FieldInfos() {
    add("", false);
  }

  /**
   * Construct a FieldInfos object using the directory and the name of the file
   * InputStream
   * @param d The directory to open the InputStream from
   * @param name The name of the file to open the InputStream from in the Directory
   * @throws IOException
   * 
   * @see #read
   */
  FieldInfos(Directory d, String name) throws IOException {
    InputStream input = d.openFile(name);
    try {
      read(input);
    } finally {
      input.close();
    }
  }

  /** Adds field info for a Document. */
  public void add(Document doc) {
    Enumeration fields = doc.fields();
    while (fields.hasMoreElements()) {
      Field field = (Field) fields.nextElement();
      add(field.name(), field.isIndexed(), field.isTermVectorStored());
    }
  }

  /**
   * @param names The names of the fields
   * @param storeTermVectors Whether the fields store term vectors or not
   */
  public void addIndexed(Collection names, boolean storeTermVectors) {
    Iterator i = names.iterator();
    int j = 0;
    while (i.hasNext()) {
      add((String)i.next(), true, storeTermVectors);
    }
  }

  /**
   * Assumes the field is not storing term vectors 
   * @param names The names of the fields
   * @param isIndexed Whether the fields are indexed or not
   * 
   * @see #add(String, boolean)
   */
  public void add(Collection names, boolean isIndexed) {
    Iterator i = names.iterator();
    int j = 0;
    while (i.hasNext()) {
      add((String)i.next(), isIndexed);
    }
  }

  /**
   * Calls three parameter add with false for the storeTermVector parameter 
   * @param name The name of the Field
   * @param isIndexed true if the field is indexed
   * @see #add(String, boolean, boolean)
   */
  public void add(String name, boolean isIndexed) {
    add(name, isIndexed, false);
  }


  /** If the field is not yet known, adds it. If it is known, checks to make
   *  sure that the isIndexed flag is the same as was given previously for this
   *  field. If not - marks it as being indexed.  Same goes for storeTermVector
   * 
   * @param name The name of the field
   * @param isIndexed true if the field is indexed
   * @param storeTermVector true if the term vector should be stored
   */
  public void add(String name, boolean isIndexed, boolean storeTermVector) {
    FieldInfo fi = fieldInfo(name);
    if (fi == null) {
      addInternal(name, isIndexed, storeTermVector);
    } else {
      if (fi.isIndexed != isIndexed) {
        fi.isIndexed = true;                      // once indexed, always index
      }
      if (fi.storeTermVector != storeTermVector) {
        fi.storeTermVector = true;                // once vector, always vector
      }
    }
  }

  private void addInternal(String name, boolean isIndexed,
                           boolean storeTermVector) {
    FieldInfo fi =
      new FieldInfo(name, isIndexed, byNumber.size(), storeTermVector);
    byNumber.addElement(fi);
    byName.put(name, fi);
  }

  public int fieldNumber(String fieldName) {
    FieldInfo fi = fieldInfo(fieldName);
    if (fi != null)
      return fi.number;
    else
      return -1;
  }

  public FieldInfo fieldInfo(String fieldName) {
    return (FieldInfo) byName.get(fieldName);
  }

  public String fieldName(int fieldNumber) {
    return fieldInfo(fieldNumber).name;
  }

  public FieldInfo fieldInfo(int fieldNumber) {
    return (FieldInfo) byNumber.elementAt(fieldNumber);
  }

  public int size() {
    return byNumber.size();
  }

  public boolean hasVectors() {
    boolean hasVectors = false;
    for (int i = 0; i < size(); i++) {
      if (fieldInfo(i).storeTermVector)
        hasVectors = true;
    }
    return hasVectors;
  }

  public void write(Directory d, String name) throws IOException {
    OutputStream output = d.createFile(name);
    try {
      write(output);
    } finally {
      output.close();
    }
  }

  public void write(OutputStream output) throws IOException {
    output.writeVInt(size());
    for (int i = 0; i < size(); i++) {
      FieldInfo fi = fieldInfo(i);
      byte bits = 0x0;
      if (fi.isIndexed) bits |= 0x1;
      if (fi.storeTermVector) bits |= 0x2;
      output.writeString(fi.name);
      //Was REMOVE
      //output.writeByte((byte)(fi.isIndexed ? 1 : 0));
      output.writeByte(bits);
    }
  }

  private void read(InputStream input) throws IOException {
    int size = input.readVInt();//read in the size
    for (int i = 0; i < size; i++) {
      String name = input.readString().intern();
      byte bits = input.readByte();
      boolean isIndexed = (bits & 0x1) != 0;
      boolean storeTermVector = (bits & 0x2) != 0;
      addInternal(name, isIndexed, storeTermVector);
    }    
  }

}
