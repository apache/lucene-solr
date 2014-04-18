package org.apache.lucene.spatial.prefix;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Iterator;

/**
 * A TokenStream used internally by {@link org.apache.lucene.spatial.prefix.PrefixTreeStrategy}.
 *
 * This is highly modelled after {@link org.apache.lucene.analysis.NumericTokenStream}.
 *
 * If there is demand for it to be public; it could be made to be.
 *
 * @lucene.internal
 */
class CellTokenStream extends TokenStream {

  private interface CellTermAttribute extends Attribute {
    Cell getCell();
    void setCell(Cell cell);

    //TODO one day deprecate this once we have better encodings
    boolean getOmitLeafByte();
    void setOmitLeafByte(boolean b);
  }

  // just a wrapper to prevent adding CTA
  private static final class CellAttributeFactory extends AttributeSource.AttributeFactory {
    private final AttributeSource.AttributeFactory delegate;

    CellAttributeFactory(AttributeSource.AttributeFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
      if (CharTermAttribute.class.isAssignableFrom(attClass))
        throw new IllegalArgumentException("CellTokenStream does not support CharTermAttribute.");
      return delegate.createAttributeInstance(attClass);
    }
  }

  private static final class CellTermAttributeImpl extends AttributeImpl
      implements CellTermAttribute, TermToBytesRefAttribute {
    private BytesRef bytes = new BytesRef();
    private Cell cell;
    private boolean omitLeafByte;//false by default (whether there's a leaf byte or not)

    @Override
    public Cell getCell() {
      return cell;
    }

    @Override
    public boolean getOmitLeafByte() {
      return omitLeafByte;
    }

    @Override
    public void setCell(Cell cell) {
      this.cell = cell;
      omitLeafByte = false;//reset
    }

    @Override
    public void setOmitLeafByte(boolean b) {
      omitLeafByte = b;
    }

    @Override
    public void clear() {
      // this attribute has no contents to clear!
      // we keep it untouched as it's fully controlled by outer class.
    }

    @Override
    public void copyTo(AttributeImpl target) {
      final CellTermAttribute a = (CellTermAttribute) target;
      a.setCell(cell);
      a.setOmitLeafByte(omitLeafByte);
    }

    @Override
    public void fillBytesRef() {
      if (omitLeafByte)
        cell.getTokenBytesNoLeaf(bytes);
      else
        cell.getTokenBytesWithLeaf(bytes);
    }

    @Override
    public BytesRef getBytesRef() {
      return bytes;
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
      fillBytesRef();
      reflector.reflect(TermToBytesRefAttribute.class, "bytes", BytesRef.deepCopyOf(bytes));
    }
  }

  public CellTokenStream() {
    this(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY);
  }

  public CellTokenStream(AttributeFactory factory) {
    super(new CellAttributeFactory(factory));
  }

  public CellTokenStream setCells(Iterator<Cell> iter) {
    this.iter = iter;
    return this;
  }

  @Override
  public void reset() throws IOException {
    if (iter == null)
      throw new IllegalStateException("call setCells() before usage");
    cellAtt.setCell(null);
    cellAtt.setOmitLeafByte(false);
  }

  /** Outputs the token of a cell, and if its a leaf, outputs it again with the leaf byte. */
  @Override
  public final boolean incrementToken() {
    if (iter == null)
      throw new IllegalStateException("call setCells() before usage");

    // this will only clear all other attributes in this TokenStream
    clearAttributes();

    if (cellAtt.getOmitLeafByte()) {
      cellAtt.setOmitLeafByte(false);
      return true;
    }
    //get next
    if (!iter.hasNext())
      return false;
    cellAtt.setCell(iter.next());
    if (cellAtt.getCell().isLeaf())
      cellAtt.setOmitLeafByte(true);
    return true;
  }

  {
    addAttributeImpl(new CellTermAttributeImpl());//because non-public constructor
  }
  //members
  private final CellTermAttribute cellAtt = addAttribute(CellTermAttribute.class);

  //TODO support position increment, and TypeAttribute

  private Iterator<Cell> iter = null; // null means not initialized

}
