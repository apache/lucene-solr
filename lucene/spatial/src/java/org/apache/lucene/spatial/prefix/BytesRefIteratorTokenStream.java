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

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/**
 * A TokenStream used internally by {@link org.apache.lucene.spatial.prefix.PrefixTreeStrategy}.
 *
 * This is modelled after {@link org.apache.lucene.analysis.NumericTokenStream}.
 *
 * @lucene.internal
 */
class BytesRefIteratorTokenStream extends TokenStream {

  // just a wrapper to prevent adding CharTermAttribute
  private static final class BRAttributeFactory extends AttributeFactory {
    private final AttributeFactory delegate;

    BRAttributeFactory(AttributeFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
      if (CharTermAttribute.class.isAssignableFrom(attClass))
        throw new IllegalArgumentException(getClass() + " does not support CharTermAttribute.");
      return delegate.createAttributeInstance(attClass);
    }
  }

  private static final class BRTermToBytesRefAttributeImpl extends AttributeImpl
      implements TermToBytesRefAttribute {
    private final BytesRef bytes = new BytesRef();

    void setBytesRef(BytesRef inputBytes) {
      // shallow clone.  this.bytesRef is final
      bytes.bytes = inputBytes.bytes;
      bytes.offset = inputBytes.offset;
      bytes.length = inputBytes.length;
    }

    @Override
    public void clear() {
      // we keep it untouched as it's fully controlled by the outer class.
    }

    @Override
    public void copyTo(AttributeImpl target) {
      final BRTermToBytesRefAttributeImpl a = (BRTermToBytesRefAttributeImpl) target;
      a.setBytesRef(BytesRef.deepCopyOf(bytes));
    }

    @Override
    public void fillBytesRef() {
      //nothing to do; it's populated by incrementToken
    }

    @Override
    public BytesRef getBytesRef() {
      return bytes;
    }

    @Override
    public BRTermToBytesRefAttributeImpl clone() {
      // super.clone won't work since we need a new BytesRef reference and it's nice to have it final. The superclass
      // has no state to copy anyway.
      final BRTermToBytesRefAttributeImpl clone = new BRTermToBytesRefAttributeImpl();
      clone.setBytesRef(BytesRef.deepCopyOf(bytes));
      return clone;
    }

    @Override
    public int hashCode() {
      return bytes.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      BRTermToBytesRefAttributeImpl other = (BRTermToBytesRefAttributeImpl) obj;
      if (!bytes.equals(other.bytes)) return false;
      return true;
    }
  }

  public BytesRefIteratorTokenStream() {
    super(new BRAttributeFactory(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY));
    addAttributeImpl(new BRTermToBytesRefAttributeImpl());//because non-public constructor
    bytesAtt = (BRTermToBytesRefAttributeImpl) addAttribute(TermToBytesRefAttribute.class);
  }

  public BytesRefIterator getBytesRefIterator() {
    return bytesIter;
  }

  public BytesRefIteratorTokenStream setBytesRefIterator(BytesRefIterator iter) {
    this.bytesIter = iter;
    return this;
  }

  @Override
  public void reset() throws IOException {
    if (bytesIter == null)
      throw new IllegalStateException("call setBytesRefIterator() before usage");
    bytesAtt.getBytesRef().length = 0;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (bytesIter == null)
      throw new IllegalStateException("call setBytesRefIterator() before usage");

    // this will only clear all other attributes in this TokenStream
    clearAttributes();//TODO but there should be no "other" attributes

    // get next
    BytesRef bytes = bytesIter.next();
    if (bytes == null) {
      return false;
    } else {
      bytesAtt.setBytesRef(bytes);
      //note: we don't bother setting posInc or type attributes.  There's no point to it.
      return true;
    }
  }

  //members
  private final BRTermToBytesRefAttributeImpl bytesAtt;

  private BytesRefIterator bytesIter = null; // null means not initialized

}
