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
package org.apache.lucene.util;


/**
 * Base class for Attributes that can be added to a 
 * {@link org.apache.lucene.util.AttributeSource}.
 * <p>
 * Attributes are used to add data in a dynamic, yet type-safe way to a source
 * of usually streamed objects, e. g. a {@link org.apache.lucene.analysis.TokenStream}.
 */
public abstract class AttributeImpl implements Cloneable, Attribute {  
  /**
   * Clears the values in this AttributeImpl and resets it to its 
   * default value. If this implementation implements more than one Attribute interface
   * it clears all.
   */
  public abstract void clear();
  
  /**
   * Clears the values in this AttributeImpl and resets it to its value
   * at the end of the field. If this implementation implements more than one Attribute interface
   * it clears all.
   * <p>
   * The default implementation simply calls {@link #clear()}
   */
  public void end() {
    clear();
  }
  
  /**
   * This method returns the current attribute values as a string in the following format
   * by calling the {@link #reflectWith(AttributeReflector)} method:
   * 
   * <ul>
   * <li><em>iff {@code prependAttClass=true}:</em> {@code "AttributeClass#key=value,AttributeClass#key=value"}
   * <li><em>iff {@code prependAttClass=false}:</em> {@code "key=value,key=value"}
   * </ul>
   *
   * @see #reflectWith(AttributeReflector)
   */
  public final String reflectAsString(final boolean prependAttClass) {
    final StringBuilder buffer = new StringBuilder();
    reflectWith((attClass, key, value) -> {
      if (buffer.length() > 0) {
        buffer.append(',');
      }
      if (prependAttClass) {
        buffer.append(attClass.getName()).append('#');
      }
      buffer.append(key).append('=').append((value == null) ? "null" : value);
    });
    return buffer.toString();
  }
  
  /**
   * This method is for introspection of attributes, it should simply
   * add the key/values this attribute holds to the given {@link AttributeReflector}.
   *
   * <p>Implementations look like this (e.g. for a combined attribute implementation):
   * <pre class="prettyprint">
   *   public void reflectWith(AttributeReflector reflector) {
   *     reflector.reflect(CharTermAttribute.class, "term", term());
   *     reflector.reflect(PositionIncrementAttribute.class, "positionIncrement", getPositionIncrement());
   *   }
   * </pre>
   *
   * <p>If you implement this method, make sure that for each invocation, the same set of {@link Attribute}
   * interfaces and keys are passed to {@link AttributeReflector#reflect} in the same order, but possibly
   * different values. So don't automatically exclude e.g. {@code null} properties!
   *
   * @see #reflectAsString(boolean)
   */
  public abstract void reflectWith(AttributeReflector reflector);
  
  /**
   * Copies the values from this Attribute into the passed-in
   * target attribute. The target implementation must support all the
   * Attributes this implementation supports.
   */
  public abstract void copyTo(AttributeImpl target);

  /**
   * In most cases the clone is, and should be, deep in order to be able to
   * properly capture the state of all attributes.
   */
  @Override
  public AttributeImpl clone() {
    AttributeImpl clone = null;
    try {
      clone = (AttributeImpl)super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);  // shouldn't happen
    }
    return clone;
  }
}
