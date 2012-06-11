// -*- c-basic-offset: 2 -*-
package org.apache.lucene.analysis.morfologik;

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

import org.apache.lucene.util.AttributeImpl;

/**
 * Morphosyntactic annotations for surface forms.
 * @see MorphosyntacticTagAttribute
 */
public class MorphosyntacticTagAttributeImpl extends AttributeImpl 
  implements MorphosyntacticTagAttribute, Cloneable {

  /**
   * Either the original tag from WordData or a clone.
   */
  private CharSequence tag;

  /** 
   * Set the tag.
   */
  public void setTag(CharSequence pos) {
    this.tag = ((pos == null || pos.length() == 0) ? null : pos);
  }

  /**
   * Returns the POS tag of the term. If you need a copy of this char sequence, clone it
   * because it may change with each new term!
   */
  public CharSequence getTag() {
    return tag;
  }

  public void clear() {
    tag = null;
  }

  public boolean equals(Object other) {
    if (other instanceof MorphosyntacticTagAttribute) {
      return equal(this.getTag(), ((MorphosyntacticTagAttribute) other).getTag());
    }
    return false;
  }

  /**
   * Check if two char sequences are the same.
   */
  private boolean equal(CharSequence chs1, CharSequence chs2) {
    if (chs1 == null && chs2 == null)
      return true;
    if (chs1 == null || chs2 == null)
      return false;
    int l1 = chs1.length();
    int l2 = chs2.length();
    if (l1 != l2)
      return false;
    for (int i = 0; i < l1; i++)
      if (chs1.charAt(i) != chs2.charAt(i))
        return false;
    return true;
  }

  public int hashCode() {
    return this.tag == null ? 0 : tag.hashCode();
  }

  public void copyTo(AttributeImpl target) {
    ((MorphosyntacticTagAttribute) target).setTag(this.tag);
  }

  public MorphosyntacticTagAttributeImpl clone() {
    MorphosyntacticTagAttributeImpl cloned = new MorphosyntacticTagAttributeImpl();
    cloned.tag = (tag == null ? null : tag.toString());
    return cloned;
  }
}
