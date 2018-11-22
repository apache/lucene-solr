/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.tagger;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/**
 * Implementation of the {@link TaggingAttribute}
 */
public class TaggingAttributeImpl extends AttributeImpl implements TaggingAttribute {

  /**
   * the private field initialised with {@link TaggingAttribute#DEFAULT_TAGGABLE}
   */
  private boolean taggable = TaggingAttribute.DEFAULT_TAGGABLE;

  /*
   * (non-Javadoc)
   * @see org.opensextant.solrtexttagger.LookupAttribute#isLookup()
   */
  @Override
  public boolean isTaggable() {
    return taggable;
  }

  /*
   * (non-Javadoc)
   * @see org.opensextant.solrtexttagger.LookupAttribute#setLookup(boolean)
   */
  @Override
  public void setTaggable(boolean lookup) {
    this.taggable = lookup;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.util.AttributeImpl#clear()
   */
  @Override
  public void clear() {
    taggable = DEFAULT_TAGGABLE;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.util.AttributeImpl#copyTo(org.apache.lucene.util.AttributeImpl)
   */
  @Override
  public void copyTo(AttributeImpl target) {
    ((TaggingAttribute) target).setTaggable(taggable);
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(TaggingAttribute.class, "taggable", isTaggable());
  }

}
