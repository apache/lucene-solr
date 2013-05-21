package org.apache.lucene.analysis.tokenattributes;

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

/** Default implementation of {@link DeletedAttribute}. */
public class DeletedAttributeImpl extends AttributeImpl implements DeletedAttribute, Cloneable {
  private boolean deleted;
  
  public DeletedAttributeImpl() {}

  @Override
  public boolean deleted() {
    return deleted;
  }

  @Override
  public void set(boolean deleted) {
    this.deleted = deleted;
  }

  @Override
  public void clear() {
    deleted = false;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof DeletedAttributeImpl) {
      DeletedAttributeImpl o = (DeletedAttributeImpl) other;
      return o.deleted == deleted;
    }
    
    return false;
  }

  @Override
  public int hashCode() {
    return deleted ? 31 : 57;
  } 
  
  @Override
  public void copyTo(AttributeImpl target) {
    DeletedAttribute t = (DeletedAttribute) target;
    t.set(deleted);
  }  
}
