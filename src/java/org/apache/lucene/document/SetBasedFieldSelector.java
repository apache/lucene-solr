package org.apache.lucene.document;

import java.util.Set;
/**
 * Copyright 2004 The Apache Software Foundation
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

/**
 * Declare what fields to load normally and what fields to load lazily
 *
 **/
public class SetBasedFieldSelector implements FieldSelector {
  
  private Set fieldsToLoad;
  private Set lazyFieldsToLoad;
  
  

  /**
   * Pass in the Set of {@link Field} names to load and the Set of {@link Field} names to load lazily.  If both are null, the
   * Document will not have any {@link Field} on it.  
   * @param fieldsToLoad A Set of {@link String} field names to load.  May be empty, but not null
   * @param lazyFieldsToLoad A Set of {@link String} field names to load lazily.  May be empty, but not null  
   */
  public SetBasedFieldSelector(Set fieldsToLoad, Set lazyFieldsToLoad) {
    this.fieldsToLoad = fieldsToLoad;
    this.lazyFieldsToLoad = lazyFieldsToLoad;
  }

  /**
   * Indicate whether to load the field with the given name or not. If the {@link Field#name()} is not in either of the 
   * initializing Sets, then {@link org.apache.lucene.document.FieldSelectorResult#NO_LOAD} is returned.  If a Field name
   * is in both <code>fieldsToLoad</code> and <code>lazyFieldsToLoad</code>, lazy has precedence.
   * 
   * @param fieldName The {@link Field} name to check
   * @return The {@link FieldSelectorResult}
   */
  public FieldSelectorResult accept(String fieldName) {
    FieldSelectorResult result = FieldSelectorResult.NO_LOAD;
    if (fieldsToLoad.contains(fieldName) == true){
      result = FieldSelectorResult.LOAD;
    }
    if (lazyFieldsToLoad.contains(fieldName) == true){
      result = FieldSelectorResult.LAZY_LOAD;
    }                                           
    return result;
  }
}