package org.apache.lucene.document;

import java.util.Set;
/**
 * Created by IntelliJ IDEA.
 * User: Grant Ingersoll
 * Date: Apr 14, 2006
 * Time: 6:53:07 PM
 * $Id:$
 * Copyright 2005.  Center For Natural Language Processing
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