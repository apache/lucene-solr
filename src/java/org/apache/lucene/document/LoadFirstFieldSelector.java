package org.apache.lucene.document;
/**
 * Created by IntelliJ IDEA.
 * User: Grant Ingersoll
 * Date: Apr 15, 2006
 * Time: 10:13:07 AM
 * $Id:$
 * Copyright 2005.  Center For Natural Language Processing
 */


/**
 * Load the First field and break.
 * <p/>
 * See {@link FieldSelectorResult#LOAD_AND_BREAK}
 */
public class LoadFirstFieldSelector implements FieldSelector {

  public FieldSelectorResult accept(String fieldName) {
    return FieldSelectorResult.LOAD_AND_BREAK;
  }
}