package org.apache.lucene.document;
/**
 * Created by IntelliJ IDEA.
 * User: Grant Ingersoll
 * Date: Apr 14, 2006
 * Time: 5:29:26 PM
 * $Id:$
 * Copyright 2005.  Center For Natural Language Processing
 */

/**
 * Similar to a {@link java.io.FileFilter}, the FieldSelector allows one to make decisions about
 * what Fields get loaded on a {@link Document} by {@link org.apache.lucene.index.IndexReader#document(int,org.apache.lucene.document.FieldSelector)}
 *
 **/
public interface FieldSelector {

  /**
   * 
   * @param fieldName
   * @return true if the {@link Field} with <code>fieldName</code> should be loaded or not
   */
  FieldSelectorResult accept(String fieldName);
}
