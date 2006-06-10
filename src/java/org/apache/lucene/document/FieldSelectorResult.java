package org.apache.lucene.document;
/**
 * Created by IntelliJ IDEA.
 * User: Grant Ingersoll
 * Date: Apr 14, 2006
 * Time: 5:40:17 PM
 * $Id:$
 * Copyright 2005.  Center For Natural Language Processing
 */

/**
 *  Provides information about what should be done with this Field 
 *
 **/
//Replace with an enumerated type in 1.5
public final class FieldSelectorResult {

  public static final FieldSelectorResult LOAD = new FieldSelectorResult(0);
  public static final FieldSelectorResult LAZY_LOAD = new FieldSelectorResult(1);
  public static final FieldSelectorResult NO_LOAD = new FieldSelectorResult(2);
  public static final FieldSelectorResult LOAD_AND_BREAK = new FieldSelectorResult(3);
  
  private int id;

  private FieldSelectorResult(int id)
  {
    this.id = id;
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final FieldSelectorResult that = (FieldSelectorResult) o;

    if (id != that.id) return false;

    return true;
  }

  public int hashCode() {
    return id;
  }
}
