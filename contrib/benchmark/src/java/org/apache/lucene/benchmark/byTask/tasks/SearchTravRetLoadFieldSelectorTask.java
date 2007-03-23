package org.apache.lucene.benchmark.byTask.tasks;
/**
 * Created by IntelliJ IDEA.
 * User: Grant Ingersoll
 * Date: Mar 22, 2007
 * Time: 10:04:49 PM
 * $Id:$
 * Copyright 2007.  Center For Natural Language Processing
 */

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.index.IndexReader;

import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.io.IOException;

/**
 * Search and Travrese and Retrieve docs task using a SetBasedFieldSelector.
 *
 * <p>Note: This task reuses the reader if it is already open.
 * Otherwise a reader is opened at start and closed at the end.
 *
 * Takes optional param: comma separated list of Fields to load.
 */
public class SearchTravRetLoadFieldSelectorTask extends SearchTravTask {

  protected FieldSelector fieldSelector;
  public SearchTravRetLoadFieldSelectorTask(PerfRunData runData) {
    super(runData);
    
  }

  public boolean withRetrieve() {
    return true;
  }


  protected int retrieveDoc(IndexReader ir, int id) throws IOException {
    return (ir.document(id, fieldSelector) == null ? 0 : 1);
  }

  public void setParams(String params) {
    Set fieldsToLoad = new HashSet();
    for (StringTokenizer tokenizer = new StringTokenizer(params, ","); tokenizer.hasMoreTokens();) {
      String s = tokenizer.nextToken();
      fieldsToLoad.add(s);
    }
    fieldSelector = new SetBasedFieldSelector(fieldsToLoad, Collections.EMPTY_SET);
  }
}
