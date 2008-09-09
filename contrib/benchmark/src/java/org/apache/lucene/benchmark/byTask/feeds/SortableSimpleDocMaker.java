package org.apache.lucene.benchmark.byTask.feeds;

import java.util.Properties;
import java.util.Random;

import org.apache.lucene.benchmark.byTask.utils.Config;

/**
 * Adds fields appropriate for sorting.
 * 
 */
public class SortableSimpleDocMaker extends SimpleDocMaker {
  private int sortRange;

  protected DocData getNextDocData() throws NoMoreDataException {
    DocData doc = super.getNextDocData();
    Properties props = new Properties();
    props.put("sort_field", Integer.toString(getRandomNumber(0, sortRange)));
    doc.setProps(props);
    return doc;
  }

  /*
   * (non-Javadoc)
   * 
   * @see SimpleDocMaker#setConfig(java.util.Properties)
   */
  public void setConfig(Config config) {
    super.setConfig(config);
    sortRange = config.get("sort.rng", 20000);
  }

  private int getRandomNumber(final int low, final int high) {
    Random r = new Random();
    int randInt = (Math.abs(r.nextInt()) % (high - low)) + low;
    return randInt;
  }
}
