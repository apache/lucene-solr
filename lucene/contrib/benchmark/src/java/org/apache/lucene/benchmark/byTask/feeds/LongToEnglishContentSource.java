package org.apache.lucene.benchmark.byTask.feeds;

import org.apache.lucene.util.English;

import java.io.IOException;
import java.util.Date;


/**
 *
 *
 **/
public class LongToEnglishContentSource extends ContentSource{
  private long counter = Long.MIN_VALUE + 10;

  @Override
  public void close() throws IOException {

  }
  //TODO: reduce/clean up synchonization
  @Override
  public synchronized DocData getNextDocData(DocData docData) throws NoMoreDataException, IOException {
    docData.clear();
    docData.setBody(English.longToEnglish(counter));
    docData.setName("doc_" + String.valueOf(counter));
    docData.setTitle("title_" + String.valueOf(counter));
    docData.setDate(new Date());
    if (counter == Long.MAX_VALUE){
      counter = Long.MIN_VALUE + 10;//loop around
    }
    counter++;
    return docData;
  }

  @Override
  public void resetInputs() throws IOException {
    counter = Long.MIN_VALUE + 10;
  }
}
