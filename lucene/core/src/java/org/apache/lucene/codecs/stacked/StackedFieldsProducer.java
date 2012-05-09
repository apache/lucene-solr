package org.apache.lucene.codecs.stacked;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.poi.openxml4j.exceptions.InvalidOperationException;

public class StackedFieldsProducer extends FieldsProducer {
  FieldsProducer mainProducer;
  FieldsProducer stackedProducer;
  StackedMap map;
  
  public StackedFieldsProducer(FieldsProducer mainProducer, FieldsProducer stackedProducer, StackedMap map) {
    this.mainProducer = mainProducer;
    this.stackedProducer = stackedProducer;
    this.map = map;
  }
  
  @Override
  public void close() throws IOException {
    if (mainProducer != null) {
      mainProducer.close();
    }
    stackedProducer.close();
  }
  
  @Override
  public FieldsEnum iterator() throws IOException {
    if (mainProducer == null) {
      return stackedProducer.iterator();
    }
    return new StackedFieldsEnum();
  }
  
  class StackedFieldsEnum extends FieldsEnum {
    FieldsEnum main, stacked;
    TreeSet<String> allFields = new TreeSet<String>();
    Iterator<String> it = null;
    String curField = null;
    
    StackedFieldsEnum() throws IOException {
      main = mainProducer.iterator();
      stacked = stackedProducer.iterator();
      String fld;
      while ((fld = main.next()) != null) {
        allFields.add(fld);
      }
      while ((fld = stacked.next()) != null) {
        allFields.add(fld);
      }
    }

    @Override
    public String next() throws IOException {
      if (it == null) {
        it = allFields.iterator();
      }
      if (it.hasNext()) {
        curField = it.next();
        return curField;
      } else {
        return null;
      }
    }

    @Override
    public Terms terms() throws IOException {
      if (curField == null) {
        throw new IOException("next() has to be called first");
      }
      return StackedFieldsProducer.this.terms(curField);
    }    
  }
  
  @Override
  public Terms terms(String field) throws IOException {
    Terms main = null;
    if (mainProducer != null) {
      mainProducer.terms(field);
    }
    Terms stacked = stackedProducer.terms(field);
    if (stacked == null && main == null) {
      return null;
    }
    if (stacked == null) {
      return main;
    } else {
      return new StackedTerms(field, main, stacked, map);
    }
  }

  // XXX wacky, but what can be the impact?
  @Override
  public int size() throws IOException {
    if (mainProducer != null) {
      return mainProducer.size();
    } else {
      return stackedProducer.size();
    }
  }  
}
