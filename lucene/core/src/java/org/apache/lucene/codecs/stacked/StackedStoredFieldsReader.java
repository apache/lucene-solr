package org.apache.lucene.codecs.stacked;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

public class StackedStoredFieldsReader extends StoredFieldsReader {
  private StackedMap map;
  private StoredFieldsReader main, stacked;
  
  public StackedStoredFieldsReader(StackedMap map, StoredFieldsReader main, StoredFieldsReader stacked) {
    this.map = map;
    this.main = main;
    this.stacked = stacked;
  }

  @Override
  public void close() throws IOException {
    main.close();
    stacked.close();
  }

  @Override
  public void visitDocument(int n, StoredFieldVisitor visitor)
      throws CorruptIndexException, IOException {
    Map<String,Integer> stackedIds = map.getStackedStoredIds(n);
    if (stackedIds != null) {
      VisitorWrapper wrapper = new VisitorWrapper(visitor);
      // first collect unchanged fields from the main
      wrapper.excludeFields(stackedIds.keySet());
      main.visitDocument(n, wrapper);
      // now collect fields from stacked updates
      wrapper.excludeFields(Collections.<String>emptySet());
      // !!!!!!!!!!! MAJOR COST !!!!!!!!!!
      for (Entry<String,Integer> e : stackedIds.entrySet()) {
        wrapper.includeFields(e.getKey());
        stacked.visitDocument(e.getValue(), wrapper);
      }
    } else {
      main.visitDocument(n, visitor);
    }
  }
  
  private static class VisitorWrapper extends StoredFieldVisitor {
    Set<String> includeFields = new HashSet<String>();
    Set<String> excludeFields = new HashSet<String>();
    StoredFieldVisitor wrapped;
    
    public VisitorWrapper(StoredFieldVisitor wrapped) {
      this.wrapped = wrapped;
    }
    
    public void includeFields(String... fields) {
      this.includeFields.clear();
      this.includeFields.addAll(Arrays.asList(fields));
    }

    public void excludeFields(Collection<String> fields) {
      this.excludeFields.clear();
      this.excludeFields.addAll(fields);
    }
    
    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      if (includeFields.isEmpty()) {
        if (excludeFields.isEmpty()) {
          return wrapped.needsField(fieldInfo);
        } else {
          if (!excludeFields.contains(fieldInfo.name)) {
            return wrapped.needsField(fieldInfo);
          } else {
            return Status.NO;
          }
        }
      } else {
        if (includeFields.contains(fieldInfo.name)) {
          if (excludeFields.contains(fieldInfo.name)) {
            return Status.NO;
          } else {
            return wrapped.needsField(fieldInfo);
          }
        } else {
          return Status.NO;
        }
      }
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value, int offset,
        int length) throws IOException {
      wrapped.binaryField(fieldInfo, value, offset, length);
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value)
        throws IOException {
      wrapped.stringField(fieldInfo, value);
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
      wrapped.intField(fieldInfo, value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
      wrapped.longField(fieldInfo, value);
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
      wrapped.floatField(fieldInfo, value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value)
        throws IOException {
      wrapped.doubleField(fieldInfo, value);
    }
    
  }

  @Override
  public StoredFieldsReader clone() {
    return new StackedStoredFieldsReader(map, main.clone(), stacked.clone());
  }
  
}
