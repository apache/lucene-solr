/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.schema;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.search.*;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 * Utility Field used for random sorting.  It should not be passed a value.
 * <p>
 * This random sorting implementation uses the dynamic field name to set the
 * random 'seed'.  To get random sorting order, you need to use a random
 * dynamic field name.  For example, you will need to configure schema.xml:
 * <pre>
 * &lt;types&gt;
 *  ...
 *  &lt;fieldType name="random" class="solr.RandomSortField" /&gt;
 *  ... 
 * &lt;/types&gt;
 * &lt;fields&gt;
 *  ...
 *  &lt;dynamicField name="random*" type="random" indexed="true" stored="false"/&gt;
 *  ...
 * &lt;/fields&gt;
 * </pre>
 * 
 * Examples of queries:
 * <ul>
 * <li>http://localhost:8983/solr/select/?q=*:*&amp;fl=name&amp;sort=random_1234%20desc</li>
 * <li>http://localhost:8983/solr/select/?q=*:*&amp;fl=name&amp;sort=random_2345%20desc</li>
 * <li>http://localhost:8983/solr/select/?q=*:*&amp;fl=name&amp;sort=random_ABDC%20desc</li>
 * <li>http://localhost:8983/solr/select/?q=*:*&amp;fl=name&amp;sort=random_21%20desc</li>
 * </ul>
 * Note that multiple calls to the same URL will return the same sorting order.
 * 
 *
 * @since solr 1.3
 */
public class RandomSortField extends FieldType {
  // Thomas Wang's hash32shift function, from http://www.cris.com/~Ttwang/tech/inthash.htm
  // slightly modified to return only positive integers.
  private static int hash(int key) {
    key = ~key + (key << 15); // key = (key << 15) - key - 1;
    key = key ^ (key >>> 12);
    key = key + (key << 2);
    key = key ^ (key >>> 4);
    key = key * 2057; // key = (key + (key << 3)) + (key << 11);
    key = key ^ (key >>> 16);
    return key >>> 1; 
  }

  /** 
   * Given a field name and an IndexReader, get a random hash seed.
   * Using dynamic fields, you can force the random order to change 
   */
  private static int getSeed(String fieldName, LeafReaderContext context) {
    final DirectoryReader top = (DirectoryReader) ReaderUtil.getTopLevelContext(context).reader();
    // calling getVersion() on a segment will currently give you a null pointer exception, so
    // we use the top-level reader.
    return fieldName.hashCode() + context.docBase + (int)top.getVersion();
  }
  
  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    return new SortField(field.getName(), randomComparatorSource, reverse);
  }
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    return null;
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    return new RandomValueSource(field.getName());
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException { }


  private static FieldComparatorSource randomComparatorSource = new FieldComparatorSource() {
    @Override
    public FieldComparator<Integer> newComparator(final String fieldname, final int numHits, int sortPos, boolean reversed) {
      return new SimpleFieldComparator<Integer>() {
        int seed;
        private final int[] values = new int[numHits];
        int bottomVal;
        int topVal;

        @Override
        public int compare(int slot1, int slot2) {
          return values[slot1] - values[slot2];  // values will be positive... no overflow possible.
        }

        @Override
        public void setBottom(int slot) {
          bottomVal = values[slot];
        }

        @Override
        public void setTopValue(Integer value) {
          topVal = value.intValue();
        }

        @Override
        public int compareBottom(int doc) {
          return bottomVal - hash(doc+seed);
        }

        @Override
        public void copy(int slot, int doc) {
          values[slot] = hash(doc+seed);
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) {
          seed = getSeed(fieldname, context);
        }

        @Override
        public Integer value(int slot) {
          return values[slot];
        }

        @Override
        public int compareTop(int doc) {
          // values will be positive... no overflow possible.
          return topVal - hash(doc+seed);
        }
      };
    }
  };



  public static class RandomValueSource extends ValueSource {
    private final String field;

    public RandomValueSource(String field) {
      this.field=field;
    }

    @Override
    public String description() {
      return field;
    }

    @Override
    public FunctionValues getValues(@SuppressWarnings({"rawtypes"})Map context, final LeafReaderContext readerContext) throws IOException {
      return new IntDocValues(this) {
          private final int seed = getSeed(field, readerContext);
          @Override
          public int intVal(int doc) {
            return hash(doc+seed);
          }
        };
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof RandomValueSource)) return false;
      RandomValueSource other = (RandomValueSource)o;
      return this.field.equals(other.field);
    }

    @Override
    public int hashCode() {
      return field.hashCode();
    };
  }
}





