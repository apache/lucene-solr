/**
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

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.response.XMLWriter;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.SolrIndexReader;

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
 *  &lt;dynamicField name="random*" type="rand" indexed="true" stored="false"/&gt;
 *  ...
 * &lt;/fields&gt;
 * </pre>
 * 
 * Examples of queries:
 * <ul>
 * <li>http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_1234%20desc</li>
 * <li>http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_2345%20desc</li>
 * <li>http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_ABDC%20desc</li>
 * <li>http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_21%20desc</li>
 * </ul>
 * Note that multiple calls to the same URL will return the same sorting order.
 * 
 * @version $Id$
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
  private static int getSeed(String fieldName, IndexReader r) {
    SolrIndexReader top = (SolrIndexReader)r;
    int base=0;
    while (top.getParent() != null) {
      base += top.getBase();
      top = top.getParent();
    }

    // calling getVersion() on a segment will currently give you a null pointer exception, so
    // we use the top-level reader.
    return fieldName.hashCode() + base + (int)top.getVersion();
  }

  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    return new SortField(field.getName(), randomComparatorSource, reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field) {
    return new RandomValueSource(field.getName());
  }

  @Override
  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException { }

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException { }


  private static FieldComparatorSource randomComparatorSource = new FieldComparatorSource() {
    public FieldComparator newComparator(final String fieldname, final int numHits, int sortPos, boolean reversed) throws IOException {
      return new FieldComparator() {
        int seed;
        private final int[] values = new int[numHits];
        int bottomVal;

        public int compare(int slot1, int slot2) {
          return values[slot1] - values[slot2];  // values will be positive... no overflow possible.
        }

        public void setBottom(int slot) {
          bottomVal = values[slot];
        }

        public int compareBottom(int doc) throws IOException {
          return bottomVal - hash(doc+seed);
        }

        public void copy(int slot, int doc) throws IOException {
          values[slot] = hash(doc+seed);
        }

        public void setNextReader(IndexReader reader, int docBase) throws IOException {
          seed = getSeed(fieldname, reader);
        }

        public Comparable value(int slot) {
          return values[slot];
        }
      };
    }
  };



  public class RandomValueSource extends ValueSource {
    private final String field;

    public RandomValueSource(String field) {
      this.field=field;
    }

    @Override
    public String description() {
      return field;
    }

    @Override
    public DocValues getValues(Map context, final IndexReader reader) throws IOException {
      return new DocValues() {
          private final int seed = getSeed(field, reader);
          @Override
          public float floatVal(int doc) {
            return (float)hash(doc+seed);
          }

          @Override
          public int intVal(int doc) {
            return (int)hash(doc+seed);
          }

          @Override
          public long longVal(int doc) {
            return (long)hash(doc+seed);
          }

          @Override
          public double doubleVal(int doc) {
            return (double)hash(doc+seed);
          }

          @Override
          public String strVal(int doc) {
            return Integer.toString(hash(doc+seed));
          }

          @Override
          public String toString(int doc) {
            return description() + '=' + intVal(doc);
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





