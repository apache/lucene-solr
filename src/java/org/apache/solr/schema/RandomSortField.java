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

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreDocComparator;
import org.apache.lucene.search.SortComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.solr.request.TextResponseWriter;
import org.apache.solr.request.XMLWriter;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.ValueSource;

/**
 * Utility Field used for random sorting.  It should not be passed a value.
 * 
 * This random sorting implementation uses the dynamic field name to set the
 * random 'seed'.  To get random sorting order, you need to use a random
 * dynamic field name.  For example, you will need to configure schema.xml:
 * 
 * <types>
 *  ...
 *  <fieldType name="random" class="solr.RandomSortField" />
 *  ... 
 * </types>
 * <fields>
 *  ...
 *  <dynamicField name="random*" type="rand" indexed="true" stored="false"/>
 *  ...
 * </fields>
 * 
 *  http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_1234%20desc
 *  http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_2345%20desc
 *  http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_ABDC%20desc
 *  http://localhost:8983/solr/select/?q=*:*&fl=name&sort=rand_21%20desc
 *  
 * Note that multiple calls to the same URL will return the same sorting order.
 * 
 * @author ryan
 * @author yonik
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

  private static class RandomComparator implements ScoreDocComparator {
    final int seed;

    RandomComparator(int seed) {
      this.seed = seed;
    }

    public int compare(ScoreDoc i, ScoreDoc j) {
      return hash(i.doc + seed) - hash(j.doc + seed);
    }

    public Comparable sortValue(ScoreDoc i) {
      return new Integer(hash(i.doc + seed));
    }

    public int sortType() {
      return SortField.CUSTOM;
    }
  };

  /** given a field name, get a random hash seed -- using dynamic fields, you can force the random order to change */
  private static int getSeed(String fieldName) {
    return fieldName.hashCode();      
  }

  private static class RandomSort extends SortField {
    public RandomSort(String n, boolean reverse) {
      super(n, SortField.CUSTOM, reverse);
    }

    @Override
    public SortComparatorSource getFactory() {
      final int seed = getSeed(getField());
      
      return new SortComparatorSource() {
        public ScoreDocComparator newComparator(IndexReader reader, String fieldname) throws IOException {
          return new RandomComparator( (int)(seed^reader.getVersion()) );
        }

        @Override
        public int hashCode() {
          return getField().hashCode();
        }

        @Override
        public boolean equals(Object o) {
          return (o instanceof RandomSort) && getField().equals(((RandomSort) o).getField());
        }
      };
    }
  }

  public class RandomValueSource extends ValueSource {
    private final String field;
    final int seed;

    public RandomValueSource(String field) {
      this.field=field;
      seed = getSeed(field);     
    }

    @Override
    public String description() {
      return field;
    }

    @Override
    public DocValues getValues(IndexReader reader) throws IOException {
      return new DocValues() {
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

  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    return new RandomSort(field.getName(), reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field) {
    return new RandomValueSource(field.getName());
  }

  @Override
  public void write(XMLWriter xmlWriter, String name, Fieldable f) throws IOException { }

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) throws IOException { }
}





