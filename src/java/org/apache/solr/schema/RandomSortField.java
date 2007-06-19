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
import java.util.Random;

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreDocComparator;
import org.apache.lucene.search.SortComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.solr.request.TextResponseWriter;
import org.apache.solr.request.XMLWriter;
import org.apache.solr.search.function.ValueSource;

/**
 * Utility Field used for random sorting.  It should not be passed a value.
 * 
 * To enable random sorting, you will need to add something like this 
 * to the schema.xml
 * 
 * <types>
 *  ...
 *  <fieldType name="random" class="solr.RandomSortField" />
 *  ... 
 * </types>
 * <fields>
 *  ...
 *  <field name="random" type="random" indexed="true" stored="false"/>
 *  ...
 * </fields>
 *  
 * @author ryan
 * @version $Id$
 * @since solr 1.3
 */
public class RandomSortField extends FieldType 
{
  /** Special comparator for sorting hits in random order */
  private static final ScoreDocComparator COMPARE = new ScoreDocComparator() {
    final Random rand = new Random();
    
    public int compare (ScoreDoc i, ScoreDoc j) {
      return (rand.nextBoolean()) ? 1 : -1; //rand.nextInt() >>> 31; ??
    }
    public Comparable sortValue (ScoreDoc i) {
      return new Float(rand.nextFloat());
    }
    public int sortType() {
      return SortField.CUSTOM;
    }
  };
  
  /** use random sorting order.  */
  private static class RandomSort extends SortField {
    public RandomSort( String n )
    {
      super( n, SortField.CUSTOM );
    }

    @Override
    public SortComparatorSource getFactory() {
      return new SortComparatorSource() {
        public ScoreDocComparator newComparator(IndexReader reader, String fieldname) throws IOException {
          return COMPARE;
        }
      };
    }
  }
  
  @Override
  public SortField getSortField(SchemaField field,boolean reverse) {
    return new RandomSort(field.getName());
  }

  @Override
  public ValueSource getValueSource(SchemaField field) {
    throw new UnsupportedOperationException("Random field does not have a value source");
  }

  @Override
  public void write(XMLWriter xmlWriter, String name, Fieldable f) {}

  @Override
  public void write(TextResponseWriter writer, String name, Fieldable f) {}
}





