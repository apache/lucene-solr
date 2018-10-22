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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * Joins leftStream with rightStream based on a Equalitor. Both streams must be sorted by the fields being joined on.
 * Resulting stream is sorted by the equalitor.
 * @since 6.0.0
 **/

public abstract class BiJoinStream extends JoinStream implements Expressible {
  
  protected PushBackStream leftStream;
  protected PushBackStream rightStream;
  
  // This is used to determine whether we should iterate the left or right side (depending on stream order).
  // It is built from the incoming equalitor and streams' comparators.
  protected StreamComparator iterationComparator;
  protected StreamComparator leftStreamComparator, rightStreamComparator;
  
  public BiJoinStream(TupleStream leftStream, TupleStream rightStream, StreamEqualitor eq) throws IOException {
    super(eq, leftStream, rightStream);
    init();
  }
  
  public BiJoinStream(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
    init();
  }
  
  private void init() throws IOException {
    
    // Validates all incoming streams for tuple order
    validateTupleOrder();
    
    leftStream = getStream(0);
    rightStream = getStream(1);
    
    // iterationComparator is a combination of the equalitor and the comp from each stream. This can easily be done by
    // grabbing the first N parts of each comp where N is the number of parts in the equalitor. Because we've already
    // validated tuple order (the comps) then we know this can be done.
    iterationComparator = createIterationComparator(eq, leftStream.getStreamSort());
    leftStreamComparator = createSideComparator(eq, leftStream.getStreamSort());
    rightStreamComparator = createSideComparator(eq, rightStream.getStreamSort());
  }
  
  protected void validateTupleOrder() throws IOException {
    if (!isValidTupleOrder()) {
      throw new IOException(
          "Invalid JoinStream - all incoming stream comparators (sort) must be a superset of this stream's equalitor.");
    }
  }
  
  private StreamComparator createIterationComparator(StreamEqualitor eq, StreamComparator comp) throws IOException {
    if (eq instanceof MultipleFieldEqualitor && comp instanceof MultipleFieldComparator) {
      // we know the comp is at least as long as the eq because we've already validated the tuple order
      StreamComparator[] compoundComps = new StreamComparator[((MultipleFieldEqualitor) eq).getEqs().length];
      for (int idx = 0; idx < compoundComps.length; ++idx) {
        StreamEqualitor sourceEqualitor = ((MultipleFieldEqualitor) eq).getEqs()[idx];
        StreamComparator sourceComparator = ((MultipleFieldComparator) comp).getComps()[idx];
        
        if (sourceEqualitor instanceof FieldEqualitor && sourceComparator instanceof FieldComparator) {
          FieldEqualitor fieldEqualitor = (FieldEqualitor) sourceEqualitor;
          FieldComparator fieldComparator = (FieldComparator) sourceComparator;
          compoundComps[idx] = new FieldComparator(fieldEqualitor.getLeftFieldName(),
              fieldEqualitor.getRightFieldName(), fieldComparator.getOrder());
        } else {
          throw new IOException("Failed to create an iteration comparator");
        }
      }
      return new MultipleFieldComparator(compoundComps);
    } else if (comp instanceof MultipleFieldComparator) {
      StreamEqualitor sourceEqualitor = eq;
      StreamComparator sourceComparator = ((MultipleFieldComparator) comp).getComps()[0];
      
      if (sourceEqualitor instanceof FieldEqualitor && sourceComparator instanceof FieldComparator) {
        FieldEqualitor fieldEqualitor = (FieldEqualitor) sourceEqualitor;
        FieldComparator fieldComparator = (FieldComparator) sourceComparator;
        return new FieldComparator(fieldEqualitor.getLeftFieldName(), fieldEqualitor.getRightFieldName(),
            fieldComparator.getOrder());
      } else {
        throw new IOException("Failed to create an iteration comparator");
      }
    } else {
      StreamEqualitor sourceEqualitor = eq;
      StreamComparator sourceComparator = comp;
      
      if (sourceEqualitor instanceof FieldEqualitor && sourceComparator instanceof FieldComparator) {
        FieldEqualitor fieldEqualitor = (FieldEqualitor) sourceEqualitor;
        FieldComparator fieldComparator = (FieldComparator) sourceComparator;
        return new FieldComparator(fieldEqualitor.getLeftFieldName(), fieldEqualitor.getRightFieldName(),
            fieldComparator.getOrder());
      } else {
        throw new IOException("Failed to create an iteration comparator");
      }
    }
  }
  
  private StreamComparator createSideComparator(StreamEqualitor eq, StreamComparator comp) throws IOException {
    if (eq instanceof MultipleFieldEqualitor && comp instanceof MultipleFieldComparator) {
      // we know the comp is at least as long as the eq because we've already validated the tuple order
      StreamComparator[] compoundComps = new StreamComparator[((MultipleFieldEqualitor) eq).getEqs().length];
      for (int idx = 0; idx < compoundComps.length; ++idx) {
        StreamComparator sourceComparator = ((MultipleFieldComparator) comp).getComps()[idx];
        
        if (sourceComparator instanceof FieldComparator) {
          FieldComparator fieldComparator = (FieldComparator) sourceComparator;
          compoundComps[idx] = new FieldComparator(fieldComparator.getLeftFieldName(),
              fieldComparator.getRightFieldName(), fieldComparator.getOrder());
        } else {
          throw new IOException("Failed to create an side comparator");
        }
      }
      return new MultipleFieldComparator(compoundComps);
    } else if (comp instanceof MultipleFieldComparator) {
      StreamComparator sourceComparator = ((MultipleFieldComparator) comp).getComps()[0];
      
      if (sourceComparator instanceof FieldComparator) {
        FieldComparator fieldComparator = (FieldComparator) sourceComparator;
        return new FieldComparator(fieldComparator.getLeftFieldName(), fieldComparator.getRightFieldName(),
            fieldComparator.getOrder());
      } else {
        throw new IOException("Failed to create an side comparator");
      }
    } else {
      StreamComparator sourceComparator = comp;
      
      if (sourceComparator instanceof FieldComparator) {
        FieldComparator fieldComparator = (FieldComparator) sourceComparator;
        return new FieldComparator(fieldComparator.getLeftFieldName(), fieldComparator.getRightFieldName(),
            fieldComparator.getOrder());
      } else {
        throw new IOException("Failed to create an side comparator");
      }
    }
  }
}
