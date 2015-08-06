package org.apache.lucene.analysis.stages;

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

import java.io.IOException;
import java.io.Reader;
import java.lang.ref.WeakReference;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.WeakIdentityMap;

/** Represents one stage of an analysis pipeline. */
public abstract class Stage {
  protected final Stage prevStage;

  // Single NodeTracker instance is shared across all
  // stages:
  protected final NodeTracker nodes;

  /** Which Attributes this stage defines */
  private final Map<Class<? extends Attribute>, Attribute> atts = new LinkedHashMap<Class<? extends Attribute>, Attribute>();

  protected Stage(Stage prevStage) {
    this.prevStage = prevStage;
    if (prevStage == null) {
      this.nodes = new NodeTracker();
    } else {
      this.nodes = prevStage.nodes;
    }
  }

  protected final <A extends Attribute> A create(Class<A> attClass) {
    Attribute att = atts.get(attClass);
    if (att == null) {
      try {
        att = attClass.newInstance();
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Could not instantiate implementing class for " + attClass.getName());
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Could not instantiate implementing class for " + attClass.getName());
      }

      atts.put(attClass, att);
      return attClass.cast(att);
    } else {
      throw new IllegalArgumentException(attClass + " was already added");
    }
  }

  public final <A extends Attribute> A get(Class<A> attClass) {
    Attribute attImpl = atts.get(attClass);
    if (attImpl == null) {
      if (prevStage != null) {
        return prevStage.get(attClass);
      } else {
        return null;
      }
    }

    return attClass.cast(attImpl);
  }

  public abstract boolean next() throws IOException;

  // Only set for first Stage in a chain:
  private Reader input;

  public void reset(Reader reader) {
    if (prevStage != null) {
      prevStage.reset(reader);
    } else {
      nodes.reset();
      input = reader;
    }
  }

  protected final int correctOffset(int currentOff) {
    // nocommit should we strongly type this (like
    // Tokenizer/TokenFilter today)?
    if (input == null) {
      throw new IllegalStateException("only first Stage can call correctOffset");
    }
    return (input instanceof CharFilter) ? ((CharFilter) input).correctOffset(currentOff) : currentOff;
  }

  // nocommit should we impl close()?  why?

  public boolean anyNodesCanChange() {
    return nodes.anyNodesCanChange();
  }
}
