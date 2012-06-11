package org.apache.lucene.search;

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

import java.util.ArrayList;

/** Expert: Describes the score computation for document and query. */
public class Explanation {
  private float value;                            // the value of this node
  private String description;                     // what it represents
  private ArrayList<Explanation> details;                      // sub-explanations

  public Explanation() {}

  public Explanation(float value, String description) {
    this.value = value;
    this.description = description;
  }

  /**
   * Indicates whether or not this Explanation models a good match.
   *
   * <p>
   * By default, an Explanation represents a "match" if the value is positive.
   * </p>
   * @see #getValue
   */
  public boolean isMatch() {
    return (0.0f < getValue());
  }


  
  /** The value assigned to this explanation node. */
  public float getValue() { return value; }
  /** Sets the value assigned to this explanation node. */
  public void setValue(float value) { this.value = value; }

  /** A description of this explanation node. */
  public String getDescription() { return description; }
  /** Sets the description of this explanation node. */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * A short one line summary which should contain all high level
   * information about this Explanation, without the "Details"
   */
  protected String getSummary() {
    return getValue() + " = " + getDescription();
  }
  
  /** The sub-nodes of this explanation node. */
  public Explanation[] getDetails() {
    if (details == null)
      return null;
    return details.toArray(new Explanation[0]);
  }

  /** Adds a sub-node to this explanation node. */
  public void addDetail(Explanation detail) {
    if (details == null)
      details = new ArrayList<Explanation>();
    details.add(detail);
  }

  /** Render an explanation as text. */
  @Override
  public String toString() {
    return toString(0);
  }
  protected String toString(int depth) {
    StringBuilder buffer = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      buffer.append("  ");
    }
    buffer.append(getSummary());
    buffer.append("\n");

    Explanation[] details = getDetails();
    if (details != null) {
      for (int i = 0 ; i < details.length; i++) {
        buffer.append(details[i].toString(depth+1));
      }
    }

    return buffer.toString();
  }


  /** Render an explanation as HTML. */
  public String toHtml() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("<ul>\n");

    buffer.append("<li>");
    buffer.append(getSummary());
    buffer.append("<br />\n");

    Explanation[] details = getDetails();
    if (details != null) {
      for (int i = 0 ; i < details.length; i++) {
        buffer.append(details[i].toHtml());
      }
    }

    buffer.append("</li>\n");
    buffer.append("</ul>\n");

    return buffer.toString();
  }
}
