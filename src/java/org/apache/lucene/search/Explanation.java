package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2003 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.util.ArrayList;

/** Expert: Describes the score computation for document and query. */
public class Explanation implements java.io.Serializable {
  private float value;                            // the value of this node
  private String description;                     // what it represents
  private ArrayList details;                      // sub-explanations

  public Explanation() {}

  public Explanation(float value, String description) {
    this.value = value;
    this.description = description;
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

  /** The sub-nodes of this explanation node. */
  public Explanation[] getDetails() {
    if (details == null)
      return null;
    return (Explanation[])details.toArray(new Explanation[0]);
  }

  /** Adds a sub-node to this explanation node. */
  public void addDetail(Explanation detail) {
    if (details == null)
      details = new ArrayList();
    details.add(detail);
  }

  /** Render an explanation as HTML. */
  public String toString() {
    return toString(0);
  }
  private String toString(int depth) {
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < depth; i++) {
      buffer.append("  ");
    }
    buffer.append(getValue());
    buffer.append(" = ");
    buffer.append(getDescription());
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
    StringBuffer buffer = new StringBuffer();
    buffer.append("<ul>\n");

    buffer.append("<li>");
    buffer.append(getValue());
    buffer.append(" = ");
    buffer.append(getDescription());
    buffer.append("</li>\n");

    Explanation[] details = getDetails();
    if (details != null) {
      for (int i = 0 ; i < details.length; i++) {
        buffer.append(details[i].toHtml());
      }
    }

    buffer.append("</ul>\n");

    return buffer.toString();
  }
}
