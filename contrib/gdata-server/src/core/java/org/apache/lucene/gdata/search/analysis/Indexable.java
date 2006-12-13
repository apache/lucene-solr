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
package org.apache.lucene.gdata.search.analysis;

import javax.xml.xpath.XPathExpressionException;

import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.w3c.dom.Node;

/**
 * This class wraps the access to the GData entities to access them via xpath
 * expressions. An arbitrary valid Xpath expression can be passed to the
 * <tt>applyPath</tt> method to access an element, attribute etc. in the gdata
 * entity.
 * 
 * @author Simon Willnauer
 * @param <R> -
 *            a subtype of {@link org.w3c.dom.Node} returned by the applyPath
 *            method
 * @param <I> -
 *            a subtype of {@link org.apache.lucene.gdata.data.ServerBaseEntry}
 */
public abstract class Indexable<R extends Node, I extends ServerBaseEntry> {
    protected ServerBaseEntry applyAble;

    /**
     * @param applyAble
     */
    Indexable(I applyAble) {
        this.applyAble = applyAble;
    }

    /**
     * @param xPath -
     *            a valid xpath expression
     * @return - the requested element <b>R</b>
     * @throws XPathExpressionException
     */
    public abstract R applyPath(String xPath) throws XPathExpressionException;

    /**
     * Factory method to create new <tt>Indexable</tt> instances.
     * 
     * @param <R> -
     *            a subtype of {@link org.w3c.dom.Node} returned by the
     *            applyPath method
     * @param <I> -
     *            a subtype of
     *            {@link org.apache.lucene.gdata.data.ServerBaseEntry}
     * @param entry -
     *            the entry to wrap in a <tt>Indexable</tt>
     * @return - a new instance of <tt>Indexable</tt> to access the entry via
     *         Xpath
     * @throws NotIndexableException - if <b>I<b> can not be parsed. 
     */
    public static <R extends Node, I extends ServerBaseEntry> Indexable<R, I> getIndexable(
            I entry) throws NotIndexableException {
        return new DomIndexable<R, I>(entry);
    }

}
