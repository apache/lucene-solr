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
package org.apache.lucene.gdata.gom.core;

import java.util.Stack;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.lucene.gdata.gom.GOMDocument;
import org.apache.lucene.gdata.gom.GOMEntry;
import org.apache.lucene.gdata.gom.GOMFeed;
import org.apache.lucene.gdata.gom.GOMNamespace;

/**
 * @author Simon Willnauer
 */
public class GOMBuilder {

	private final XMLStreamReader streamReader;

	private final GOMFactory factory;

	private final Stack<AtomParser> parserStack;

	/**
	 * @param arg0
	 */
	public GOMBuilder(XMLStreamReader arg0) {
		if (arg0 == null)
			throw new IllegalArgumentException(
					"XMLStreamReader instance must not be null");
		this.streamReader = arg0;
		this.factory = GOMFactory.createInstance();
		this.parserStack = new Stack<AtomParser>();
	}

	public GOMDocument<GOMFeed> buildGOMFeed() throws XMLStreamException {
		GOMDocument<GOMFeed> document = new GOMDocumentImpl<GOMFeed>();
		GOMFeed element = startFeedDocument(document);
		document.setRootElement(element);
		parse(this.streamReader);

		return document;
	}

	private void parse(XMLStreamReader aReader) throws XMLStreamException {

		int next = 0;

		while ((next = next()) != XMLStreamConstants.END_DOCUMENT) {

			if (next == XMLStreamConstants.START_ELEMENT) {
				AtomParser childParser = this.parserStack.peek()
						.getChildParser(this.streamReader.getName());
				processAttributes(childParser);
				this.parserStack.push(childParser);
			} else if (next == XMLStreamConstants.END_ELEMENT) {
				this.parserStack.pop().processEndElement();
			} else if (next == XMLStreamConstants.CHARACTERS) {
				this.parserStack.peek().processElementValue(
						this.streamReader.getText());
			} else if (next == XMLStreamConstants.CDATA) {
				System.out.println("CDdata");
			}
			// System.out.println(next);
		}

	}

	/**
	 * @param childParser
	 */
	private void processAttributes(AtomParser childParser) {
		int attributeCount = this.streamReader.getAttributeCount();
		for (int i = 0; i < attributeCount; i++) {
			childParser.processAttribute(this.streamReader.getAttributeName(i),
					this.streamReader.getAttributeValue(i));
		}
	}

	public GOMDocument<GOMEntry> buildGOMEntry() throws XMLStreamException {
		GOMDocument<GOMEntry> document = new GOMDocumentImpl<GOMEntry>();
		GOMEntry element = startEntryDocument(document);
		document.setRootElement(element);
		parse(this.streamReader);

		return document;

	}

	private GOMEntry startEntryDocument(GOMDocument aDocument)
			throws XMLStreamException {
		aDocument.setVersion(this.streamReader.getVersion());
		aDocument.setCharacterEncoding(this.streamReader
				.getCharacterEncodingScheme());
		GOMEntry entry = this.factory.createEntry();
		if (next() != XMLStreamConstants.START_ELEMENT)
			throw new GDataParseException("Expected start of feed element");
		processAttributes(entry);
		this.parserStack.push(entry);
		int count = this.streamReader.getNamespaceCount();
		for (int i = 0; i < count; i++) {
			GOMNamespace namespace = new GOMNamespace(this.streamReader
					.getNamespaceURI(i), this.streamReader
					.getNamespacePrefix(i));
			entry.addNamespace(namespace);
		}
		return entry;
	}

	private GOMFeed startFeedDocument(GOMDocument aDocument)
			throws XMLStreamException {
		aDocument.setVersion(this.streamReader.getVersion());
		aDocument.setCharacterEncoding(this.streamReader
				.getCharacterEncodingScheme());
		GOMFeed feed = this.factory.createFeed();
		if (next() != XMLStreamConstants.START_ELEMENT)
			throw new GDataParseException("Expected start of feed element");
		processAttributes(feed);
		this.parserStack.push(feed);
		int count = this.streamReader.getNamespaceCount();
		for (int i = 0; i < count; i++) {

			GOMNamespace namespace = new GOMNamespace(this.streamReader
					.getNamespaceURI(i), this.streamReader
					.getNamespacePrefix(i));
			System.out.println(namespace);
			feed.addNamespace(namespace);
		}
		return feed;
	}

	private int next() throws XMLStreamException {
		return this.streamReader.next();

	}

}
