/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.tagger;

import javax.xml.stream.XMLResolver;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import java.io.InputStream;
import java.io.StringReader;

import com.ctc.wstx.stax.WstxInputFactory;
import org.apache.commons.io.input.ClosedInputStream;
import org.codehaus.stax2.LocationInfo;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLStreamReader2;

/**
 * Corrects offsets to adjust for XML formatted data. The goal is such that the caller should be
 * able to insert a start XML tag at the start offset and a corresponding end XML tag at the end
 * offset of the tagger, and have it be valid XML.  See {@link #correctPair(int, int)}.
 *
 * This will not work on invalid XML.
 *
 * Not thread-safe.
 */
public class XmlOffsetCorrector extends OffsetCorrector {

  //TODO use StAX without hard requirement on woodstox.   xmlStreamReader.getLocation().getCharacterOffset()

  private static final XMLInputFactory2 XML_INPUT_FACTORY;
  static {
    // note: similar code in Solr's EmptyEntityResolver
    XML_INPUT_FACTORY = new WstxInputFactory();
    XML_INPUT_FACTORY.setXMLResolver(new XMLResolver() {
      @Override
      public InputStream resolveEntity(String publicId, String systemId, String baseURI, String namespace) {
        return ClosedInputStream.CLOSED_INPUT_STREAM;
      }
    });
    // TODO disable DTD?
    // XML_INPUT_FACTORY.setProperty(XMLInputFactory.IS_VALIDATING, Boolean.FALSE)
    XML_INPUT_FACTORY.configureForSpeed();
  }

  /**
   * Initialize based on the document text.
   * @param docText non-null XML content.
   * @throws XMLStreamException If there's a problem parsing the XML.
   */
  public XmlOffsetCorrector(String docText) throws XMLStreamException {
    super(docText, false);

    int tagCounter = 0;
    int thisTag = -1;

    //note: we *could* add a virtual outer tag to guarantee all text is in the context of a tag,
    // but we shouldn't need to because there is no findable text outside the top element.

    final XMLStreamReader2 xmlStreamReader =
            (XMLStreamReader2) XML_INPUT_FACTORY.createXMLStreamReader(new StringReader(docText));

    while (xmlStreamReader.hasNext()) {
      int eventType = xmlStreamReader.next();
      switch (eventType) {
        case XMLEvent.START_ELEMENT: {
          tagInfo.ensureCapacity(tagInfo.size() + 5);
          final int parentTag = thisTag;
          final LocationInfo info = xmlStreamReader.getLocationInfo();
          tagInfo.add(parentTag);
          tagInfo.add((int) info.getStartingCharOffset(), (int) info.getEndingCharOffset());
          tagInfo.add(-1, -1);//these 2 will be populated when we get to the close tag
          thisTag = tagCounter++;

          parentChangeOffsets.add((int) info.getStartingCharOffset());
          parentChangeIds.add(thisTag);
          break;
        }
        case XMLEvent.END_ELEMENT: {
          final LocationInfo info = xmlStreamReader.getLocationInfo();
          tagInfo.set(5 * thisTag + 3, (int) info.getStartingCharOffset());
          tagInfo.set(5 * thisTag + 4, (int) info.getEndingCharOffset());
          thisTag = getParentTag(thisTag);

          parentChangeOffsets.add((int) info.getEndingCharOffset());
          parentChangeIds.add(thisTag);
          break;
        }
        default: //do nothing
      }
    }
  }

}
