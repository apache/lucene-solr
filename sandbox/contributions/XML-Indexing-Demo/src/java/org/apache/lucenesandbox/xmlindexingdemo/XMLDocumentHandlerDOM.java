package org.apache.lucenesandbox.xmlindexingdemo;

import org.w3c.dom.*;
import org.w3c.dom.Node;
import javax.xml.parsers.*;
import org.apache.lucene.document.Field;

import java.io.File;

/**
 *
 */
public class XMLDocumentHandlerDOM
{
    public org.apache.lucene.document.Document createXMLDocument(File f)
    {
	org.apache.lucene.document.Document document = new org.apache.lucene.document.Document();
	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	try
        {
	    DocumentBuilder df = dbf.newDocumentBuilder();
	    org.w3c.dom.Document d = df.parse(f);
	    Node root = d.getDocumentElement();
	    traverseTree(root, document);
	}
	catch (Exception e)
        {
	    System.out.println("error: " + e);
	    e.printStackTrace();
	}
	return document;
    }

    static private void traverseTree(Node node, org.apache.lucene.document.Document document)
    {
	NodeList nl = node.getChildNodes();
	if (nl.getLength() == 0)
        {
	    if (node.getNodeType() == Node.TEXT_NODE)
	    {
		Node parentNode = node.getParentNode();
		if (parentNode.getNodeType() == Node.ELEMENT_NODE)
                {
		    String parentNodeName = parentNode.getNodeName();
// 		    String nodeValue = node.getNodeValue();
// 		    if (parentNodeName.equals("name"))
// 		    {
			Node siblingNode = node.getNextSibling();
			if (siblingNode != null)
                        {
			    if (siblingNode.getNodeType() == Node.CDATA_SECTION_NODE)
			    {
				document.add(Field.Text("name", siblingNode.getNodeValue()));
			    }
 			}
// 		    }
// 		    else if (parentNodeName.equals("profession"))
// 		    {
// 			Node siblingNode = node.getNextSibling();
// 			if (siblingNode != null)
//                         {
// 			    if (siblingNode.getNodeType() == Node.CDATA_SECTION_NODE)
//                             {
// 				document.add(Field.Text([arentNodeName, siblingNode.getNodeValue()));
// 			    }
// 			}
// 		    }
// 		    else if (parentNodeName == "addressLine1")
//                     {
// 			Node siblingNode = node.getNextSibling();
// 			if(siblingNode != null)
// 			{
// 			    if (siblingNode.getNodeType() == Node.CDATA_SECTION_NODE)
// 		            {
// 				document.add(Field.Text("addressLine1", siblingNode.getNodeValue()));
// 			    }
// 			}
// 		    }
// 		    else if (parentNodeName.equals("addressLine2"))
// 		    {
// 			Node siblingNode = node.getNextSibling();
// 			if (siblingNode != null)
// 			{
// 			    if (siblingNode.getNodeType() == Node.CDATA_SECTION_NODE)
// 			    {
// 				document.add(Field.Text("addressLine2", siblingNode.getNodeValue()));
// 			    }
// 			}
// 		    }
// 		    if (parentNodeName.equals("city"))
// 		    {
// 			Node siblingNode = node.getNextSibling();
// 			if (siblingNode != null)
//                         {
// 			    if (siblingNode.getNodeType() == Node.CDATA_SECTION_NODE)
// 			    {
// 				document.add(Field.Text("city", siblingNode.getNodeValue()));
// 			    }
// 			}
// 		    }
// 		    else if (parentNodeName.equals("zip"))
// 		    {
// 			Node siblingNode = node.getNextSibling();
// 			if (siblingNode != null)
// 			{
// 			    if (siblingNode.getNodeType() == Node.CDATA_SECTION_NODE)
// 			    {
// 				document.add(Field.Text("zip", siblingNode.getNodeValue()));
// 			    }
// 			}
// 		    }
// 		    else if (parentNodeName.equals("state"))
// 		    {
// 			Node siblingNode = node.getNextSibling();
// 			if (siblingNode != null)
// 			{
// 			    if (siblingNode.getNodeType() == Node.CDATA_SECTION_NODE)
// 			    {
// 				document.add(Field.Text("state", siblingNode.getNodeValue()));
// 			    }
// 			}
// 		    }
// 		    else if (parentNodeName.equals("country"))
// 		    {
// 			Node siblingNode = node.getNextSibling();
// 			if (siblingNode != null)
// 			{
// 			    if (siblingNode.getNodeType() == Node.CDATA_SECTION_NODE)
// 			    {
// 				document.add(Field.Text("country", siblingNode.getNodeValue()));
// 			    }
// 			}
// 		    }
		}
	    }
        }
        else
        {
	    for(int i=0; i<nl.getLength(); i++)
            {
		traverseTree(nl.item(i), document);
	    }
        }
    }
}
