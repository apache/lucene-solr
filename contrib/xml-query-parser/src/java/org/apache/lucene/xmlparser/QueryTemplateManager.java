package org.apache.lucene.xmlparser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 * Provides utilities for turning query form input (such as from a web page or Swing gui) into 
 * Lucene XML queries by using XSL templates.  This approach offers a convenient way of externalizing 
 * and changing how user input is turned into Lucene queries. 
 * Database applications often adopt similar practices by externalizing SQL in template files that can
 * be easily changed/optimized by a DBA.  
 * @author Mark Harwood
 */
public class QueryTemplateManager
{
	static DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance ();
	static TransformerFactory tFactory = TransformerFactory.newInstance();
	
	public static String getQueryAsXmlString(Properties formProperties, String templateName) throws SAXException, IOException, ParserConfigurationException, TransformerException 
	{
		return getQueryAsXmlString(formProperties, 
				getDOMSource(QueryTemplateManager.class.getResourceAsStream(templateName)));
	}
	
	public static String getQueryAsXmlString(Properties formProperties, Source xslDs) throws SAXException, IOException, ParserConfigurationException, TransformerException
	{
  		ByteArrayOutputStream baos=new ByteArrayOutputStream();
  		StreamResult result=new StreamResult(baos);
  		transformCriteria(formProperties,xslDs,result);
  		return baos.toString();
	}
	
	public static Document getQueryAsDOM(Properties formProperties, String templateName) throws SAXException, IOException, ParserConfigurationException, TransformerException
	{
		return getQueryAsDOM(formProperties, getDOMSource(QueryTemplateManager.class.getResourceAsStream(templateName)));
	}
	public static Document getQueryAsDOM(Properties formProperties, InputStream xslIs) throws SAXException, IOException, ParserConfigurationException, TransformerException
	{
		return getQueryAsDOM(formProperties, getDOMSource(xslIs));
	}
	
	
	public static Document getQueryAsDOM(Properties formProperties, Source xslDs) throws SAXException, IOException, ParserConfigurationException, TransformerException
	{
  		DOMResult result=new DOMResult();
  		transformCriteria(formProperties,xslDs,result);
  		return (Document)result.getNode();
	}
	
	public static void transformCriteria(Properties formProperties, Source xslDs, Result result) throws SAXException, IOException, ParserConfigurationException, TransformerException
	{
        dbf.setNamespaceAware(true);	    
		
		Transformer transformer = tFactory.newTransformer(xslDs);
	    //Create an XML document representing the search index document.
		DocumentBuilder db = dbf.newDocumentBuilder ();
		org.w3c.dom.Document doc = db.newDocument ();
		Element root = doc.createElement ("Document");
		doc.appendChild (root);
		
		Enumeration keysEnum = formProperties.keys();
		while(keysEnum.hasMoreElements())
		{
		    String propName=(String) keysEnum.nextElement();
		    String value=formProperties.getProperty(propName);
    		if((value!=null)&&(value.length()>0))
    		{
    		    DOMUtils.insertChild(root,propName,value);    			
    		}
		}		
		//Use XSLT to to transform into an XML query string using the  queryTemplate
		DOMSource xml=new DOMSource(doc);
		transformer.transform(xml,result);		
	}
	
	public static DOMSource getDOMSource(InputStream xslIs) throws ParserConfigurationException, SAXException, IOException 
	{
        dbf.setNamespaceAware(true);	    
		DocumentBuilder builder = dbf.newDocumentBuilder();
		org.w3c.dom.Document xslDoc = builder.parse(xslIs);
		return new DOMSource(xslDoc);		
	}
}
