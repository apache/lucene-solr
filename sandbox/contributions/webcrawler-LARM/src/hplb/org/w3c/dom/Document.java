/*
 * $Id$
 */

package hplb.org.w3c.dom;

/**
 * 
 */
public interface Document extends DocumentFragment {
    public Node             getDocumentType();
    public void             setDocumentType(Node arg);
    
    public Element          getDocumentElement();
    public void             setDocumentElement(Element arg);
    
    public DocumentContext  getContextInfo();
    public void             setContextInfo(DocumentContext arg);
    
    public DocumentContext  createDocumentContext();
    public Element          createElement(String tagName, AttributeList attributes);
    public Text             createTextNode(String data);
    public Comment          createComment(String data);
    public PI               createPI(String name, String data);
    public Attribute        createAttribute(String name, Node value);
    public AttributeList    createAttributeList();
    public NodeIterator     getElementsByTagName();
}
