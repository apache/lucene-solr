package org.apache.solr.core;

import net.sf.saxon.event.PipelineConfiguration;
import net.sf.saxon.om.AttributeInfo;
import net.sf.saxon.om.AttributeMap;
import net.sf.saxon.om.NamespaceMap;
import net.sf.saxon.om.NodeName;
import net.sf.saxon.s9api.Location;
import net.sf.saxon.tree.tiny.TinyBuilder;
import net.sf.saxon.type.SchemaType;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.PropertiesUtil;

import java.util.Properties;

public class SolrTinyBuilder extends TinyBuilder  {
  private final Properties substituteProps;

  /**
   * Create a TinyTree builder
   *
   * @param pipe information about the pipeline leading up to this Builder
   * @param substituteProps
   */
  public SolrTinyBuilder(PipelineConfiguration pipe, Properties substituteProps) {
    super(pipe);
    this.substituteProps = substituteProps;
  }

  protected int makeTextNode(CharSequence chars, int len) {
    String sub = PropertiesUtil
        .substituteProperty(chars.subSequence(0, len).toString(),
            substituteProps);

    return super.makeTextNode(sub, sub.length());
  }

  protected String getAttValue(AttributeInfo att) {
    String sub = PropertiesUtil
        .substituteProperty(att.getValue(),
            substituteProps);
    return sub;
  }

}
