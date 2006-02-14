<?xml version="1.0" encoding="utf-8"?>

<!-- $Id: status.xsl,v 1.4 2005/05/31 20:34:42 ronp Exp $ -->
<!-- $Source: /cvs/main/searching/org.apache.solrSolarServer/resources/admin/status.xsl,v $ -->
<!-- $Name:  $ -->

<xsl:stylesheet
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  version="1.0">


  <xsl:output
    method="html"
    indent="yes"
    doctype-public="-//W3C//DTD HTML 4.01//EN"
    doctype-system="http://www.w3.org/TR/html4/strict.dtd" />

  <xsl:template match="/">
    <html>
      <head>
        <link rel="stylesheet" type="text/css" href="solr-admin.css"></link>
        <link rel="icon" href="/favicon.ico" type="image/ico"></link>
        <link rel="shortcut icon" href="/favicon.ico" type="image/ico"></link>
        <title>SOLR Status</title>
      </head>
      <body>
        <a href=".">
           <img border="0" align="right" height="88" width="215" src="solr-head.gif" alt="SOLR">
           </img>
        </a>
        <h1>SOLR Status (<xsl:value-of select="solr/schema" />)</h1>
          <xsl:value-of select="solr/host" />
          <br clear="all" />
        <xsl:apply-templates/>
        <br /><br />
        <a href=".">Return to Admin Page</a>
      </body>
    </html>
  </xsl:template>

  <xsl:template match="solr">
  <table>
    <tr>
      <td>
      </td>
      <td>
        Current Time: <xsl:value-of select="now" />
      </td>
    </tr>
    <tr>
      <td>
      </td>
      <td>
        Server Start Time: <xsl:value-of select="start" />
      </td>
    </tr>
  </table>
  <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="solr/schema" />

  <xsl:template match="solr/host" />

  <xsl:template match="solr/now" />

  <xsl:template match="solr/start" />

  <xsl:template match="solr/status">
    <br clear="all" />
    <h2>status</h2>
    <table>
      <xsl:for-each select="*">
        <tr>
          <td align="right">
            <strong><xsl:value-of select="name()"/>:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="."/>&#xa0;</tt>
          </td>
        </tr>
      </xsl:for-each>
    </table>
  </xsl:template>


</xsl:stylesheet>
