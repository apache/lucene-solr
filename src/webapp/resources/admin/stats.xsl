<?xml version="1.0" encoding="utf-8"?>

<!-- $Id$ -->
<!-- $URL$ -->

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
        <title>SOLR Statistics</title>
      </head>
      <body>
        <a href=".">
	   <img border="0" align="right" height="61" width="142" src="solr-head.gif" alt="SOLR">
	   </img>
	</a>
        <h1>SOLR Statistics (<xsl:value-of select="solr/schema" />)</h1>
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
        <H3>Category</H3>
      </td>
      <td>
        [<a href="#core">Core</a>]
        [<a href="#cache">Cache</a>]
        [<a href="#query">Query</a>]
        [<a href="#update">Update</a>]
        [<a href="#other">Other</a>]
      </td>
    </tr>
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

  <xsl:template match="solr/solr-info">
  <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="solr/solr-info/CORE">
    <br />
    <a name="core"><h2>Core</h2></a>
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
    <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="solr/solr-info/CORE/entry">
        <tr>
          <td align="right">
            <strong>name:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="name"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>class:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="class"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>version:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="version"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>description:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="description"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>stats:&#xa0;</strong>
          </td>
          <td>
            <xsl:for-each select="stats/stat[@name]">
              <xsl:value-of select="@name"/>
              <xsl:text> : </xsl:text>
              <xsl:variable name="name" select="@name" />
              <xsl:value-of select="." /><br />
            </xsl:for-each>
          </td>
        </tr>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>

  <xsl:template match="solr/solr-info/CACHE">
    <br />
    <a name="cache"><h2>Cache</h2></a>
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
    <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="solr/solr-info/CACHE/entry">
        <tr>
          <td align="right">
            <strong>name:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="name"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>class:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="class"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>version:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="version"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>description:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="description"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>stats:&#xa0;</strong>
          </td>
          <td>
            <xsl:for-each select="stats/stat[@name]">
              <xsl:value-of select="@name"/>
              <xsl:text> : </xsl:text>
              <xsl:variable name="name" select="@name" />
              <xsl:value-of select="." /><br />
            </xsl:for-each>
          </td>
        </tr>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>

  <xsl:template match="solr/solr-info/QUERYHANDLER">
    <br />
    <a name="query"><h2>Query Handlers</h2></a>
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
    <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="solr/solr-info/QUERYHANDLER/entry">
        <tr>
          <td align="right">
            <strong>name:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="name"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>class:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="class"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>version:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="version"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>description:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="description"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>stats:&#xa0;</strong>
          </td>
          <td>
            <xsl:for-each select="stats/stat[@name]">
              <xsl:value-of select="@name"/>
              <xsl:text> : </xsl:text>
              <xsl:variable name="name" select="@name" />
              <xsl:value-of select="." /><br />
            </xsl:for-each>
          </td>
        </tr>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>

  <xsl:template match="solr/solr-info/UPDATEHANDLER">
    <br />
    <a name="update"><h2>Update Handlers</h2></a>
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
    <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="solr/solr-info/UPDATEHANDLER/entry">
        <tr>
          <td align="right">
            <strong>name:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="name"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>class:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="class"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>version:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="version"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>description:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="description"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>stats:&#xa0;</strong>
          </td>
          <td>
            <xsl:for-each select="stats/stat[@name]">
              <xsl:value-of select="@name"/>
              <xsl:text> : </xsl:text>
              <xsl:variable name="name" select="@name" />
              <xsl:value-of select="." /><br />
            </xsl:for-each>
          </td>
        </tr>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>

  <xsl:template match="solr/solr-info/OTHER">
    <br />
    <a name="other"><h2>Other</h2></a>
    <table>
        <tr>
          <td align="right">
            &#xa0;
          </td>
          <td>
          </td>
        </tr>
    <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="solr/solr-info/OTHER/entry">
        <tr>
          <td align="right">
            <strong>name:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="name"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>class:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="class"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>version:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="version"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>description:&#xa0;</strong>
          </td>
          <td>
            <tt><xsl:value-of select="description"/>&#xa0;</tt>
          </td>
        </tr>
        <tr>
          <td align="right">
            <strong>stats:&#xa0;</strong>
          </td>
          <td>
            <xsl:for-each select="stats/stat[@name]">
              <xsl:value-of select="@name"/>
              <xsl:text> : </xsl:text>
              <xsl:variable name="name" select="@name" />
              <xsl:value-of select="." /><br />
            </xsl:for-each>
          </td>
        </tr>
        <tr>
          <td align="right">
          </td>
          <td>
          </td>
        </tr>
  </xsl:template>

</xsl:stylesheet>
