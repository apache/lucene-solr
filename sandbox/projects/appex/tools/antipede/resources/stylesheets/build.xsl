<?xml version="1.0" ?>
<!-- Create documentation from an ant build file  -->
<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform" >
	<xsl:output method="html"  indent="yes"/>
	
	<xsl:template match="project">
		<html>
		<body>
		<h1><xsl:value-of select="./@name"/></h1>
			<pre><xsl:value-of select="./description"/></pre>
			<br />
			<xsl:apply-templates select="target"/>
		</body>
		</html>
	</xsl:template >
	
	<xsl:template match="*[ @description | @taskname]">
	<!-- use taskname if it exists, otherwise use the task element name-->
		<li><b>
		<xsl:if test="not(@taskname)">
				<xsl:value-of select="name()"/>
		</xsl:if>
		<xsl:value-of select="@taskname"/></b>
		<xsl:text> </xsl:text>
		<xsl:value-of select="@description"/>
		</li>
	</xsl:template>
	
	<xsl:template match="//target">
	  
	  <xsl:if test = "not(starts-with(@name,'-'))">
	   <p>
		<b><xsl:value-of select="@name"/></b> - 
		<xsl:value-of select="@description"/>
		<ul><xsl:apply-templates select="./*"/></ul>
		</p>
	  </xsl:if>
	</xsl:template>
		
	<xsl:template match="*"/>
	
</xsl:stylesheet>
