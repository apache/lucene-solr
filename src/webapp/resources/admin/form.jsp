<%@include file="header.jsp" %>

<br clear="all">
<h2>/select mode</h2>

<form method="GET" action="../select">
<table>
<tr>
  <td>
	<strong>Solr/Lucene Statement</strong>
  </td>
  <td>
	<textarea rows="5" cols="60" name="q"></textarea>
  </td>
</tr>
<tr>
  <td>
	<strong>Protocol Version</strong>
  </td>
  <td>
	<input name="version" type="text" value="2.1">
  </td>
</tr>
<tr>
  <td>
	<strong>Start Row</strong>
  </td>
  <td>
	<input name="start" type="text" value="0">
  </td>
</tr>
<tr>
  <td>
	<strong>Maximum Rows Returned</strong>
  </td>
  <td>
	<input name="rows" type="text" value="10">
  </td>
</tr>
<tr>
  <td>
	<strong>Fields to Return</strong>
  </td>
  <td>
	<input name="fl" type="text" value="">
  </td>
</tr>
<tr>
  <td>
	<strong>Query Type</strong>
  </td>
  <td>
	<input name="qt" type="text" value="standard">
  </td>
</tr>
<tr>
  <td>
	<strong>Style Sheet</strong>
  </td>
  <td>
	<input name="stylesheet" type="text" value="">
  </td>
</tr>
<tr>
  <td>
	<strong>Indent XML</strong>
  </td>
  <td>
	<input name="indent" type="checkbox" checked="true">
  </td>
</tr>
<tr>
  <td>
	<strong>Debug: enable</strong>
  </td>
  <td>
	<input name="debugQuery" type="checkbox" >
  <em><font size="-1">  Note: do "view source" in your browser to see explain() correctly indented</font></em>
  </td>
</tr>
<tr>
  <td>
	<strong>Debug: explain others</strong>
  </td>
  <td>
	<input name="explainOther" type="text" >
  <em><font size="-1">  apply original query scoring to matches of this query</font></em>
  </td>
</tr>
<tr>
  <td>
  </td>
  <td>
	<input class="stdbutton" type="submit" value="search">
  </td>
</tr>
</table>
</form>


</body>
</html>
