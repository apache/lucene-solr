// Lucene Search Query Constructor
// Author:  Kelvin Tan  (kelvint at apache.org)
// Version: $Id$

// Change this according to what you use to name the field modifiers in your form.
// e.g. with the search field "name", the form element of its modifier 
// will be "name<modifierSuffix>"
var modifierSuffix = 'Modifier';

// If not a field-specific search
// e.g. with the arbitary form element 'foobar', its modifier will be 
// <noFieldPrefix>foobarModifier and its form element 
// will be <noFieldPrefix>foobar
var noFieldPrefix = 'noField-';

// Do you wish the query to be displayed as an alert box?
var debug = false;

// Do you wish the function to submit the form upon query construction?
var submitForm = false;

// prefix modifier for boolean AND queries
var AND_MODIFIER = '+';

// prefix modifier for boolean NOT queries
var NOT_MODIFIER = '-';

// prefix modifier for boolean OR queries
var OR_MODIFIER  = ' ';

var NO_MODIFIER = 0;

// default modifier for terms
var DEFAULT_TERM_MODIFIER = AND_MODIFIER;

// default modifier for groups of terms (denoted by parantheses)
var DEFAULT_GROUP_MODIFIER = AND_MODIFIER;

// used to delimit multiple values from checkboxes and select lists
var VALUE_DELIMITER = ' ';

// Constructs the query
// @param query Form field to represent the constructed query to be submitted
// @param debug Turn on debugging?
function doMakeQuery( query, dbg )
{
  if(typeof(dbg) != "undefined")
    debug = dbg;
    
  var frm = query.form;
  var formElements = frm.elements;
  query.value = '';
  
  // keep track of the fields we've examined
  var dict = new Array();
  
  for(var i=0; i<formElements.length; i++)
  {
    var element = formElements[i];
    var elementName = element.name;
    if(elementName != "" && !contains(dict, elementName))
    {
      dict[dict.length] = elementName;
      
      // ensure we get the whole group (of checkboxes, radio, etc), if applicable
      var elementValue = getFieldValue(frm[element.name]);
      if(elementValue.length > 0)
      {
        var subElement = frm[elementName + modifierSuffix];
        if(typeof(subElement) != "undefined") // found a field/fieldModifier pair
        {
          var termMod, groupMod;
          var modifier = getFieldValue(subElement);
          // modifier field is in the form <termModifier>|<groupModifier>
          if(modifier.indexOf('|') > -1)
          {
            var idx = modifier.indexOf('|');
            termMod = modifier.substring(0, idx);
            if(termMod == '') termMod = DEFAULT_TERM_MODIFIER;
            groupMod = modifier.substring(idx + 1);
            if(groupMod == '') groupMod = DEFAULT_GROUP_MODIFIER;
          }
          else
          {
            termMod = modifier;
            if(termMod == '') termMod = DEFAULT_TERM_MODIFIER;
            groupMod = DEFAULT_GROUP_MODIFIER;
          }
          appendTerms(query, termMod, elementValue, elementName, groupMod);
        }
      }
    }
  }

  if(debug) {alert('Query:' + query.value);}
  
  if(submitForm)
  {
    frm.submit();
  }
  else
  {
    return query;
  }
}

// Constructs a Google-like query (all terms are ANDed)
// @param query Form field to represent the constructed query to be submitted
// @return Submits the form if submitOnConstruction=true, else returns the query param
function doANDTerms(query)
{
  appendTerms(query, AND_MODIFIER, query.value);
  if(submitForm)
  {
    frm.submit();
  }
  else
  {
    return query;
  }
}

function buildTerms(termModifier, fieldValue)
{
  fieldValue = trim(fieldValue);
  var splitStr = fieldValue.split(" ");
  fieldValue = '';
  var inQuotes = false;
  for(var i=0;i<splitStr.length;i++)
  {
    if(splitStr[i].length > 0)
    {
      if(!inQuotes)
      {
        fieldValue = fieldValue + termModifier + splitStr[i] + ' ';
      }
      else
      { 
        fieldValue = fieldValue + splitStr[i] + ' ';
      }      
      if(splitStr[i].indexOf('"') > -1) inQuotes = !inQuotes
    }
  }
  fieldValue = trim(fieldValue);  
  return fieldValue;
}

// Appends terms to a query. 
// @param query Form field of query
// @param termModifier Term modifier
// @param value Value to be appended. Tokenized by spaces, 
//    and termModifier will be applied to each token.
// @param fieldName Name of search field. Omit if not a field-specific query.
// @param groupModifier Modifier applied to each group of terms.
// @return query form field
function appendTerms(query, termModifier, value, fieldName, groupModifier)
{
  if(typeof(groupModifier) == "undefined")
    groupModifier = DEFAULT_GROUP_MODIFIER;
  
  value = buildTerms(termModifier, value);
  
  // not a field-specific search
  if(fieldName == null || fieldName.indexOf(noFieldPrefix) != -1 || fieldName.length == 0)
  {
    if(groupModifier == NO_MODIFIER)
    {
      if(query.value.length == 0)
      {
        query.value = value;
      }
      else
      {
        query.value = query.value + ' ' + value;
      }  
    }
    else
    { 
      if(query.value.length == 0)
      {
        query.value = groupModifier + '(' + value + ')';
      }
      else
      {
        query.value = query.value + ' ' + groupModifier + '(' + value + ')';
      }  
    }
  }
  else
  {
    if(query.value.length == 0)
    {
      query.value = groupModifier + fieldName + ':(' + value + ')';
    }
    else
    {
      query.value = query.value + ' ' + groupModifier +fieldName + ':(' + value + ')';
    }  
  }
  query.value = trim(query.value)
  return query;
}

// Obtain the value of a form field.
// @param field Form field
// @return Array of values, or string value depending on field type, 
//    or empty string if field param is undefined or null
function getFieldValue(field)
{
  if(field == null || typeof(field) == "undefined")
    return "";
  if(typeof(field) != "undefined" && typeof(field[0]) != "undefined" && field[0].type=="checkbox")
    return getCheckedValues(field);
  if(typeof(field) != "undefined" && typeof(field[0]) != "undefined" && field[0].type=="radio")
    return getRadioValue(field);
  if(typeof(field) != "undefined" && field.type.match("select*")) 
    return getSelectedValues(field);
  if(typeof(field) != "undefined")
    return field.value;
}

function getRadioValue(radio)
{
  for(var i=0; i<radio.length; i++)
  {
    if(radio[i].checked)
      return radio[i].value;
  }
}

function getCheckedValues(checkbox)
{
  var r = new Array();
  for(var i = 0; i < checkbox.length; i++)
  {
    if(checkbox[i].checked)
      r[r.length] = checkbox[i].value;
  }
  return r.join(VALUE_DELIMITER);
}

function getSelectedValues (select) {
  var r = new Array();
  for (var i = 0; i < select.options.length; i++)
    if (select.options[i].selected)
    {
      r[r.length] = select.options[i].value;
    }
  return r.join(VALUE_DELIMITER);
}

function quote(value)
{
  return "\"" + trim(value) + "\"";
}

function contains(array, s)
{
  for(var i=0; i<array.length; i++)
  {
    if(s == array[i])
      return true;
  }
  return false;
}

function trim(inputString) {
   if (typeof inputString != "string") { return inputString; }
   
   var temp = inputString;
   
   // Replace whitespace with a single space
   var pattern = /\s+/ig;
   temp = temp.replace(pattern, " ");
  
   // Trim 
   pattern = /^(\s*)([\w\W]*)(\b\s*$)/;
   if (pattern.test(temp)) { temp = temp.replace(pattern, "$2"); }
   // run it another time through for words which don't end with a character or a digit
   pattern = /^(\s*)([\w\W]*)(\s*$)/;
   if (pattern.test(temp)) { temp = temp.replace(pattern, "$2"); }
   return temp; // Return the trimmed string back to the user
}