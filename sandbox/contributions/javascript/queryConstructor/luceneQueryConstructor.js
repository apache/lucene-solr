// Lucene Search Query Constructor
// Author:  Kelvin Tan  (kelvint at apache.org)
// Version: $Id$

// Change this according to what you use to name the field modifiers in your form.
// e.g. with the field "name", the modifier will be called "nameModifier"
var modifierSuffix = 'Modifier';

// Do you wish the query to be displayed as an alert box?
var debug = false;

// Do you wish the function to submit the form upon query construction?
var submitOnConstruction = false;

// prefix modifier for boolean AND queries
var AND_MODIFIER = '+';

// prefix modifier for boolean NOT queries
var NOT_MODIFIER = '-';

// prefix modifier for boolean OR queries
var OR_MODIFIER  = '';

// default prefix modifier for boolean queries
var DEFAULT_MODIFIER = OR_MODIFIER;

// used to delimit multiple values from checkboxes and select lists
var VALUE_DELIMITER = ' ';

// Constructs the query
// @param query Form field to represent the constructed query to be submitted
// @param debug Turn on debugging?
// @return Submits the form if submitOnConstruction=true, else returns the query param
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
      var elementValue = trim(getFieldValue(frm[element.name]));

      if(elementValue.length > 0 && elementValue != ' ')
      {
        var subElement = frm[elementName + modifierSuffix];
        if(typeof(subElement) != "undefined") // found a field/fieldModifier pair
        {
          // assume that the user only allows one logic, i.e. AND OR, AND NOT, OR NOT, etc not supported
          var logic = getFieldValue(subElement);

          if(logic == 'And')
          {
            addFieldWithModifier(query, AND_MODIFIER, elementName, elementValue);
          }
          else if(logic == 'Not')
          {
            addFieldWithModifier(query, NOT_MODIFIER, elementName, elementValue);
          }
          else if(logic == 'Or')
          {
            addFieldWithModifier(query, OR_MODIFIER, elementName, elementValue);
          }
          else
          {
            addFieldWithModifier(query, DEFAULT_MODIFIER, elementName, elementValue);
          }
        }
      }
    }
  }

  if(debug)
  {
    alert('Query:' + query.value);
  }

  if(submitOnConstruction)
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
  var temp = '';
  splitStr = query.value.split(" ");
  query.value = '';
  for(var i=0;i<splitStr.length;i++)
  {
    if(splitStr[i].length > 0) addModifier(query, AND_MODIFIER, splitStr[i]);
  }
  if(submitOnConstruction)
  {
    frm.submit();
  }
  else
  {
    return query;
  }
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

function getFieldValue(field)
{
  if(typeof(field[0]) != "undefined" && field[0].type=="checkbox")
    return getCheckedValues(field);
  if(typeof(field[0]) != "undefined" && field[0].type=="radio")
    return getRadioValue(field);
  if(field.type.match("select*"))
    return getSelectedValues(field);

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

function addModifier(query, modifier, value)
{
  value = trim(value);
  
  if(query.value.length == 0)
  {
    query.value = modifier + '(' + value + ')';
  }
  else
  {
    query.value = query.value + ' ' + modifier + '(' + value + ')';
  }  
}

function addFieldWithModifier(query, modifier, field, fieldValue)
{
  fieldValue = trim(fieldValue);

  if(query.value.length == 0)
  {
    query.value = modifier + '(' + field + ':(' + fieldValue + '))';
  }
  else
  {
    query.value = query.value + ' ' + modifier + '(' + field + ':(' + fieldValue + '))';
  }
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
   return temp; // Return the trimmed string back to the user
}
