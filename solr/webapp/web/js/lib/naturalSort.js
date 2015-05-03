/*

naturalSort.js
- by Jim Palmer and other contributors

The MIT License (MIT)

Copyright (c) 2011 Jim Palmer and other contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

#     naturalSort.js 0.7.0
#     https://github.com/jarinudom/naturalSort.js
#     (c) 2011 Jim Palmer and other contributors
#     naturalSort.js may be freely distributed under the MIT license.
window.naturalSort = (a, b) ->
  re = /(^([+\-]?(?:0|[1-9]\d*)(?:\.\d*)?(?:[eE][+\-]?\d+)?)?$|^0x[0-9a-f]+$|\d+)/g
  sre = /(^[ ]*|[ ]*$)/g
  dre = /(^([\w ]+,?[\w ]+)?[\w ]+,?[\w ]+\d+:\d+(:\d+)?[\w ]?|^\d{1,4}[\/\-]\d{1,4}[\/\-]\d{1,4}|^\w+, \w+ \d+, \d{4})/
  hre = /^0x[0-9a-f]+$/i
  ore = /^0/
  i = (s) ->
    naturalSort.insensitive and ('' + s).toLowerCase() or '' + s

  # convert all to strings strip whitespace
  x = i(a).replace(sre, '') or ''
  y = i(b).replace(sre, '') or ''

  # chunk/tokenize
  xN = x.replace(re, '\u0000$1\u0000').replace(/\0$/, '').replace(/^\0/, '').split('\u0000')
  yN = y.replace(re, '\u0000$1\u0000').replace(/\0$/, '').replace(/^\0/, '').split('\u0000')

  # numeric, hex or date detection
  xD = parseInt(x.match(hre), 16) or (xN.length isnt 1 and x.match(dre) and Date.parse(x))
  yD = parseInt(y.match(hre), 16) or xD and y.match(dre) and Date.parse(y) or null
  oFxNcL = undefined
  oFyNcL = undefined

  # first try and sort Hex codes or Dates
  if yD
    return -1 if xD < yD
    return 1  if xD > yD

  # natural sorting through split numeric strings and default strings
  cLoc = 0
  numS = Math.max(xN.length, yN.length)

  while cLoc < numS
    # find floats not starting with '0', string or 0 if not defined (Clint Priest)
    oFxNcL = !(xN[cLoc] || '').match(ore) && parseFloat(xN[cLoc]) || xN[cLoc] || 0
    oFyNcL = !(yN[cLoc] || '').match(ore) && parseFloat(yN[cLoc]) || yN[cLoc] || 0

    # handle numeric vs string comparison - number < string - (Kyle Adams)
    if isNaN(oFxNcL) != isNaN(oFyNcL)
      return (if (isNaN(oFxNcL)) then 1 else -1)

    # Rely on string comparison of different types - i.e. '02' < 2 != '02' < '2'
    else if typeof oFxNcL != typeof oFyNcL
      oFxNcL += ''
      oFyNcL += ''

    return -1 if oFxNcL < oFyNcL
    return 1  if oFxNcL > oFyNcL

    cLoc++

  return 0
