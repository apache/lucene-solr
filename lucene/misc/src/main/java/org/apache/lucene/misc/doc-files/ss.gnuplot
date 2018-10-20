#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# ####################################################################
#
# Instructions for generating SVG renderings of the functions 
# used in SweetSpotSimilarity
#
# ####################################################################
#
#
set terminal svg size 600,400 dynamic enhanced fname 'arial'  fsize 11 butt solid 
set key inside left top vertical Right noreverse enhanced autotitles box linetype -1 linewidth 1.000
#
# #######  BASELINE TF
#
set output 'ss.baselineTf.svg'
set title "SweetSpotSimilarity.baselineTf(x)"
set xrange [0:20]
set yrange [-1:8]
btf(base,min,x)=(x <= min) ? base : sqrt(x+(base**2)-min)
#
plot btf(0,0,x) ti "all defaults", \
     btf(1.5,0,x) ti "base=1.5", \
     btf(0,5,x) ti "min=5", \
     btf(1.5,5,x) ti "min=5, base=1.5"
#
# #######  HYPERBOLIC TF
#
set output 'ss.hyperbolicTf.svg'
set title "SweetSpotSimilarity.hyperbolcTf(x)"
set xrange [0:20]
set yrange [0:3]
htf(min,max,base,xoffset,x)=min+(max-min)/2*(((base**(x-xoffset)-base**-(x-xoffset))/(base**(x-xoffset)+base**-(x-xoffset)))+1)
#
plot htf(0,2,1.3,10,x) ti "all defaults", \
     htf(0,2,1.3,5,x) ti "xoffset=5", \
     htf(0,2,1.2,10,x) ti "base=1.2", \
     htf(0,1.5,1.3,10,x) ti "max=1.5"
#
# #######  LENGTH NORM
#
set key inside right top
set output 'ss.computeLengthNorm.svg'
set title "SweetSpotSimilarity.computeLengthNorm(t)"
set xrange [0:20]
set yrange [0:1.2]
set mxtics 5 
cln(min,max,steepness,x)=1/sqrt( steepness * (abs(x-min) + abs(x-max) - (max-min)) + 1 )
#
plot cln(1,1,0.5,x) ti "all defaults", \
     cln(1,1,0.2,x) ti "steepness=0.2", \
     cln(1,6,0.2,x) ti "max=6, steepness=0.2", \
     cln(3,5,0.5,x) ti "min=3, max=5"
