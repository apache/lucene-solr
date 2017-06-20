from subprocess import call
import os

PAIRWISE_THRESHOLD = 1.e-1
FEATURE_DIFF_THRESHOLD = 1.e-6

class LibSvmFormatter:
    def processQueryDocFeatureVector(self,docClickInfo,trainingFile):
        '''Expects as input a sorted by queries list or generator that provides the context 
        for each query in a tuple composed of: (query , docId , relevance , source , featureVector).
        The list of documents that are part of the same query will generate comparisons
        against each other for training. '''
        with open(trainingFile,"w") as output:
            self.featureNameToId  = {}
            self.featureIdToName = {}
            self.curFeatIndex = 1;
            curListOfFv = []
            curQueryAndSource = ""
            for query,docId,relevance,source,featureVector in docClickInfo:
                if curQueryAndSource != query + source:
                    #Time to flush out all the pairs
                    _writeRankSVMPairs(curListOfFv,output);
                    curListOfFv = []
                    curQueryAndSource = query + source
                curListOfFv.append((relevance,self._makeFeaturesMap(featureVector)))
            _writeRankSVMPairs(curListOfFv,output); #This catches the last list of comparisons

    def _makeFeaturesMap(self,featureVector):
        '''expects a list of strings with "feature name":"feature value" pairs. Outputs a map of map[key] = value.
        Where key is now an integer. libSVM requires the key to be an integer but not all libraries have
        this requirement.'''
        features = {}
        for keyValuePairStr in featureVector:
            featName,featValue = keyValuePairStr.split("=");
            features[self._getFeatureId(featName)] = float(featValue);
        return features

    def _getFeatureId(self,key):
        if key not in self.featureNameToId:
                self.featureNameToId[key] = self.curFeatIndex;
                self.featureIdToName[self.curFeatIndex] = key;
                self.curFeatIndex += 1;
        return self.featureNameToId[key];

    def convertLibSvmModelToLtrModel(self,libSvmModelLocation,outputFile,modelName,featureStoreName):
        with open(libSvmModelLocation, 'r') as inFile:
            with open(outputFile,'w') as convertedOutFile:
                # TODO: use json module instead of direct write
                convertedOutFile.write('{\n\t"class":"org.apache.solr.ltr.model.LinearModel",\n')
                convertedOutFile.write('\t"store": "' + str(featureStoreName) + '",\n')
                convertedOutFile.write('\t"name": "' + str(modelName) + '",\n')
                convertedOutFile.write('\t"features": [\n')
                isFirst = True;
                for featKey in self.featureNameToId.keys():
                    convertedOutFile.write('\t\t{ "name":"' + featKey  + '"}' if isFirst else ',\n\t\t{ "name":"' + featKey  + '"}' );
                    isFirst = False;
                convertedOutFile.write("\n\t],\n");
                convertedOutFile.write('\t"params": {\n\t\t"weights": {\n');

                startReading = False
                isFirst = True
                counter = 1
                for line in inFile:
                    if startReading:
                        newParamVal = float(line.strip())
                        if not isFirst:
                            convertedOutFile.write(',\n\t\t\t"' + self.featureIdToName[counter] + '":' + str(newParamVal))
                        else:
                            convertedOutFile.write('\t\t\t"' + self.featureIdToName[counter] + '":' + str(newParamVal))
                            isFirst = False
                        counter += 1
                    elif line.strip() == 'w':
                        startReading = True
                convertedOutFile.write('\n\t\t}\n\t}\n}')

def _writeRankSVMPairs(listOfFeatures,output):
    '''Given a list of (relevance, {Features Map}) where the list represents
    a set of documents to be compared, this calculates all pairs and
    writes the Feature Vectors in a format compatible with libSVM.
    Ex: listOfFeatures = [
      #(relevance, {feature1:value, featureN:value})
      (4, {1:0.9, 2:0.9, 3:0.1})
      (3, {1:0.7, 2:0.9, 3:0.2})
      (1, {1:0.1, 2:0.9, 6:0.1})
    ]    
    '''
    for d1 in range(0,len(listOfFeatures)):
        for d2 in range(d1+1,len(listOfFeatures)):
            doc1,doc2 = listOfFeatures[d1], listOfFeatures[d2]
            fv1,fv2 = doc1[1],doc2[1]
            d1Relevance, d2Relevance = float(doc1[0]),float(doc2[0])
            if  d1Relevance - d2Relevance > PAIRWISE_THRESHOLD:#d1Relevance > d2Relevance
                outputLibSvmLine("+1",subtractFvMap(fv1,fv2),output);
                outputLibSvmLine("-1",subtractFvMap(fv2,fv1),output);
            elif d1Relevance - d2Relevance < -PAIRWISE_THRESHOLD: #d1Relevance < d2Relevance:
                outputLibSvmLine("+1",subtractFvMap(fv2,fv1),output);
                outputLibSvmLine("-1",subtractFvMap(fv1,fv2),output);
            else: #Must be approximately equal relevance, in which case this is a useless signal and we should skip
                continue;

def subtractFvMap(fv1,fv2):
    '''returns the fv from fv1 - fv2'''
    retFv = fv1.copy();
    for featInd in fv2.keys():
        subVal = 0.0;
        if featInd in fv1:
            subVal = fv1[featInd] - fv2[featInd]
        else:
            subVal = -fv2[featInd]
        if abs(subVal) > FEATURE_DIFF_THRESHOLD: #This ensures everything is in sparse format, and removes useless signals
            retFv[featInd] = subVal;
        else:
            retFv.pop(featInd, None)
    return retFv;

def outputLibSvmLine(sign,fvMap,outputFile):
    outputFile.write(sign)
    for feat in fvMap.keys():
        outputFile.write(" " + str(feat) + ":" + str(fvMap[feat]));
    outputFile.write("\n")

def trainLibSvm(libraryLocation,libraryOptions,trainingFileName,trainedModelFileName):
    if os.path.isfile(libraryLocation):
        call([libraryLocation, libraryOptions, trainingFileName, trainedModelFileName])
    else:
        raise Exception("NO LIBRARY FOUND: " + libraryLocation);
