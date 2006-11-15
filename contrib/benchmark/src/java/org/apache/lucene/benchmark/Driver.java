package org.apache.lucene.benchmark;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.digester.Digester;
import org.apache.lucene.benchmark.standard.StandardBenchmarker;
import org.apache.lucene.benchmark.stats.TestData;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 *  Sets up the
 *
 **/
public class Driver
{
    private File workingDir;
    private Benchmarker benchmarker;
    private BenchmarkOptions options;

    public Driver()
    {
    }

    public Driver(Benchmarker benchmarker, BenchmarkOptions options)
    {
        this.benchmarker = benchmarker;
        this.options = options;
    }


    /**
     * Creates a Driver using Digester
     * @param inputSource
     */
    public Driver(File workingDir, InputSource inputSource) throws IOException, SAXException
    {
        Digester digester = new Digester();
        digester.setValidating(false);
        digester.addObjectCreate("benchmark/benchmarker", "class", StandardBenchmarker.class);
        digester.addSetProperties("benchmark/benchmarker");
        digester.addSetNext("benchmark/benchmarker", "setBenchmarker");
        digester.addObjectCreate("benchmark/options", "class", BenchmarkOptions.class);
        digester.addSetProperties("benchmark/options");
        digester.addSetNext("benchmark/options", "setOptions");
        digester.push(this);
        digester.parse(inputSource);
        this.workingDir = workingDir;
    }

    private void run() throws Exception
    {
        TestData [] data = benchmarker.benchmark(workingDir, options);
        //Print out summary:
        /*System.out.println("Test Data:");
        for (int i = 0; i < data.length; i++)
        {
            TestData testData = data[i];
            System.out.println("---------------");
            System.out.println(testData.showRunData(testData.getId()));
            System.out.println("---------------");
        }*/

    }

    public Benchmarker getBenchmarker()
    {
        return benchmarker;
    }

    public void setBenchmarker(Benchmarker benchmarker)
    {
        this.benchmarker = benchmarker;
    }

    public BenchmarkOptions getOptions()
    {
        return options;
    }

    public void setOptions(BenchmarkOptions options)
    {
        this.options = options;
    }

    public File getWorkingDir()
    {
        return workingDir;
    }

    public void setWorkingDir(File workingDir)
    {
        this.workingDir = workingDir;
    }

    public static void main(String[] args)
    {

        if (args.length != 2)
        {
            printHelp(args);
            System.exit(0);
        }
        File workingDir = new File(args[0]);
        File configFile = new File(args[1]);
        if (configFile.exists())
        {
            //Setup
            try
            {
                Driver driver = new Driver(workingDir, new InputSource(new FileReader(configFile)));
                driver.run();
            }
            catch (Exception e)
            {
                e.printStackTrace(System.err);
            }
        }

    }


    private static void printHelp(String[] args)
    {
        System.out.println("Usage: java -cp [...] " + Driver.class.getName() + "<working dir> <config-file>");
    }
}
