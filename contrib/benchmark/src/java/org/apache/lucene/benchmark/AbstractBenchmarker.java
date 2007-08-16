package org.apache.lucene.benchmark;

import java.io.File;
import java.io.IOException;
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
 *
 * @deprecated Use the Task based benchmarker
 **/
public abstract class AbstractBenchmarker implements Benchmarker
{
    /**
     * Delete files and directories, even if non-empty.
     *
     * @param dir file or directory
     * @return true on success, false if no or part of files have been deleted
     * @throws java.io.IOException
     */
    public static boolean fullyDelete(File dir) throws IOException
    {
        if (dir == null || !dir.exists()) return false;
        File contents[] = dir.listFiles();
        if (contents != null)
        {
            for (int i = 0; i < contents.length; i++)
            {
                if (contents[i].isFile())
                {
                    if (!contents[i].delete())
                    {
                        return false;
                    }
                }
                else
                {
                    if (!fullyDelete(contents[i]))
                    {
                        return false;
                    }
                }
            }
        }
        return dir.delete();
    }
}
