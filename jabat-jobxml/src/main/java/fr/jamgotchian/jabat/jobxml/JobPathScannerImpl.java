/*
 * Copyright 2012 Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.jamgotchian.jabat.jobxml;

import com.google.common.base.Predicate;
import fr.jamgotchian.jabat.jobxml.util.JobXmlException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobPathScannerImpl implements JobPathScanner {

    private final JobPath path;

    public JobPathScannerImpl(JobPath path) {
        this.path = path;
    }

    public JobPathScannerImpl() {
        this(new JobPathImpl());
    }

    @Override
    public InputStream scan(Predicate<InputStream> predicate) {
        if (predicate == null) {
            throw new IllegalArgumentException("predicate is null");
        }
        try {
            for (File directory : path.getDirectories()) {
                File[] files = directory.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File pathname) {
                        return pathname.getPath().endsWith("-job.xml");
                    }
                });
                if (files != null) {
                    for (File file : files) {
                        InputStream is = new FileInputStream(file);
                        try {
                            if (predicate.apply(is)) {
                                return new FileInputStream(file);
                            }
                        } finally {
                            is.close();
                        }
                    }
                }
            }
            return null;
        } catch (IOException e) {
            throw new JobXmlException(e);
        }
    }
}
