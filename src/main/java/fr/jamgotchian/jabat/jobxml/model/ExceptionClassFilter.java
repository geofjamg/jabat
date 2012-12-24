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
package fr.jamgotchian.jabat.jobxml.model;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ExceptionClassFilter {

    private final Set<Class<?>> includedClasses;

    private final Set<Class<?>> excludedClasses;

    ExceptionClassFilter(Set<Class<?>> includedClasses, Set<Class<?>> excludedClasses) {
        this.includedClasses = includedClasses;
        this.excludedClasses = excludedClasses;
    }

    public Set<Class<?>> getIncludedClasses() {
        return includedClasses;
    }

    public Set<Class<?>> getExcludedClasses() {
        return excludedClasses;
    }

}
