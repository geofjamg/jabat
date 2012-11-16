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
package fr.jamgotchian.jabat.artifact.annotated;

import com.google.common.base.Predicate;
import static fr.jamgotchian.jabat.util.MethodUtil.*;
import java.lang.reflect.Method;
import javax.batch.annotation.ProcessItem;

/**
 * @ProcessItem <output-item-type> <method-name>(<item-type> item) throws Exception
 * 
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ItemProcessorAnnotatedClass extends AnnotatedClass {

    private final Method processItemMethod;
    
    public ItemProcessorAnnotatedClass(Class<?> clazz) {
        super(clazz);
        processItemMethod = findAnnotatedMethod(clazz, ProcessItem.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return m.getReturnType() != Void.TYPE
                        && hasOneParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        processItemMethod.setAccessible(true);
    }

    public Class<?> getItemType() {
        return processItemMethod.getParameterTypes()[0];
    }

    public Method getProcessItemMethod() {
        return processItemMethod;
    }
       
}
