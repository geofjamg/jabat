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
import fr.jamgotchian.jabat.util.MethodUtil;
import static fr.jamgotchian.jabat.util.MethodUtil.*;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import javax.batch.annotation.ItemWriter;
import javax.batch.annotation.WriteItems;

/**
 * @WriteItems void <method-name> (List<item-type> items) throws Exception
 * 
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ItemWriterAnnotatedClass extends ResourceAnnotatedClass {
  
    private final Method writeItemsMethod;

    public ItemWriterAnnotatedClass(Class<?> clazz) {
        super(clazz, ItemWriter.class);
        writeItemsMethod = findAnnotatedMethod(clazz, WriteItems.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return m.getReturnType() == Void.TYPE
                        && m.getParameterTypes().length == 1
                        && List.class.isAssignableFrom(m.getParameterTypes()[0])
                        && ((ParameterizedType) m.getGenericParameterTypes()[0]).getActualTypeArguments().length == 1
                        && MethodUtil.throwsOneException(m, Exception.class);
            }
        });
        writeItemsMethod.setAccessible(true);
    }

    public Method getWriteItemsMethod() {
        return writeItemsMethod;
    }
    
    public Type getItemType() {
        return ((ParameterizedType) writeItemsMethod.getGenericParameterTypes()[0])
                .getActualTypeArguments()[0];        
    }

}
