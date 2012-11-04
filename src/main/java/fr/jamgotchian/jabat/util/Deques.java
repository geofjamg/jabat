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
package fr.jamgotchian.jabat.util;

import java.util.Deque;
import java.util.Iterator;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class Deques {

    private Deques() {
    }

    public static <E> E getSecond(Deque<E> deque) {
        if (deque.size() < 2) {
            throw new IllegalArgumentException("Cannot the second element of a size " 
                    + deque.size() + " deque");
        }
        Iterator<E> it = deque.iterator();
        it.next();
        return it.next();
    }
}
