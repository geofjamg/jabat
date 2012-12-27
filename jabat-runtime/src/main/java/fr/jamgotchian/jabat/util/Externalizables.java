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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class Externalizables {

    private Externalizables() {
    }
    
    public static Externalizable deserialize(byte[] data) throws IOException, ClassNotFoundException {
        if (data == null) {
            return null;
        }
        ObjectInputStream is = null;
        Externalizable externalizable = null;
        try {
            is = new ObjectInputStream(new ByteArrayInputStream(data));
            externalizable = (Externalizable) is.readObject();
        } finally {
            if (is != null) {
                is.close();
            }
        }
        return externalizable;
    }

    public static byte[] serialize(Externalizable externalizable) throws IOException {
        if (externalizable == null) {
            return null;
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream os = null;
        try {
            os = new ObjectOutputStream(bos);
            os.writeObject(externalizable);
        } finally {
            if (os != null) {
                os.close();
            }
        }
        return bos.toByteArray();
    }
    
}
