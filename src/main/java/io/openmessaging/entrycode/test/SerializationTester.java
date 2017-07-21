
package io.openmessaging.entrycode.test;

import io.openmessaging.entrycode.entity.QueueData;
import io.openmessaging.entrycode.entity.QueueIndex;
import io.openmessaging.entrycode.impl.JavaSerializationImpl;
import io.openmessaging.entrycode.impl.MySerialization;
import io.openmessaging.entrycode.interfaces.SerializationInterface;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by nbs on 2017/5/28.
 */
public class SerializationTester {

    public static void main(String[] args) {
        SerializationInterface serializationInterface = new JavaSerializationImpl();
        Map map = new HashMap();
        map.put("1","v1");
        byte[] b = serializationInterface.objectToByteArray(map);
        Map m = (Map)serializationInterface.byteArrayToObject(b);
        assert m.get("1").equals("v1");

        QueueIndex b1 = new QueueIndex();
        b1.setQueueName("11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111");
        b1.setCurrentReadPosition(1);
        b1.setCurrentWritePosition(2);
        b1.setEndPosition(1);
        b1.setMyPosition(1);
        b1.setPageId(1);
    }

}
