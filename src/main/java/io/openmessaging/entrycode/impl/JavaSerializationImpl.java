package io.openmessaging.entrycode.impl;

import io.openmessaging.entrycode.entity.QueueData;
import io.openmessaging.entrycode.entity.QueueIndex;
import io.openmessaging.entrycode.interfaces.SerializationInterface;
import io.openmessaging.entrycode.util.Constant;

import java.io.*;

/**
 * Created by nbs on 2017/5/16.
 */
public class JavaSerializationImpl implements SerializationInterface {
    @Override
    public byte[] objectToByteArray(Object object) {
        byte[] bytes = null;
        byte[] resByte = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(object);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        if(object instanceof QueueIndex){
            resByte = new byte[Constant.QUEUE_INDEX_LENGTH];
        }else if(object instanceof QueueData){
            resByte = new byte[Constant.QUEUE_DATA_LENGTH];
        }else {
            resByte = new byte[bytes.length];
        }

        System.arraycopy(bytes,0,resByte,0,bytes.length);
        return resByte;

    }

    @Override
    public Object byteArrayToObject(byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
            ObjectInputStream ois = new ObjectInputStream (bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return obj;

    }
}
