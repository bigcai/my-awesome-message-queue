package io.openmessaging.entrycode.interfaces;

/**
 * Created by nbs on 2017/5/16.
 */
public interface SerializationInterface {

    byte[] objectToByteArray(Object object);

    Object byteArrayToObject(byte[] bytes);
}
