package io.openmessaging.entrycode.impl;

import io.openmessaging.KeyValue;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.DefaultKeyValue;
import io.openmessaging.entrycode.entity.QueueData;
import io.openmessaging.entrycode.entity.QueueIndex;
import io.openmessaging.entrycode.interfaces.SerializationInterface;
import io.openmessaging.entrycode.util.ByteUtil;
import io.openmessaging.entrycode.util.Constant;

import java.util.Set;

/**
 * Created by linl on 2017/6/1.
 */
public class MySerialization implements SerializationInterface {
    private static int TYPE_INT = 1;
    private static int TYPE_LONG = 2;
    private static int TYPE_dOUBLE = 3;
    private static int TYPE_STRING = 4;

    JavaSerializationImpl javaSerialization = new JavaSerializationImpl();

    @Override
    public byte[] objectToByteArray(Object object) {
        if (object instanceof QueueIndex) {//反射效率低，直接每种方法写特定的序列化方法
            return QueueIndexSerialization((QueueIndex) object);
        } else if (object instanceof QueueData) {
            return QueueDataSerialization((QueueData) object);
        } else if (object instanceof DefaultBytesMessage) {
            return MessageSerialization((DefaultBytesMessage) object);
            //return javaSerialization.objectToByteArray(object);
        } else {
        	return new byte[0];
        }
    }

    @Override
    public Object byteArrayToObject(byte[] bytes) {
        if (bytes.length == Constant.QUEUE_INDEX_LENGTH) {
            return byteToQueueIndex(bytes);
        } else if (bytes.length == Constant.QUEUE_DATA_LENGTH) {
            return byteToQueueData(bytes);
        }
        //return javaSerialization.byteArrayToObject(bytes);
        return byteToMessage(bytes);
    }

    private byte[] QueueIndexSerialization(QueueIndex queueIndex) {
        int position = 0;
        byte[] bytes = new byte[Constant.QUEUE_INDEX_LENGTH];
        byte[] queueName = queueIndex.getQueueName().getBytes();
        int stringLength = queueName.length;//最开头的4个字节用来保存字符串有多长，否则非定长无法反序列化。
        System.arraycopy(ByteUtil.toByte(stringLength), 0, bytes, 0, 4);
        System.arraycopy(queueName, 0, bytes, (position += 4), stringLength);
        System.arraycopy(ByteUtil.toByte(queueIndex.getPageId()), 0, bytes, (position += stringLength), 4);
        System.arraycopy(ByteUtil.toByte(queueIndex.getType()), 0, bytes, (position += 4), 4);
        System.arraycopy(ByteUtil.toByte(queueIndex.getCurrentReadPosition()), 0, bytes, (position += 4), 4);
        System.arraycopy(ByteUtil.toByte(queueIndex.getCurrentWritePosition()), 0, bytes, (position += 4), 4);
        System.arraycopy(ByteUtil.toByte(queueIndex.getMyPosition()), 0, bytes, (position += 4), 4);
        System.arraycopy(ByteUtil.toByte(queueIndex.getStartPosition()), 0, bytes, (position += 4), 4);
        System.arraycopy(ByteUtil.toByte(queueIndex.getEndPosition()), 0, bytes, (position += 4), 4);
        return bytes;
    }

    private QueueIndex byteToQueueIndex(byte[] bytes) {
        int stringLength = ByteUtil.byteArrayToInt(new byte[]{bytes[0], bytes[1], bytes[2], bytes[3]});
        int position = 4;
        QueueIndex queueIndex = new QueueIndex();
        byte[] b = new byte[stringLength];
        byte[] c = new byte[4];
        System.arraycopy(bytes, position, b, 0, stringLength);
        queueIndex.setQueueName(new String(b));
        System.arraycopy(bytes, (position += stringLength), c, 0, 4);
        queueIndex.setPageId(ByteUtil.byteArrayToInt(c));
        System.arraycopy(bytes, (position += 4), c, 0, 4);
        queueIndex.setType(ByteUtil.byteArrayToInt(c));
        System.arraycopy(bytes, (position += 4), c, 0, 4);
        queueIndex.setCurrentReadPosition(ByteUtil.byteArrayToInt(c));
        System.arraycopy(bytes, (position += 4), c, 0, 4);
        queueIndex.setCurrentWritePosition(ByteUtil.byteArrayToInt(c));
        System.arraycopy(bytes, (position += 4), c, 0, 4);
        queueIndex.setMyPosition(ByteUtil.byteArrayToInt(c));
        System.arraycopy(bytes, (position += 4), c, 0, 4);
        queueIndex.setStartPosition(ByteUtil.byteArrayToInt(c));
        System.arraycopy(bytes, (position += 4), c, 0, 4);
        queueIndex.setEndPosition(ByteUtil.byteArrayToInt(c));
        return queueIndex;
    }

    private byte[] QueueDataSerialization(QueueData queueData) {
        int position = 0;
        byte[] bytes = new byte[Constant.QUEUE_DATA_LENGTH];
        byte[] queueName = queueData.getQueueName().getBytes();
        int stringLength = queueName.length;
        System.arraycopy(ByteUtil.toByte(stringLength), 0, bytes, 0, 4);
        System.arraycopy(queueName, 0, bytes, (position += 4), stringLength);
        System.arraycopy(ByteUtil.toByte(queueData.getPageId()), 0, bytes, (position += stringLength), 4);
        System.arraycopy(ByteUtil.toByte(queueData.getStartPosition()), 0, bytes, (position += 4), 4);
        System.arraycopy(ByteUtil.toByte(queueData.getEndPosition()), 0, bytes, (position += 4), 4);
        return bytes;
    }

    private QueueData byteToQueueData(byte[] bytes) {
        int stringLength = ByteUtil.byteArrayToInt(new byte[]{bytes[0], bytes[1], bytes[2], bytes[3]});
        int position = 4;
        QueueData queueData = new QueueData();
        byte[] b = new byte[stringLength];
        byte[] c = new byte[4];
        System.arraycopy(bytes, position, b, 0, stringLength);
        queueData.setQueueName(new String(b));
        System.arraycopy(bytes, (position += stringLength), c, 0, 4);
        queueData.setPageId(ByteUtil.byteArrayToInt(c));
        System.arraycopy(bytes, (position += 4), c, 0, 4);
        queueData.setStartPosition(ByteUtil.byteArrayToInt(c));
        System.arraycopy(bytes, (position += 4), c, 0, 4);
        queueData.setEndPosition(ByteUtil.byteArrayToInt(c));
        return queueData;
    }
    
    // Message序列化
    private byte[] MessageSerialization(DefaultBytesMessage message) {
        int position = 0;
        byte[] msgHeadersBytes = KeyValueSerialization( (DefaultKeyValue)message.headers() );
        byte[] propertiesBytes = KeyValueSerialization( (DefaultKeyValue)message.properties() );
        byte[] bodyBytes = message.getBody();
        int headerLength = msgHeadersBytes.length;
        int propertiesLength = propertiesBytes.length;
        int bodyLength = bodyBytes.length;
        byte[] bytes = new byte[ 4 + headerLength + 4 + propertiesLength + 4+ bodyLength];
        
        System.arraycopy(ByteUtil.toByte(headerLength), 0, bytes, position, 4);
        System.arraycopy(msgHeadersBytes, 0, bytes, position += 4, headerLength);
        
        System.arraycopy(ByteUtil.toByte(propertiesLength), 0, bytes, (position += headerLength), 4);
        System.arraycopy(propertiesBytes, 0, bytes, (position += 4), propertiesLength);
        
        System.arraycopy(ByteUtil.toByte(bodyLength), 0, bytes, (position += propertiesLength), 4);
        System.arraycopy(bodyBytes, 0, bytes, (position += 4), bodyLength);
        
        return bytes;
    }
    // DefaultKeyValue序列化
    private byte[] KeyValueSerialization(DefaultKeyValue keyValue) {

        Set<String> keys =  keyValue.keySet();
        byte[][] map = new byte[keys.size()][];
        int iter = 0;
        int position = 0;
        for ( String key : keys ) {
            Object obj = keyValue.get( key );
            if( obj instanceof Integer ) {
                map[iter++] = keyValueToByte( key,  obj,  TYPE_INT );
            } else if( obj instanceof Long ) {
                map[iter++] = keyValueToByte( key,  obj,  TYPE_LONG );
            } else if( obj instanceof Double ) {
                map[iter++] = keyValueToByte( key,  obj,  TYPE_dOUBLE );
            } else if( obj instanceof String ) {
                map[iter++] = keyValueToByte( key,  obj,  TYPE_STRING );
            }
        }
        int mapLength = 0;
        for (byte[] bytes : map) {
            mapLength += bytes.length;
        }
        byte[] mapBytes = new byte[mapLength];
        for (byte[] bytes : map) {
            int bytesLength = bytes.length;
            System.arraycopy(bytes, 0, mapBytes, position, bytesLength);
            position += bytesLength;
        }
        return mapBytes;
    }

    private byte[] keyValueToByte(String key, Object obj, int type ) {
        byte[] bytes = null;
        if( type == TYPE_INT ) {
            // 4个字节类型，4个字节key长度，n个字节的字符串key，n个字节的value
            byte[] typeBytes = ByteUtil.toByte(type);
            byte[] keyLengthBytes = ByteUtil.toByte(key.length());
            byte[] keyBytes = key.getBytes();
            byte[] valueBytes = ByteUtil.toByte((int)obj);
            int byteslength = typeBytes.length + keyLengthBytes.length + keyBytes.length + valueBytes.length ;
            bytes = new byte[byteslength];
            int position = 0;
            System.arraycopy(typeBytes, 0, bytes, position, typeBytes.length);
            System.arraycopy(keyLengthBytes, 0, bytes, (position += typeBytes.length), keyLengthBytes.length);
            System.arraycopy(keyBytes, 0, bytes, (position += keyLengthBytes.length), keyBytes.length);
            System.arraycopy(valueBytes, 0, bytes, (position += keyBytes.length), valueBytes.length);

        } else if( type == TYPE_LONG ) {
            // 4个字节类型，4个字节key长度，n个字节的字符串key，n个字节的value
            byte[] typeBytes = ByteUtil.toByte(type);
            byte[] keyLengthBytes = ByteUtil.toByte(key.length());
            byte[] keyBytes = key.getBytes();
            byte[] valueBytes = ByteUtil.toByte((long)obj);
            int byteslength = typeBytes.length + keyLengthBytes.length + keyBytes.length + valueBytes.length ;
            bytes = new byte[byteslength];
            int position = 0;
            System.arraycopy(typeBytes, 0, bytes, position, typeBytes.length);
            System.arraycopy(keyLengthBytes, 0, bytes, (position += typeBytes.length), keyLengthBytes.length);
            System.arraycopy(keyBytes, 0, bytes, (position += keyLengthBytes.length), keyBytes.length);
            System.arraycopy(valueBytes, 0, bytes, (position += keyBytes.length), valueBytes.length);

        } else if( type == TYPE_dOUBLE ) {
            // 4个字节类型，4个字节key长度，n个字节的字符串key，n个字节的value
            byte[] typeBytes = ByteUtil.toByte(type);
            byte[] keyLengthBytes = ByteUtil.toByte(key.length());
            byte[] keyBytes = key.getBytes();
            byte[] valueBytes = ByteUtil.toByte((long)obj);
            int byteslength = typeBytes.length + keyLengthBytes.length + keyBytes.length + valueBytes.length ;
            bytes = new byte[byteslength];
            int position = 0;
            System.arraycopy(typeBytes, 0, bytes, position, typeBytes.length);
            System.arraycopy(keyLengthBytes, 0, bytes, (position += typeBytes.length), keyLengthBytes.length);
            System.arraycopy(keyBytes, 0, bytes, (position += keyLengthBytes.length), keyBytes.length);
            System.arraycopy(valueBytes, 0, bytes, (position += keyBytes.length), valueBytes.length);

        } else if( type == TYPE_STRING ) {
            // 4个字节类型，4个字节key长度，n个字节的字符串key，4个字节长度，n个字节的value
            byte[] typeBytes = ByteUtil.toByte(type);
            byte[] keyLengthBytes = ByteUtil.toByte(key.length());
            byte[] keyBytes = key.getBytes();
            byte[] valueLengthBytes = ByteUtil.toByte(((String)obj).length());
            byte[] valueBytes = ((String)obj).getBytes();
            int byteslength = typeBytes.length + keyLengthBytes.length + keyBytes.length + valueLengthBytes.length + valueBytes.length ;
            bytes = new byte[byteslength];
            int position = 0;
            System.arraycopy(typeBytes, 0, bytes, position, typeBytes.length);
            System.arraycopy(keyLengthBytes, 0, bytes, (position += typeBytes.length), keyLengthBytes.length);
            System.arraycopy(keyBytes, 0, bytes, (position += keyLengthBytes.length), keyBytes.length);
            System.arraycopy(valueLengthBytes, 0, bytes, (position += keyBytes.length), valueLengthBytes.length);
            System.arraycopy(valueBytes, 0, bytes, (position += valueLengthBytes.length), valueBytes.length);
        }
        return bytes;
    }

    // Message反序列化
    private DefaultBytesMessage byteToMessage(byte[] bytes) {
        // 4个字节的长度+ n个字节的headers + 4个字节的长度+ n个字节的properties + 4个字节的长度+ n个字节的body
        int position = 0;

    	byte[] headersLengthBytes = new byte[4];
    	System.arraycopy(bytes, position, headersLengthBytes, 0, 4);
        position +=4;
    	int headersLength = ByteUtil.byteArrayToInt(headersLengthBytes);
    	byte[] headersBytes = new byte[headersLength];
        System.arraycopy(bytes, position, headersBytes, 0, headersLength);
        KeyValue headers = byteArrayToKeyValue(headersBytes);
        position +=headersLength;
        
        byte[] propertiesLengthBytes = new byte[4];
    	System.arraycopy(bytes, position, propertiesLengthBytes, 0, 4);
        position +=4;
    	int propertiesLength = ByteUtil.byteArrayToInt(propertiesLengthBytes);
    	byte[] propertiesBytes = new byte[propertiesLength];
        System.arraycopy(bytes, position, propertiesBytes, 0, propertiesLength);
        KeyValue properties = byteArrayToKeyValue(propertiesBytes);
        position +=propertiesLength;

        byte[] bodyLengthBytes = new byte[4];
        System.arraycopy(bytes, position, bodyLengthBytes, 0, 4);
        position +=4;
        int bodyLength = ByteUtil.byteArrayToInt(bodyLengthBytes);
        byte[] body = new byte[bodyLength];
    	System.arraycopy(bytes, position, body, 0, bodyLength);
        DefaultBytesMessage myMessage = new DefaultBytesMessage( headers, properties, body);
        return myMessage;
    }

    private KeyValue byteArrayToKeyValue(byte[] bytes) {
        KeyValue keyValue = new DefaultKeyValue();
        int position = 0;
        int bytesLength = bytes.length;
        while( position < bytesLength ) {
            byte[] typeBytes = new byte[4];
            System.arraycopy(bytes, position, typeBytes, 0, 4);
            position +=4;
            int type = ByteUtil.byteArrayToInt(typeBytes);
            if( type == TYPE_INT ) {
                // 4个字节类型，4个字节key长度，n个字节的字符串key，n个字节的value
                byte[] keyLengthBytes = new byte[4];
                System.arraycopy(bytes, position, keyLengthBytes, 0, 4);
                position +=4;
                int keyLength = ByteUtil.byteArrayToInt(keyLengthBytes);

                byte[] keyBytes = new byte[keyLength];
                System.arraycopy(bytes, position, keyBytes, 0, keyLength);
                position +=keyLength;
                String key = new String( keyBytes );

                byte[] valueBytes = new byte[4];
                System.arraycopy(bytes, position, valueBytes, 0, 4);
                position +=4;
                int value = ByteUtil.byteArrayToInt(valueBytes);

                keyValue.put(key, value);

            } else if( type == TYPE_LONG ) {
                // 4个字节类型，4个字节key长度，n个字节的字符串key，n个字节的value
                byte[] keyLengthBytes = new byte[4];
                System.arraycopy(bytes, position, keyLengthBytes, 0, 4);
                position +=4;
                int keyLength = ByteUtil.byteArrayToInt(keyLengthBytes);

                byte[] keyBytes = new byte[keyLength];
                System.arraycopy(bytes, position, keyBytes, 0, keyLength);
                position +=keyLength;
                String key = new String( keyBytes );

                byte[] valueBytes = new byte[8];
                System.arraycopy(bytes, position, valueBytes, 0, 8);
                position +=8;
                long value = ByteUtil.byteArrayToLong(valueBytes);

                keyValue.put(key, value);

            } else if( type == TYPE_dOUBLE ) {
                // 4个字节类型，4个字节key长度，n个字节的字符串key，n个字节的value
                byte[] keyLengthBytes = new byte[4];
                System.arraycopy(bytes, position, keyLengthBytes, 0, 4);
                position +=4;
                int keyLength = ByteUtil.byteArrayToInt(keyLengthBytes);

                byte[] keyBytes = new byte[keyLength];
                System.arraycopy(bytes, position, keyBytes, 0, keyLength);
                position +=keyLength;
                String key = new String( keyBytes );

                byte[] valueBytes = new byte[8];
                System.arraycopy(bytes, position, valueBytes, 0, 8);
                position +=8;
                long value = ByteUtil.byteArrayToLong(valueBytes);

                keyValue.put(key, (double) value);
            } else if( type == TYPE_STRING ) {
                // 4个字节类型，4个字节key长度，n个字节的字符串key，4个字节长度，n个字节的value
                byte[] keyLengthBytes = new byte[4];
                System.arraycopy(bytes, position, keyLengthBytes, 0, 4);
                position +=4;
                int keyLength = ByteUtil.byteArrayToInt(keyLengthBytes);

                byte[] keyBytes = new byte[keyLength];
                System.arraycopy(bytes, position, keyBytes, 0, keyLength);
                position +=keyLength;
                String key = new String( keyBytes );

                byte[] valueLengthBytes = new byte[4];
                System.arraycopy(bytes, position, valueLengthBytes, 0, 4);
                position +=4;
                int valueLength = ByteUtil.byteArrayToInt(valueLengthBytes);

                byte[] valueBytes = new byte[valueLength];
                System.arraycopy(bytes, position, valueBytes, 0, valueLength);
                position +=valueLength;
                String value = new String( valueBytes );

                keyValue.put(key, value);
            }
        }
        return keyValue;
    }



    public static void main(String[] args) {
        DefaultBytesMessage myMessage = new DefaultBytesMessage();
        myMessage.headers().put("type","test");
        myMessage.properties().put("path","D:/");
        myMessage.setBody(new byte[]{1,2,3});

        System.out.println(myMessage.headers().getString( "type" ) + " type");
        System.out.println(myMessage.properties().getString( "path" ) + " path");
        for (byte b : myMessage.getBody()) {
            System.out.println(b);
        }

        MySerialization s = new MySerialization();
        byte[] msgBytes = s.MessageSerialization( myMessage );
        DefaultBytesMessage myMessageNew = s.byteToMessage( msgBytes );

        System.out.println(myMessageNew.headers().getString( "type" ) + " type");
        System.out.println(myMessageNew.properties().getString( "path" ) + " path");
        for (byte b : myMessageNew.getBody()) {
            System.out.println(b);
        }

    }
}
