package io.openmessaging.demobackup;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.demo.DefaultKeyValue;

public class DefaultBytesMessage implements BytesMessage {

    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties;
    private byte[] body;

    
    public DefaultBytesMessage(KeyValue headers, KeyValue properties, byte[] body) {
		super();
		this.headers = headers;
		this.properties = properties;
		this.body = body;
	}

	public DefaultBytesMessage(){

    }

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }
    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        if (properties == null) properties = new DefaultKeyValue();
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }
}
