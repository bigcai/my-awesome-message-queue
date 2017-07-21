package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.entrycode.impl.JavaSerializationImpl;
import io.openmessaging.entrycode.interfaces.SerializationInterface;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;

public class DefaultBytesMessage extends io.openmessaging.demobackup.DefaultBytesMessage implements Serializable {
	private static SerializationInterface serializationInterface = new JavaSerializationImpl();

	public DefaultBytesMessage(){
		super();
	}
	public DefaultBytesMessage(byte[] body) {
		super(body);
	}

	public DefaultBytesMessage(KeyValue headers, KeyValue properties, byte[] body) {
		super( headers, properties, body );
	}

	public byte[] toBytes() {
		byte[] bytes =  serializationInterface.objectToByteArray(this);
		return bytes;
	}
	public static DefaultBytesMessage toObject(byte[] dest) {
		return (DefaultBytesMessage) serializationInterface.byteArrayToObject(dest);
	}

	@Override
	public boolean equals(Object obj){
		if (this == obj)      //传入的对象就是它自己，如s.equals(s)；肯定是相等的；
			return true;
		if (obj == null)     //如果传入的对象是空，肯定不相等
			return false;
		if (getClass() != obj.getClass())  //如果不是同一个类型的，如Studnet类和Animal类，
			//也不用比较了，肯定是不相等的
			return false;
		Message o = (Message)obj;
		Set<String> header;
		if(o.headers() != null){
			header = o.headers().keySet();
			for(String i : header){
				if(!(o.headers().getString(i).equals(this.headers().getString(i)) )) return  false;
			}
		}
		Set<String> pro;
		if(o.properties() != null){
			pro = o.properties().keySet();
			for(String i : pro){
				if(!(o.properties().getString(i).equals(this.properties().getString(i)))) return  false;
			}
		}

		if( ! Arrays.equals(this.getBody(), ((DefaultBytesMessage)obj).getBody()) ) {
			return false;
		}
		else {
			return true;
		}
			
	}

	@Override
	public int hashCode() {
		if (this == null)
			return 0;

		int result = 1;

		Set<String> header;
		if(this.headers() != null){
			header = this.headers().keySet();
			for(String i : header){
				result = 31 * result + i.hashCode()  ;
			}
		}

		Set<String> pro;
		if(this.properties() != null){
			pro = this.properties().keySet();
			for(String i : pro){
				result = 31 * result + (int)i.hashCode()  ;
			}
		}

		for (byte b : this.getBody())
			result = 31 * result + (int)b  ;

		return result;
	}

}
