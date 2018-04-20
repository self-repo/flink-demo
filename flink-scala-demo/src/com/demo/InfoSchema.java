package com.demo;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class InfoSchema implements DeserializationSchema<Info> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TypeInformation<Info> getProducedType() {
		// TODO Auto-generated method stub
		return null;
	}

	public Info deserialize(byte[] message) throws IOException {
		return new Info(new String(message));
	}

	public boolean isEndOfStream(Info nextElement) {
		// TODO Auto-generated method stub
		return false;
	}

}
