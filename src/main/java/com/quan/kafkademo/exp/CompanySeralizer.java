package com.quan.kafkademo.exp;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CompanySeralizer implements Serializer<Company> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if(data ==null){
            return null;
        }
        byte[] name;
        byte[] address;
        try{
            if(data.getName()==null) {
                name = new byte[0];
            }  else{
                name = data.getName().getBytes("UTF-8");
            }
            if(data.getAddress()==null){
                address = new byte[0];
            }else{
                address = data.getAddress().getBytes("UTF-8");
            }
            ByteBuffer buffer =  ByteBuffer.allocate(4+4+name.length+address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return  buffer.array();
        }catch(Exception e) {
            e.printStackTrace();
        }
        return  new byte[0];
    }

    @Override
    public void close() {

    }
}
