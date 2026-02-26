package com.framework.eventbus;

import java.io.*;

public class EventSerializer {

    public byte[] serialize(Object object) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {

            out.writeObject(object);
            return bos.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    public Object deserialize(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInputStream in = new ObjectInputStream(bis)) {

            return in.readObject();

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }
}