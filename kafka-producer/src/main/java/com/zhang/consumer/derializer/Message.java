package com.zhang.consumer.derializer;

public class Message {
    private final int id;

    private final String name;

    public Message(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
