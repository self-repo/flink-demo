package com.demo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class Price {

    private String name;
    private double price;

    public Price(String name, double price) {
        this.name = name;
        this.price = price;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Price [name=" + name + ", price=" + price + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Price) {
            Price other = (Price) obj;
            return name.equals(other.name) && price == other.price;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (name + price).hashCode();
    }

    public static TypeSerializer<Price> createTypeSerializer() {
        TypeInformation<Price> typeInformation = (TypeInformation<Price>) TypeExtractor.createTypeInfo(Price.class);
        return typeInformation.createSerializer(new ExecutionConfig());
    }

}
