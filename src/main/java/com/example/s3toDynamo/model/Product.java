package com.example.s3toDynamo.model;

public class Product {
  String ProductId;
  String name;
  Float price;

  public String getProductId() {
    return ProductId;
  }

  public void setProductId(String productId) {
    ProductId = productId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Float getPrice() {
    return price;
  }

  public void setPrice(Float price) {
    this.price = price;
  }
}
