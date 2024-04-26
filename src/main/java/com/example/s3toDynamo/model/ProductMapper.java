package com.example.s3toDynamo.model;


import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Map;

public class ProductMapper {
  private static final String PK = "ProductId";
  private static final String NAME = "name";
  private static final String PRICE = "price";


  public static Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> productToDynamoDb(Product product) {
    return Map.of(
      PK, new AttributeValue().withS(product.getProductId()),
      NAME,  new AttributeValue().withS(product.getName()),
      PRICE,  new AttributeValue().withN(product.getPrice().toString())
    );
  }
}
