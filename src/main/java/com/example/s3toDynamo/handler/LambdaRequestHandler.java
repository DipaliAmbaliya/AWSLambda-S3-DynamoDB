package com.example.s3toDynamo.handler;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.StringUtils;
import com.example.s3toDynamo.model.Product;
import com.example.s3toDynamo.model.ProductMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;


public class LambdaRequestHandler
  implements RequestHandler<S3Event, Void> {
  AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
  AmazonDynamoDB dynamoDBclient = AmazonDynamoDBClientBuilder.standard().build();
  @Override
  public Void handleRequest(S3Event s3event, Context context) {
    S3EventNotification.S3EventNotificationRecord record = s3event.getRecords().get(0);
    String bkt = record.getS3().getBucket().getName();
    String key = record.getS3().getObject().getKey().replace('+', ' ');
    try {
      key = URLDecoder.decode(key, "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
    try {
      context.getLogger().log("bkt: " + bkt + " key: "+ key);
      // get data from s3
      List<Product> list = getDataFromS3(bkt, key, context);
      // insert into dynamodb
      putProduct(list);

    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
    return null;
  }

  private List<Product> getDataFromS3(String bkt, String key, Context context) throws IOException {
    S3Object s3Object = s3Client.getObject(bkt, key);
    S3ObjectInputStream s3Stream = s3Object.getObjectContent();
    BufferedReader reader = new BufferedReader(new InputStreamReader(s3Stream, StringUtils.UTF8));
    List<Product> list = new ArrayList<Product>();
    String line;
    context.getLogger().log("reader: " + reader.toString());
    Gson gson = new GsonBuilder().create();
    while ((line = reader.readLine()) != null) {
      context.getLogger().log("line: " + line);
      Product product = gson.fromJson(line, Product.class);
      context.getLogger().log("Product: " + product.getProductId());
      list.add(product);
    }
    return list;
  }

  public void putProduct(List<Product> products) {

    DynamoDB dynamoDB = new DynamoDB(dynamoDBclient);

    Table table = dynamoDB.getTable("Product");
    for (Product product : products) {
      dynamoDBclient.putItem(new PutItemRequest()
        .withTableName("Product").
        withItem(ProductMapper.productToDynamoDb(product)));
    }
  }
}