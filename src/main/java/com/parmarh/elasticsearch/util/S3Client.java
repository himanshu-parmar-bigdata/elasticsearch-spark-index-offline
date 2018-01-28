package com.parmarh.elasticsearch.util;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class S3Client {

    private static AmazonS3 s3Client =   AmazonS3ClientBuilder.standard()
            .withRegion("us-east-1")
            .build();

    public static String readFile(String s3Url) throws IOException {
        AmazonS3URI s3URI = new AmazonS3URI(s3Url);
        return readFromS3(s3URI.getBucket(),s3URI.getKey());
    }

    public static String readFromS3(String bucketName, String key) throws IOException {
        S3Object s3object = s3Client.getObject(new GetObjectRequest(
                bucketName, key));
        System.out.println(s3object.getObjectMetadata().getContentType());
        System.out.println(s3object.getObjectMetadata().getContentLength());

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(s3object.getObjectContent()));
        StringBuffer stringBuffer = new StringBuffer();
        String line;
        while((line = reader.readLine()) != null) {
            stringBuffer.append(line);
        }

        return stringBuffer.toString();
    }
}
