package com.jlcompany.image.ms.service;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

public class GCPDownloader {

//    public static void downloadObject(String projectId, String bucketName, String objectName, String destFilePath) {
    public static InputStream downloadObjectAsStream(String projectId, String bucketName, String objectName) {

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

        Blob blob = storage.get(BlobId.of(bucketName, objectName));
        System.out.println("storage.get() called!!!!" + " time: " +  java.time.LocalDateTime.now());
        byte[] content = blob.getContent();
        System.out.println("blob.getContent() called!!!!" + " time: " +  java.time.LocalDateTime.now());
        InputStream contentAsStream = new ByteArrayInputStream(content);
        return contentAsStream;
    }

    public static ReadChannel readObjectToChannel(String projectId, String bucketName, String objectName) throws IOException {

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

        ReadChannel reader = storage.reader(bucketName, objectName);

        return reader;

//        try (ReadChannel reader = storage.reader(bucketName, objectName)) {
//            ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
//            while (reader.read(bytes) > 0) {
//                bytes.flip();
//                // do something with bytes
//                bytes.clear();
//            }
//        }

    }

}
