package com.jlcompany.image.ms.controller;

import com.google.cloud.ReadChannel;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import reactor.core.publisher.Flux;

import javax.imageio.ImageIO;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;

import static com.jlcompany.image.ms.service.GCPDownloader.downloadObjectAsStream;
import static com.jlcompany.image.ms.service.GCPDownloader.readObjectToChannel;

@RestController
public class ImageController {

    @GetMapping(value = "/image")
    public String getBooking() {
        return "hello1";
    }

    @GetMapping(value = "/stream")
    public ResponseEntity<StreamingResponseBody> streamData() {
        StreamingResponseBody responseBody = response -> {
            for (int i = 1; i <= 1000; i++) {
                try {
                    Thread.sleep(10);
                    response.write(("Data stream line - " + i + "\n").getBytes());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_PLAIN)
                .body(responseBody);
    }

    @GetMapping(value = "/flux", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Flux<Object> streamDataFlux() {
        return Flux.interval(Duration.ofSeconds(1)).map(i -> "Data stream line - " + i );
    }

    @GetMapping(value = "/stream/image")
    public ResponseEntity<StreamingResponseBody> streamImage() {

        InputStream inputStream = downloadObjectAsStream("images-323613", "streaming-images", "large.png");

        StreamingResponseBody responseBody = outputStream -> {

            System.out.println("Streaming started!!!!" + " time: " +  java.time.LocalDateTime.now());
            int numberOfBytesToWrite;
//            byte[] data = new byte[1024];
            byte[] data = new byte[1000000];
            while ((numberOfBytesToWrite = inputStream.read(data, 0, data.length)) != -1) {
                System.out.println("Writing some bytes.. bytes left = " + numberOfBytesToWrite + " time: " + java.time.LocalDateTime.now());
                outputStream.write(data, 0, numberOfBytesToWrite);
            }

            inputStream.close();
        };

        return ResponseEntity.ok()
                .contentType(MediaType.IMAGE_PNG)
                .body(responseBody);
    }

    @GetMapping(value = "/stream/image2")
    public ResponseEntity<StreamingResponseBody> streamImage2() throws IOException {

        ReadChannel reader = readObjectToChannel("images-323613", "streaming-images", "large.png");

        StreamingResponseBody responseBody = outputStream -> {

            System.out.println("Streaming started!!!!" + " time: " +  java.time.LocalDateTime.now());
            int numberOfBytesToWrite;
//            byte[] data = new byte[1000000];

            ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
            while ((numberOfBytesToWrite = reader.read(bytes)) > 0) {
                bytes.flip();

                System.out.println("Writing some bytes.. Time: " + java.time.LocalDateTime.now());
                outputStream.write(bytes.array(), 0, numberOfBytesToWrite);

                bytes.clear();
            }

            System.out.println("Streaming ended.. Time: " + java.time.LocalDateTime.now());
            reader.close();
            System.out.println("Closed reader.. Time: " + java.time.LocalDateTime.now());
        };

        return ResponseEntity.ok()
                .contentType(MediaType.IMAGE_PNG)
                .body(responseBody);
    }

}
