package com.sainath.grpc.greeting.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.io.File;
import java.io.IOException;

public class GreetingServer {

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Hello gRPC");

        // Plaintext server
//        Server server = ServerBuilder.forPort(50051)
//                .addService(new GreetServiceImpl())
//                .build();

        // Secured channel
        Server server = ServerBuilder.forPort(50051)
                        .addService(new GreetServiceImpl())
                        .addService(ProtoReflectionService.newInstance()) // reflection
                        .useTransportSecurity(
                                new File("ssl/server.crt"),
                                new File("ssl/server.pem")
                        )
                        .build();

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received shutdown request");
            server.shutdown();
            System.out.println("Successfully stopped the server");
        }));

        server.awaitTermination();
    }
}
