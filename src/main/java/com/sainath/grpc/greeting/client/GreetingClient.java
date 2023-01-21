package com.sainath.grpc.greeting.client;

import com.proto.greet.GreetManyTimesRequest;
import com.proto.greet.GreetRequest;
import com.proto.greet.GreetResponse;
import com.proto.greet.GreetServiceGrpc;
import com.proto.greet.GreetServiceGrpc.GreetServiceFutureStub;
import com.proto.greet.GreetServiceGrpc.GreetServiceBlockingStub;
import com.proto.greet.Greeting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GreetingClient {

    public static void main(String[] args) {
        System.out.println("Hello I am gRPC client");

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        System.out.println("Creating a stub");
        // DummyServiceBlockingStub syncClient = DummyServiceGrpc.newBlockingStub(channel);

        // Created greet service client (blocking - sync)
        GreetServiceBlockingStub syncGreetClient = GreetServiceGrpc.newBlockingStub(channel);

        //GreetServiceFutureStub asyncClient = GreetServiceGrpc.newFutureStub(channel);

        // Unary
        // Created a protocol buffer greeting message
        /*Greeting greeting = Greeting.newBuilder()
                .setFirstName("Sainath")
                .setLastName("Parkar")
                .build();

        // Created greet request
        GreetRequest greetRequest = GreetRequest.newBuilder()
                .setGreeting(greeting).build();

        // Call RPC and get response
        GreetResponse greetResponse = syncGreetClient.greet(greetRequest);

        System.out.println("Response: " + greetResponse.getResult());*/

        // Server streaming
        GreetManyTimesRequest request = GreetManyTimesRequest.newBuilder()
                        .setGreeting(Greeting.newBuilder().setFirstName("Sainath").setLastName("Parkar"))
                        .build();

        // stream the responses in blocking manner
        syncGreetClient.greetManyTimes(request)
                        .forEachRemaining(greetManyTimesResponse -> {
                            System.out.println(greetManyTimesResponse.getResult());
                        });

        System.out.println("Shutting down channel");
        channel.shutdown();
    }
}
