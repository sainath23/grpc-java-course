package com.sainath.grpc.greeting.client;

import com.proto.greet.GreetEveryoneRequest;
import com.proto.greet.GreetEveryoneResponse;
import com.proto.greet.GreetManyTimesRequest;
import com.proto.greet.GreetRequest;
import com.proto.greet.GreetResponse;
import com.proto.greet.GreetServiceGrpc;
import com.proto.greet.GreetServiceGrpc.GreetServiceStub;
import com.proto.greet.GreetServiceGrpc.GreetServiceBlockingStub;
import com.proto.greet.Greeting;
import com.proto.greet.LongGreetRequest;
import com.proto.greet.LongGreetResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GreetingClient {

    public static void main(String[] args) {
        System.out.println("Hello I am gRPC client");
        new GreetingClient().run();
    }

    private void run() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        //doUnaryCall(channel);
        //doServerStreamingCall(channel);
        //doClientStreamingCall(channel);
        doBiDirectionalStreamingCall(channel);

        System.out.println("Shutting down channel");
        channel.shutdown();
    }

    private static void doUnaryCall(ManagedChannel channel) {
        System.out.println("Creating a stub");
        // Created greet service client (blocking - sync)
        GreetServiceBlockingStub syncGreetClient = GreetServiceGrpc.newBlockingStub(channel);

        // Created a protocol buffer greeting message
        Greeting greeting = Greeting.newBuilder()
                .setFirstName("Sainath")
                .setLastName("Parkar")
                .build();

        // Created greet request
        GreetRequest greetRequest = GreetRequest.newBuilder()
                .setGreeting(greeting).build();

        // Call RPC and get response
        GreetResponse greetResponse = syncGreetClient.greet(greetRequest);

        System.out.println("Response: " + greetResponse.getResult());
    }

    private static void doServerStreamingCall(ManagedChannel channel) {
        System.out.println("Creating a stub");
        // Created greet service client (blocking - sync)
        GreetServiceBlockingStub syncGreetClient = GreetServiceGrpc.newBlockingStub(channel);

        GreetManyTimesRequest request = GreetManyTimesRequest.newBuilder()
                .setGreeting(Greeting.newBuilder().setFirstName("Sainath").setLastName("Parkar"))
                .build();

        // stream the responses in blocking manner
        syncGreetClient.greetManyTimes(request)
                .forEachRemaining(greetManyTimesResponse -> {
                    System.out.println(greetManyTimesResponse.getResult());
                });
    }

    private void doClientStreamingCall(ManagedChannel channel) {
        System.out.println("Creating a stub");

        // Created greet service client (async)
        GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        StreamObserver<LongGreetRequest> requestStreamObserver = asyncClient.longGreet(new StreamObserver<LongGreetResponse>() {
            @Override
            public void onNext(LongGreetResponse value) {
                // We get a response from server
                System.out.println("Received response from server");
                System.out.println(value.getResult());
            }

            @Override
            public void onError(Throwable t) {
                // We get an error from server
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                // The server is done sending us data
                System.out.println("Server completed sending");
                countDownLatch.countDown();
            }
        });

        System.out.println("Sending message #1");
        requestStreamObserver.onNext(getLongGreetRequest("Sainath"));
        System.out.println("Sending message #2");
        requestStreamObserver.onNext(getLongGreetRequest("Sai"));
        System.out.println("Sending message #3");
        requestStreamObserver.onNext(getLongGreetRequest("Parkar"));
        System.out.println("Sending message #4");
        requestStreamObserver.onNext(getLongGreetRequest("Foo"));

        requestStreamObserver.onCompleted();

        try {
            countDownLatch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void doBiDirectionalStreamingCall(ManagedChannel channel) {
        System.out.println("Creating a stub");

        // Created greet service client (async)
        GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        StreamObserver<GreetEveryoneRequest> requestStreamObserver = asyncClient.greetEveryone(new StreamObserver<GreetEveryoneResponse>() {
            @Override
            public void onNext(GreetEveryoneResponse value) {
                System.out.println("Response from server: " + value.getResult());
            }

            @Override
            public void onError(Throwable t) {
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("Server is done sending data");
                countDownLatch.countDown();
            }
        });

        Arrays.asList("Sainath", "Foo", "Bar").forEach(name -> {
            System.out.println("Sending: " + name);
            requestStreamObserver.onNext(GreetEveryoneRequest.newBuilder()
                    .setGreeting(Greeting.newBuilder().setFirstName(name))
                    .build());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        requestStreamObserver.onCompleted();

        try {
            countDownLatch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private LongGreetRequest getLongGreetRequest(String firstName) {
        Greeting greeting = Greeting.newBuilder().setFirstName(firstName).build();
        return LongGreetRequest.newBuilder().setGreeting(greeting).build();
    }
}
