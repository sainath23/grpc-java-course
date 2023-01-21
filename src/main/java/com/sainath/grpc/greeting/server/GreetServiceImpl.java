package com.sainath.grpc.greeting.server;

import com.proto.greet.GreetManyTimesRequest;
import com.proto.greet.GreetManyTimesResponse;
import com.proto.greet.GreetRequest;
import com.proto.greet.GreetResponse;
import com.proto.greet.GreetServiceGrpc.GreetServiceImplBase;
import com.proto.greet.Greeting;
import io.grpc.stub.StreamObserver;

import java.util.stream.IntStream;

public class GreetServiceImpl extends GreetServiceImplBase {

    @Override
    public void greet(GreetRequest request, StreamObserver<GreetResponse> responseObserver) {
        // Extract the fields
        Greeting greeting = request.getGreeting();
        String firstName = greeting.getFirstName();
        String lastName = greeting.getLastName();

        // Build response
        String result = "Hello " + firstName + " " + lastName;
        GreetResponse response = GreetResponse.newBuilder()
                .setResult(result)
                .build();

        // Send the response
        responseObserver.onNext(response);

        // Complete the RPC call
        responseObserver.onCompleted();
    }

    @Override
    public void greetManyTimes(GreetManyTimesRequest request, StreamObserver<GreetManyTimesResponse> responseObserver) {
        // Extract the fields
        Greeting greeting = request.getGreeting();
        String firstName = greeting.getFirstName();
        String lastName = greeting.getLastName();

        IntStream.range(1, 11).forEach(number -> {
            String result = "Hello " + firstName + " " + lastName + ", response number: " + number;
            GreetManyTimesResponse response = GreetManyTimesResponse.newBuilder()
                    .setResult(result)
                    .build();

            responseObserver.onNext(response);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                responseObserver.onError(e);
            }
        });

        responseObserver.onCompleted();
    }
}
