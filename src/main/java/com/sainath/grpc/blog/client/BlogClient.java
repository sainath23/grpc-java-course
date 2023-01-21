package com.sainath.grpc.blog.client;

import com.proto.blog.Blog;
import com.proto.blog.BlogServiceGrpc;
import com.proto.blog.CreateBlogRequest;
import com.proto.blog.CreateBlogResponse;
import com.proto.blog.ReadBlogRequest;
import com.proto.blog.ReadBlogResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlogClient {

    //private static final Logger LOGGER = LoggerFactory.getLogger(BlogClient.class);

    public static void main(String[] args) {
        System.out.println("Hello I am gRPC client");
        new BlogClient().run();
    }

    private void run() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50052)
                .usePlaintext()
                .build();

        createBlog(channel);

    }

    private void createBlog(ManagedChannel channel) {
        BlogServiceGrpc.BlogServiceBlockingStub client = BlogServiceGrpc.newBlockingStub(channel);

        Blog blog = Blog.newBuilder()
                .setTitle("My first blog")
                .setContent("This is my first blog")
                .setAuthorId("Sainath Parkar")
                .build();

        CreateBlogRequest request = CreateBlogRequest.newBuilder().setBlog(blog).build();

        CreateBlogResponse response = client.createBlog(request);

        System.out.println("Received create blog response: " + response.toString());

        ReadBlogResponse readBlogResponse = client.readBlog(ReadBlogRequest.newBuilder()
                .setBlogId(response.getBlog().getId())
                .build());

        System.out.println("Received read blog response: " + readBlogResponse.toString());

        ReadBlogResponse readBlogResponseNotFound = client.readBlog(ReadBlogRequest.newBuilder()
                .setBlogId("63cc5ba52cea456a5aa269b5")
                .build());
    }

}
