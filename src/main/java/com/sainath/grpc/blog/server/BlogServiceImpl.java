package com.sainath.grpc.blog.server;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.proto.blog.Blog;
import com.proto.blog.BlogServiceGrpc;
import com.proto.blog.CreateBlogRequest;
import com.proto.blog.CreateBlogResponse;
import com.proto.blog.ReadBlogRequest;
import com.proto.blog.ReadBlogResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlogServiceImpl extends BlogServiceGrpc.BlogServiceImplBase {

    private static final MongoClient MONGO_CLIENT = MongoClients.create("mongodb://localhost:27017");
    private static final MongoDatabase DATABASE = MONGO_CLIENT.getDatabase("mydb");
    private static final MongoCollection<Document> COLLECTION = DATABASE.getCollection("blog");

    //private static final Logger LOGGER = LoggerFactory.getLogger(BlogServiceImpl.class);

    @Override
    public void createBlog(CreateBlogRequest request, StreamObserver<CreateBlogResponse> responseObserver) {
        System.out.println("Received create blog request");
        Blog blog = request.getBlog();
        Document document = new Document("author_id", blog.getAuthorId())
                .append("title", blog.getTitle())
                .append("content", blog.getContent());

        System.out.println("Inserting blog");
        COLLECTION.insertOne(document);

        String id = document.getObjectId("_id").toString();
        System.out.println("Inserted blog: "+ id);

        CreateBlogResponse response = CreateBlogResponse.newBuilder()
                .setBlog(blog.toBuilder().setId(id))
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void readBlog(ReadBlogRequest request, StreamObserver<ReadBlogResponse> responseObserver) {
        System.out.println("Received read blog request");
        String blogId = request.getBlogId();

        System.out.println("Searching for blog");
        Document result = null;
        try {
            result = COLLECTION.find(Filters.eq("_id", new ObjectId(blogId)))
                    .first();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.NOT_FOUND
                            .withDescription("The blog with id " + blogId + " not found!")
                            .augmentDescription(e.getLocalizedMessage())
                            .asException()
            );
        }

        if (result == null) {
            System.out.println("Blog not found");
            responseObserver.onError(
                    Status.NOT_FOUND
                            .withDescription("The blog with id " + blogId + " not found!")
                            .asException()
            );
        } else {
            System.out.println("Blog found, sending response");
            Blog blog = Blog.newBuilder()
                    .setAuthorId(result.getString("author_id"))
                    .setTitle(result.getString("author_id"))
                    .setContent(result.getString("author_id"))
                    .setId(blogId)
                    .build();

            ReadBlogResponse response = ReadBlogResponse.newBuilder()
                            .setBlog(blog)
                                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
