package com.sunil.gulabani.chapter6;

import com.amazonaws.services.sqs.model.Message;
import com.google.gson.*;
import com.sunil.gulabani.chapter6.sqs.SQSOperations;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@WebServlet("/")
public class HomeServlet extends HttpServlet {
    private String QUEUE_NAME = "chapter6sqs";

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html");

        PrintWriter out = response.getWriter();

        out.write("<html><head></head><body>");
        out.write("<h1>SQS Consumer</h1>");
        out.write("<hr>");

        SQSOperations sqsOperations = new SQSOperations();
        String queueUrl = sqsOperations.getQueueUrl(QUEUE_NAME);
        List<Message> messages = sqsOperations.receiveMessageWithAck(queueUrl);
        for(Message message: messages) {
            Gson gson = new Gson();
            JsonElement jsonElement = gson.fromJson (message.getBody(), JsonElement.class);
            JsonArray records = jsonElement.getAsJsonObject().get("Records").getAsJsonArray();
            for(JsonElement record: records) {
                JsonElement s3 = record.getAsJsonObject().get("s3");
                JsonElement bucket = s3.getAsJsonObject().get("bucket");
                out.write("S3 Bucket Name: " + bucket.getAsJsonObject().get("name"));
                out.write("<br>");
                JsonElement object = s3.getAsJsonObject().get("object");
                out.write("S3 Object Key: " + object.getAsJsonObject().get("key"));
            }
            out.write("<hr>");
        }

        out.write("</body></html>");
    }
}
