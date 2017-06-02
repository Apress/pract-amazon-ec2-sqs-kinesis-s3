package com.sunil.gulabani.chapter6;

import com.sunil.gulabani.chapter6.kinesis.KinesisOperations;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

@WebServlet("/")
public class HomeServlet extends HttpServlet {

    private String STREAM_NAME = "chapter6kinesisstream";

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html");

        PrintWriter out = response.getWriter();

        out.write("<html><head></head><body>");
        out.write(" <h1>Kinesis Consumer</h1>");
        out.write(" <hr>");
        KinesisOperations kinesisOperations = new KinesisOperations();
        Map<String, String> dataMap = kinesisOperations.getRecords(STREAM_NAME);

        for(String fileName: dataMap.keySet()) {
            out.write(" <hr>");
            out.write(" <form action='UploadFilteredLogToS3Servlet' method='post' >");
            out.write("     <p>FileName: " + fileName + "</p>");
            out.write("     <input style='display:none;' id='fileName' name='fileName' value='" + fileName + "' />");
            out.write("     <br>");
            out.write("     <textarea id='data' name='data' rows='10' cols='100'>");
            out.write(          dataMap.get(fileName));
            out.write("     </textarea>");
            out.write("     <button type='Submit'>Add to S3</button>");
            out.write(" </form>");
        }
        out.write("</body></html>");
    }
}
