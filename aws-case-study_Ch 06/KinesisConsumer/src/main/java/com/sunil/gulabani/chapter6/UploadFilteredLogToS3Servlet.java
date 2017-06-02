package com.sunil.gulabani.chapter6;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

@WebServlet("/UploadFilteredLogToS3Servlet")
public class UploadFilteredLogToS3Servlet extends HttpServlet {

    private String BUCKET_NAME = "chapter6-filter-logs";

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String fileName = request.getParameter("fileName");
        String data = request.getParameter("data");

        Path tempFile = Files.createTempFile(fileName, ".tmp");
        List<String> lines = Arrays.asList(data);
        Files.write(tempFile, lines, Charset.defaultCharset(), StandardOpenOption.WRITE);

        S3Operations s3Operations = new S3Operations();
        String eTag = s3Operations.putObject(BUCKET_NAME, fileName, tempFile.toFile());

        PrintWriter out = response.getWriter();
        out.write("<html>");
        out.write("     <body>");
        out.write("     <p>File Uploaded Successfully. ETag: " + eTag + "</p>");
        out.write("     </body>");
        out.write("</html>");
    }
}
