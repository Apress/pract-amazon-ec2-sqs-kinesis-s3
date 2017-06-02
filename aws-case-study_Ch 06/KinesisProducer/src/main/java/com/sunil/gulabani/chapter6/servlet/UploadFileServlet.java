package com.sunil.gulabani.chapter6.servlet;

import com.sunil.gulabani.chapter6.kinesis.KinesisOperations;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

@WebServlet("/UploadFileServlet")
public class UploadFileServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private ServletFileUpload uploader = null;
    private String STREAM_NAME = "chapter6kinesisstream";

    @Override
    public void init() throws ServletException{
        DiskFileItemFactory fileFactory = new DiskFileItemFactory();
        File filesDir = (File) getServletContext().getAttribute("FILES_DIR_FILE");
        fileFactory.setRepository(filesDir);
        this.uploader = new ServletFileUpload(fileFactory);
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html");

        PrintWriter out = response.getWriter();

        out.write("<html><head></head><body>");

        try {
            List<FileItem> fileItemsList = uploader.parseRequest(request);

            Iterator<FileItem> fileItemsIterator = fileItemsList.iterator();

            while(fileItemsIterator.hasNext()){
                FileItem fileItem = fileItemsIterator.next();

                byte[] fileData = fileItem.get();

                KinesisOperations kinesisProducer = new KinesisOperations();

                String sequenceNumber = kinesisProducer.putRecord(STREAM_NAME, "logs", fileData);

                out.write("File uploaded successfully to kinesis!!!");

                out.write("<br>");

                out.write("Kinesis Sequence Number: " + sequenceNumber);
            }
        } catch (FileUploadException e) {
            out.write("Exception: " + e.getMessage());
        } catch (Exception e) {
            out.write("Exception: " + e.getMessage());
        }
        out.write("</body></html>");
    }
}
