package org.example;

public class Json {
    Json(String header, String text, String author, String url, String time) {
        HEADER=header;
        TEXT=text;
        AUTHOR=author;
        URL=url;
        TIME=time;
    }

    public String HEADER;
    public String TEXT;
    public String AUTHOR;
    public String URL;
    public String TIME;
}