package org.example;

public class Json {
    Json(String header, String text, String author, String url, String time) {
        HEADER=header;
        DATA=text;
        AUTHOR=author;
        URL=url;
        TIME=time;
    }

    public String HEADER;
    public String DATA;
    public String AUTHOR;
    public String URL;
    public String TIME;
}