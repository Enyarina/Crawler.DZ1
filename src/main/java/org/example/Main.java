package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Main {
    private static Logger log = LogManager.getLogger();
    private static org.example.TaskController taskController;
    protected static String site = "https://spmiyaki.ru/";

    protected static String queueName = "myQueue";

    static public void ParseNews(Document doc, String url) {
        Elements news = doc.getElementsByClass("theiaStickySidebar").select("div[class*=news_loop_img]");
        for (Element element : news) {
            try {
                Element eTitle = element.child(0);
                String link = eTitle.attr("href");
                String text = taskController.GetPage(link);

                log.info(text);

            } catch (Exception e) {
                log.error(e);
            }
        }

        return;
    }

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        taskController = new org.example.TaskController(site);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        boolean durable = true;

        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        channel.queueDeclare(queueName, durable, false, false, null);

        channel.close();
        conn.close();

        // Create producer thread
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    taskController.produce();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        // Create consumer thread
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    taskController.consume();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        // Start both threads
        t1.start();
        t2.start();

        // t1 finishes before t2
        t1.join();
        t2.join();


        return;
    }

}
