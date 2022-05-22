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

    protected static String queueFirst = "myQueue";
    protected static String queueSecond = "myQueueSecond";

    static public void ParseNews(Document doc, String url) throws InterruptedException, IOException, TimeoutException {
        Elements news = doc.getElementsByClass("theiaStickySidebar").select("div[class*=news_loop_img]");
        Connection conn = taskController.factory.newConnection();
        Channel channel = conn.createChannel();
        for (Element element : news) {
            try {

                Element eTitle = element.child(0);
                String link = eTitle.attr("href");
                String text = taskController.GetPage(link);
                channel.basicPublish("", Main.queueFirst, null, link.getBytes());

                log.info(text);


            } catch (Exception e) {
                log.error(e);
            }
        }
        channel.close();
        conn.close();

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
        boolean durable = false;

        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        channel.queueDeclare(queueFirst, durable, false, false, null);
        channel.queueDeclare(queueSecond, durable, false, false, null);

        channel.close();
        conn.close();

        // Create producer thread
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    taskController.produce();
                }
                catch (InterruptedException | IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });
        // Create consumer thread
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    taskController.consume();
                }
                catch (InterruptedException | IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });

        // Create send to bd
        Thread t3 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    taskController.sendBD();
                }
                catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });

        // Start threads
        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();


        return;
    }

}
