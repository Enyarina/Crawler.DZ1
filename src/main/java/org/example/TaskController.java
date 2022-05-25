package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;


public class TaskController {
    private static Logger log = LogManager.getLogger();
    private CloseableHttpClient client = null;
    private HttpClientBuilder builder;
    private String server;
    private int retryDelay = 5 * 1000;
    private int retryCount = 2;
    private int metadataTimeout = 30 * 1000;
    ConnectionFactory factory = new ConnectionFactory();

    public TaskController(String _server) {
        CookieStore httpCookieStore = new BasicCookieStore();
        builder = HttpClientBuilder.create().setDefaultCookieStore(httpCookieStore);
        client = builder.build();
        this.server = _server;

        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
    }

    public Document getUrl(String url) {
        int code = 0;
        boolean bStop = false;
        Document doc = null;
        for (int iTry = 0; iTry < retryCount && !bStop; iTry++) {
            log.info("Получение страницы по адресу: " + url);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(metadataTimeout)
                    .setConnectTimeout(metadataTimeout)
                    .setConnectionRequestTimeout(metadataTimeout)
                    .setExpectContinueEnabled(true)
                    .build();
            HttpGet request = new HttpGet(url);
            request.setConfig(requestConfig);
            CloseableHttpResponse response = null;
            try {
                response = client.execute(request);
                code = response.getStatusLine().getStatusCode();
                if (code == 404) {
                    log.warn("error get url " + url + " code " + code);
                    bStop = true;//break;
                } else if (code == 200) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        try {
                            doc = Jsoup.parse(entity.getContent(), "UTF-8", server);
                            break;
                        } catch (IOException e) {
                            log.error(e);
                        }
                    }
                    bStop = true;//break;
                } else {
                    //if (code == 403) {
                    log.warn("error get url " + url + " code " + code);
                    response.close();
                    response = null;
                    client.close();
                    CookieStore httpCookieStore = new BasicCookieStore();
                    builder.setDefaultCookieStore(httpCookieStore);
                    client = builder.build();
                    int delay = retryDelay * 1000 * (iTry + 1);
                    log.info("wait " + delay / 1000 + " s...");
                    try {
                        Thread.sleep(delay);
                        continue;
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            } catch (IOException e) {
                log.error(e);
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }
        return doc;
    }

    public String GetPage(String link) throws IOException, TimeoutException {
        Document ndoc = getUrl(link);
        String text = "";
        if (ndoc != null) {
            Connection conn = factory.newConnection();
            Channel channel = conn.createChannel();

            // The text of the publication
            String textPublication = ndoc.select("div.content-box").text();
            log.info("Текст публикации: " + textPublication);

            // The title of the publication
            String header = ndoc.select("title").first().text();
            log.info("Заголовок: " + header);

            // The time of the publication
            String timePublication = ndoc.select("meta[property=article:published_time]").first().attr("content");
            log.info("Время публикации: " + timePublication);

            // Author of the publication
            String authorPublication = ndoc.select("meta[name=twitter:data1]").first().attr("content");
            log.info("Автор публикации: " + authorPublication);

            // Link to the page
            log.info("Ссылка на страницу: " + link);
            //Создание очереди 2

            Json json = new Json(header, textPublication, authorPublication, link, timePublication);
            ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
            String json_complete = ow.writeValueAsString(json);
            channel.basicPublish("", Main.queueSecond, null, json_complete.getBytes());

            channel.close();
            conn.close();
        }
        return text;
    }

    void produce() throws IOException, TimeoutException, InterruptedException {
        Document doc = getUrl(Main.site);
        String title;
        if (doc != null) {
            title = doc.title();
            log.info(title);
            Main.ParseNews(doc, Main.site);
        }
    }

    void consume() throws IOException, TimeoutException, InterruptedException {
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        while (true) {
            try {
                if (channel.messageCount(Main.queueFirst) == 0) continue;
                String url = new String(channel.basicGet(Main.queueFirst, true).getBody(), StandardCharsets.UTF_8);

                if (url != null)
                    GetPage(url);
                notify();
            }
            catch (IndexOutOfBoundsException e) {
                wait();
            }
        }
    }
    void sendBD() throws IOException, TimeoutException {
        while (true){
            Connection conn = factory.newConnection();
            Channel channel = conn.createChannel();

            if (channel.messageCount(Main.queueSecond) == 0) continue;
            String json = new String(channel.basicGet(Main.queueSecond, true).getBody(), StandardCharsets.UTF_8);
            Client client = new PreBuiltTransportClient(
                    Settings.builder().put("cluster.name","docker-cluster").build())
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
            String sha256hex = org.apache.commons.codec.digest.DigestUtils.sha256Hex(json);
            client.prepareIndex("CrawlerDz", "_doc", sha256hex).setSource(json, XContentType.JSON).get();
            channel.close();
            conn.close();
        }
    }

}