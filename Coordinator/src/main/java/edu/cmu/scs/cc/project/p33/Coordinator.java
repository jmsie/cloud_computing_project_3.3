package edu.cmu.scs.cc.project.p33;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.apache.log4j.Logger;

import io.vertx.ext.web.RoutingContext;

/**
 * We will describe a possible viable implementation of this task.
 * You may decide whether you want to strictly follow our suggestions or not.
 *
 * In Task 1, you may have implemented a locking mechanism in this {@link Coordinator} class.
 *
 * Hint: one possible viable implementation of Task 1 requires you to add additional data structures
 * into this {@link Coordinator} class, for the purpose of locking and ordering of operations.
 *
 * All the data structures you added, should be initialized or re-initialized in two places:
 * 1. {@link #Coordinator()}
 * 2. {@link #flushHandler(RoutingContext)}
 *
 * A reminder that you need to finish all the TODOs. You are required to remove the
 * TODOs in your last submission, as an indication of task completion and to show
 * that you are following a sound coding discipline and best practices.
 */
public class Coordinator {
    
    /**
     * Logger
     */
    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    /**
     * These PRIVATE-IPs will be filled by {@link #Coordinator()} constructor. You don't need
     * to fill them manually.
     */
    private static String dataCenter1 = "DATACENTER1-PRIVATE_IP";
    private static String dataCenter2 = "DATACENTER2-PRIVATE_IP";
    private static String dataCenter3 = "DATACENTER3-PRIVATE_IP";
    private static String truetimeServer = "<TRUETIMESERVER-PRIVATE-IP>";


    /*
     * TODO: define your own data structures here.
     */
    private static ReadWriteLock lock = null;
    
    /**
     * Initializes a Coordinator object.
     */
    public Coordinator() {
        // DO NOT MODIFY THIS
        Properties properties = null;
        try {
            InputStream in = getClass().getClassLoader().getResourceAsStream("config.prop");
            if (in == null) {
                LOGGER.error("config.prop file isn't found!!");
            } else {
                properties = new Properties();
                properties.load(in);
                in.close();
            }
        } catch (IOException e) {
            LOGGER.error("IOException while parsing config.prop!");
        }

        if (properties != null) {
            if (!(properties.containsKey("DATACENTER1") &&
                    properties.containsKey("DATACENTER2") &&
                    properties.containsKey("DATACENTER3") &&
                    properties.containsKey("TRUETIMESERVER"))) {
                LOGGER.error("Failed to read private IPs. Please make sure config.prop has proper format.");
            }

            dataCenter1 = properties.getProperty("DATACENTER1");
            dataCenter2 = properties.getProperty("DATACENTER2");
            dataCenter3 = properties.getProperty("DATACENTER3");
            truetimeServer = properties.getProperty("TRUETIMESERVER");

            KeyValueLib.dataCenters.put(dataCenter1, 1);
            KeyValueLib.dataCenters.put(dataCenter2, 2);
            KeyValueLib.dataCenters.put(dataCenter3, 3);
        }
        /*
         * TODO: initialize the data structures
        */
        lock = new ReadWriteLock();




    }

    /**
     * TODO: Implement the PUT handler.
     *
     * Each PUT operation is handled in a new thread.
     *
     * Hint: one possible viable implementation of this put handler is as follows:
     *
     * 1. Attempt to block all the other PUT/GET operations with a larger timestamp.
     * The thread may need to wait until this PUT operation is at the head of the queue.
     * 2. Send out a PUT request to each data center concurrently.
     * 3. Stop blocking other PUT/GET operations.
     * 4. Return an empty response to the client,
     * i.e., using {@code context.response().end()}.
     *
     * @param context the context for the handling of a request
     */
    public void putHandler(RoutingContext context) {
        final String key = context.request().getParam("key");
        final String value = context.request().getParam("value");

        Thread t = new Thread(new Runnable() {
            public void run() {
                // You should use the following timestamp for ordering requests
                Long timestamp = getTimeStamp(context);
                // WARNING: Do not modify the line above
                try {

                    // TODO: program here
                    // 1.


                } catch (Exception e) {
                    LOGGER.error("PutHandler Exception: " + e.getMessage());
                }
                // Every important notice should be repeated for three times
                // Do not remove this
                // Do not remove this
                // Do not remove this
                context.response().end();
            }
        });
        t.start();
    }

    /**
     * TODO: Implement the GET handler.
     *
     * Each GET operation is handled in a new thread.
     *
     * Hint: one possible viable implementation of this handler is as follows:
     *
     * 1. Attempt to block all the PUT operations with a large timestamp.
     * The thread may need to wait if this GET operation is blocked by other PUT operations.
     * 2. Get the value from the data center given the value of {@code loc}.
     * 3. Stop blocking other PUT operations.
     * 4. Pass the value back to the client, i.e., using {@code context.response().end(value)}.
     *
     * @param context the context for the handling of a request
     */
    public void getHandler(RoutingContext context) {
        final String key = context.request().getParam("key");
        final String loc = context.request().getParam("loc");

        Thread t = new Thread(new Runnable() {
            public void run() {
                // You should use the following timestamp for ordering requests
                Long timestamp = getTimeStamp(context);
                // You should use the following variable to store your value
                String value = "null";
                // WARNING: Do not modify the line above

                try {

                    // TODO: program here

                } catch (Exception e) {
                    LOGGER.error("GetHandler Exception: " + e.getMessage());
                }

                // Every important notice should be repeated for three times
                // Do not remove this
                // Do not remove this
                // Do not remove this
                context.response().end(value);
            }
        });
        t.start();
    }

    /**
     * This endpoint will be used by the auto-grader to flush the data centers
     * before tests.
     * 
     * @param context the context for the handling of a request
     */
    public void flushHandler(RoutingContext context) {
        try {
            flush(dataCenter1);
            flush(dataCenter2);
            flush(dataCenter3);
        } catch (Exception e) {
            LOGGER.error("flushHandler exception!", e);
        }

        /*
         * TODO: re-initialize the data structures.
         */

        lock = null;
        lock = new ReadWriteLock();
        
        context.response().end();
    }

    /**
     * This method flushes the data center by calling its flush endpoint.
     * 
     * Note: DO NOT modify this method.
     *
     * @param dataCenter the private ip of data center.
     * @throws Exception
     */
    private void flush(String dataCenter) throws Exception {
        URL url = new URL("http://" + dataCenter + ":8080/flush");
        BufferedReader in = new BufferedReader(new InputStreamReader(url.openConnection().getInputStream()));
        // Wait for the data center to finish flush
        while (in.readLine() != null) {
            // do nothing
        }
        in.close();
    }

    /**
     * No matched router handler.
     *
     * Note: DO NOT modify this method.
     * 
     * @param context the context for the handling of a request
     *           
     */
    public void noMatchHandler(RoutingContext context) {
        context.response().putHeader("Content-Type", "text/html");
        String response = "Not found.";
        context.response().putHeader("Content-Length", String.valueOf(response.length()));
        context.response().end(response);
        context.response().close();
    }

    /**
     * Assign a timestamp to a request.
     * 
     * Note: DO NOT modify this method.
     * 
     * @param context the context for the handling of a request
     * @return the Timestamp retrieved from the truetime server
     */
    public Long getTimeStamp(RoutingContext context) {
        try {
            return KeyValueLib.GETTIME(truetimeServer, context);
        } catch (IOException e) {
            LOGGER.error("Failed to retrieve timestamp.", e);
            return -1L;
        }
    }
}
