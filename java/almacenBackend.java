package test2;

import java.sql.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class ProductBackend {

    private static String QUEUE_NAME;
    private static String DB_HOST;
    private static int DB_PORT ;
    private static String DB_NAME ;
    private static String  DB_USER ;
    private static String DB_PASSWORD ;
    private static String RABBIT_HOST;
    static int RABBIT_PORT;

    public static void main(String[] args) throws Exception {

//        QUEUE_NAME = args[0];
//        DB_HOST = args[1];
//        DB_PORT = Integer.parseInt(args[2]);
//        DB_NAME = args[3];
//        DB_USER = args[4];
//        DB_PASSWORD = args[5];
//        RABBIT_HOST = args[6];

        QUEUE_NAME = System.getenv("QUEUE_NAME");
        DB_HOST = System.getenv("DB_HOST");
        DB_PORT = Integer.parseInt(System.getenv("DB_PORT"));
        DB_NAME = System.getenv("DB_NAME");
        DB_USER = System.getenv("DB_USER");
        DB_PASSWORD = System.getenv("DB_PASSWORD");
        RABBIT_HOST = System.getenv("RABBIT_HOST");
        RABBIT_PORT = Integer.parseInt(System.getenv("RABBIT_PORT"));

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT_HOST);
//        factory.setPort(15674);
        com.rabbitmq.client.Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        System.out.println(" [*] Esperando por mensajes. Para sailr presione CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");

            if (message.equals("{\"req\":\"getProducts\"}")) {
                String productsJson = getProductsFromDatabase();
                channel.basicPublish("", QUEUE_NAME, null, productsJson.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent product data to the queue");
            }else{
                updateStock(message);
            }
        };

        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
    }

    private static String getProductsFromDatabase() {
        try {
            String jdbcUrl = "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;
            java.sql.Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, DB_PASSWORD);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM productos");

            JSONArray productsArray = new JSONArray();

            while (resultSet.next()) {
                JSONObject productObj = new JSONObject();
                productObj.put("id", resultSet.getString("id"));
                productObj.put("producto", resultSet.getString("producto"));
                productObj.put("cantidad", resultSet.getString("cantidad"));
                productObj.put("precio", resultSet.getString("precio"));
                productObj.put("image_url", resultSet.getString("image_url"));
                productObj.put("descripcion", resultSet.getString("descripcion"));

                productsArray.put(productObj);
            }

            JSONObject responseObj = new JSONObject();
            responseObj.put("productos", productsArray);

            resultSet.close();
            statement.close();
            connection.close();

            return responseObj.toString();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    private static void updateStock(String json) {
        JSONObject jsonObject = new JSONObject(json);
        JSONArray productosArray = jsonObject.getJSONArray("productos");
        String jdbcUrl = "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;


        try (java.sql.Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, DB_PASSWORD)) {

            for (int i = 0; i < productosArray.length(); i++) {
                JSONObject producto = productosArray.getJSONObject(i);
                String id = producto.getString("ID");
                int cantidad = producto.getInt("cantidad");


                String query = "UPDATE productos SET cantidad = cantidad - ? WHERE ID = ?";
                try (PreparedStatement statement = connection.prepareStatement(query)) {
                    statement.setInt(1, cantidad);
                    statement.setString(2, id);
                    statement.executeUpdate();
                }
            }

            System.out.println("Stock actualizado exitosamente.");
        } catch (SQLException e) {
            System.err.println("Error actualizando stock: " + e.getMessage());
        }
    }
}
