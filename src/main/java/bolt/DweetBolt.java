package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class DweetBolt extends BaseRichBolt {
    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String url = "";
        try {

        //GeolocationBolt Tuple
//      outputFieldsDeclarer.declare(new Fields("message", "nombre", "lat", "lng"));
            String message = tuple.getStringByField("message").replaceAll(" ", "+");
            String nombre = tuple.getStringByField("nombre").replace(' ', '+');
            String lat = tuple.getStringByField("lat");
            String lng = tuple.getStringByField("lng");

            System.out.println(nombre);
//https://dweet.io/dweet/for/pepito?long=-4.894044&lat=41.540320&text=Estoy+en+Paris&category=Place&entity=Paris
            url = "https://dweet.io/dweet/for/prueba-dweet-alvaro-miguel-rodrigo-mbda?message=" + message
                    + "&nombre=" + nombre
                    + "&lat=" + lat
                    + "&lng=" + lng;


            //doGet
            URL peticion = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) peticion.openConnection();
            connection.setRequestMethod("GET");
//            connection.setRequestProperty("Content-length", "0");
//            connection.setUseCaches(false);
//            connection.setAllowUserInteraction(false);
            connection.connect();
            connection.getResponseCode();

            //Tratamiento respuesta
            System.out.println("**** dweetBolt OK!");

        } catch (IOException e1) {
            System.out.println("**** dweetBolt KO!");
            e1.printStackTrace();
        }

        // Confirmación de que la tupla ya ha sido tratada
        _collector.ack(tuple);


}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields());
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}