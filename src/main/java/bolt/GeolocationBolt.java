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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.StringTokenizer;

public class GeolocationBolt extends BaseRichBolt {
    private OutputCollector _collector;

    private String lat = "0.0";
    private String lng = "0.0";

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String url = "";
        JSONArray salida = new JSONArray();
        try {


            String nombre = tuple.getStringByField("nombre").replace(' ', '+');
            System.out.println(nombre);

            url = "http://maps.google.com/maps/api/geocode/json?address=" + nombre + "&sensor=false";
            URL peticion = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) peticion.openConnection();
            connection.connect();


            //Tratamiento salida
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = br.readLine()) != null)
                response.append(inputLine);

            JSONObject jsongoogle = new JSONObject(response.toString());
            JSONArray jarraygoogle = jsongoogle.getJSONArray("results");
            JSONObject jobjectgoogle = jarraygoogle.getJSONObject(0);
            JSONObject geo = jobjectgoogle.getJSONObject("geometry").getJSONObject("location");
            this.lat = geo.getString("lat");
            this.lng = geo.getString("lng");

            JSONObject sal = new JSONObject();

            sal.append("lat", this.lat);
            sal.append("lng", this.lng);
            sal.append("nombre", nombre);

            salida.put(sal);

        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (JSONException e1) {
            e1.printStackTrace();
        }


        System.out.println("***********");
        System.out.println(salida.toString());
        System.out.println("***********");

            /*
            json = {"sourceTask":2,"ackVal":0,"sourceComponent":"coger_twitters",
                   "values":["RT @FoxNews: Take a look at some photos from the #ClosingCeremony
                   of the 2018 Winter #Olympics in #PyeongChang2018, South Korea. The ceremo\u2026"],
                   "entidades":[{"categoria":"Place","nombre":"South Korea"}],
                   "messageId":{"anchors":[],"anchorsToIds":{}},
                   "location":[{"lng":["127.766922"],"nombre":["South+Korea"],"lat":["35.907757"]}],
                   "fields":{},"map":{"message":"RT @FoxNews: Take a look at some photos from the
                   #ClosingCeremony of the 2018 Winter #Olympics in #PyeongChang2018, South Korea.
                   The ceremo\u2026"},"sourceGlobalStreamid":{},"sourceStreamId":"default","empty":false}
             */

        ;
        ;

        _collector.emit(new Values(
                tuple.getStringByField("message"),
                tuple.getStringByField("nombre"),
                this.lat,
                this.lng)
        );

        // Confirmación de que la tupla ya ha sido tratada
        _collector.ack(tuple);


}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message", "nombre", "lat", "lng"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}