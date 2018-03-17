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


import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class EntitiesBolt extends BaseRichBolt {

    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String dandelion_key = "bc5e7f9be6884eb08ada8ffeb833be2f";

        try {
            JSONObject json = new JSONObject(tuple);
            String cadenaUrl;

            cadenaUrl = "https://api.dandelion.eu/datatxt/nex/v1/?social=True&min_confidence=0.6&country=-1&include=image%2Cabstract%2Ctypes%2Ccategories%2Clod&text=";
            cadenaUrl += json.getString("values").replaceAll("[^\\p{Alpha}\\p{Digit}]+","+").replace(' ', '+');
            cadenaUrl += "&token=" + dandelion_key;

            URL url = new URL(cadenaUrl);

            HttpURLConnection c = (HttpURLConnection) url.openConnection();
            c.setRequestMethod("GET");
            c.setRequestProperty("Content-length", "0");
            c.setUseCaches(false);
            c.setAllowUserInteraction(false);
            c.connect();
            int status = c.getResponseCode();

            StringBuilder sb = new StringBuilder();
            switch (status) {
                case 200:
                case 201:
                    BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream()));
                    String line;
                    while ((line = br.readLine()) != null) {
                        sb.append(line+"\n");
                    }
                    br.close();
            }

            JSONObject jsonResponse = new JSONObject(sb.toString());
            JSONArray results = jsonResponse.getJSONArray("annotations");
            JSONArray entidades = new JSONArray();
            for (int i=0; i<results.length();i++) {
                String categoria = "Otro";
                JSONObject annotation = results.getJSONObject(i);
                String label = annotation.getString("label");
                if (label.equals("HTTPS") || label.equals("HTTP")) continue;
                String nombre = annotation.getString("title");
                JSONArray tipos = annotation.getJSONArray("types");
                for (int j= 0; j< tipos.length(); j++) {
                    String tipo = tipos.getString(j).substring(28);
                    if (tipo.equals("Place") || tipo.equals("Location")) {
                        categoria = "Place";
                    }
                    else {
                        categoria = "Otro";
                    }
                }
                JSONObject entidad = new JSONObject();
                if (categoria.equals("Place")) {
                    entidad.put("nombre", nombre);
                    entidad.put("categoria", categoria);
                    entidades.put(entidad);
                }
            }

            json.put("entidades", entidades);

            _collector.emit(new Values(json.toString()));

            // Confirmación de que la tupla fue creada

            _collector.ack(tuple);

        }
        catch( JSONException | IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("json"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}