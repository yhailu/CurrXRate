//package com.vertex.ve.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.*;
import org.json.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentHashMap;
import java.net.HttpURLConnection;
import javax.net.ssl.HttpsURLConnection;

import java.net.URL;
import java.io.IOException;


@Description(name = "Lookup", value = "Lookup(str,str) - Look up reference value using file table", extended = "Example: \n"
        + " SELECT lookup(column, table) FROM hive_table; ")
public class CurrXRate extends UDF {

    private static String delimiter = "|";
    private static ConcurrentHashMap<String, Double> rateMap = new ConcurrentHashMap<String, Double>();

    private static String getHTML(String urlToRead) {
        URL url;
        HttpURLConnection conn;
        BufferedReader rd;
        String line;
        String result = "";
        try {
            url = new URL(urlToRead);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            String response = conn.getResponseMessage();
            rd = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            while ((line = rd.readLine()) != null) {
                result += line;
            }
            rd.close();
        } catch (IOException e) {
            e.getStackTrace();

        } catch (Exception e) {
            e.getStackTrace();

        }
        return result;

    }

    private static String getHTTPS(String urlToRead) {
        URL url;
        HttpsURLConnection con;
        BufferedReader rd;
        String line;
        String result = "";
        try {
            url = new URL(urlToRead);
            con = (HttpsURLConnection)url.openConnection();
            con.setRequestProperty("Accept", "application/vnd.sdmx.data+json");
            con.setRequestProperty("version", "1.0.0-wd");
            con.setRequestMethod("GET");
            int response = con.getResponseCode();
            rd = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            while ((line = rd.readLine()) != null) {
                result += line;
            }
            rd.close();
        } catch (IOException e) {
            e.printStackTrace();

        } catch (Exception e) {
            e.printStackTrace();

        }
        return result;

    }

    public static double currXRateYahooNationalBank(String date, String from, String to) {
        Double mapRate = rateMap.get(from + delimiter + to);
        //if (mapRate != null) return mapRate;
        //String url = "http://download.finance.yahoo.com/d/quotes.csv?s=" + from + to + "=X&f=sl1d1ba&e=.csv";
        String url = "http://download.finance.yahoo.com/d/quotes.csv?date=" + date + "&s=" + from + to + "=X&f=sl1d1ba&e=.csv";
        String s = getHTML(url);
        //System.out.println("s: " + s);
        Double rate = Double.parseDouble(s.split(",")[1]);
        rateMap.put(from + delimiter + to, rate);
        return rate;
    }

    public static double currXRatePolandNationalBank(String date, String from){

        String to = "PLN"; //zloty

        Double mapRate = rateMap.get(from + delimiter + to);
        //if (mapRate != null) return mapRate;
        String url = "http://api.nbp.pl/api/exchangerates/rates/a/usd/"+ date+ "/?format=json";

        try {
            String jsonStr = getHTML(url);
            JSONObject jsonObj = new JSONObject(jsonStr);
            String rates = jsonObj.getString("rates");
            rates = rates.replaceAll("\\[", "").replaceAll("\\]","");
            JSONObject jsonObj1 = new JSONObject(rates);
            double n = Double.parseDouble(jsonObj1.getString("mid"));
            rateMap.put(from + delimiter + to, n);
            return n;

        } catch (Exception e) {
            e.printStackTrace();
        }
        double rate = 0.0;

        return rate;
    }

    public static double currXRateEuropeanNationalBank(String date, String from, String to){
        /* date parameter must be in format XXXX(year)-XX(month)-XX(day) */

        Double mapRate = rateMap.get(from + delimiter + to);
        //if (mapRate != null) return mapRate;
        String url = "https://sdw-wsrest.ecb.europa.eu/service/data/EXR/D." + from + "." + to + ".SP00.A?startPeriod=" + date + "&endPeriod=" + date;
        String s = getHTTPS(url);
        try {
            String jsonStr = getHTTPS(url);
            System.out.println(jsonStr);
            JSONObject jsonObj = new JSONObject(jsonStr);
            String rates = jsonObj.getString("dataSets");
            rates = rates.substring(1, rates.length()-1);
            JSONObject jsonObj1 = new JSONObject(rates);
            String observations = jsonObj1.getString("series");
            JSONObject jsonObj2 = new JSONObject(observations);
            String rate = jsonObj2.getString("0:0:0:0:0");
            JSONObject jsonObj3 = new JSONObject(rate);
            String rate2 = jsonObj3.getString("observations");
            JSONObject jsonObj4 = new JSONObject(rate2);
            String str = jsonObj4.getString("0");
            String str1 = " ";
            str1 = str.substring(1, str.length()-1);
            double rate3 = Double.parseDouble(str1.substring(0, 5));
            rateMap.put(to+ delimiter + from, rate3);


        } catch (Exception e) {
            e.printStackTrace();
        }

        double rate = 0.0;
        //rateMap.put(from + delimiter + to, rate);
        return rate;
    }


    public Double evaluate(String date, Text txt1, Text txt2) {
        if (txt1 == null || txt2 == null) {
            return null;
        }

        double rt = currXRateYahooNationalBank(date, txt1.toString(), txt2.toString());
        double europe= currXRateEuropeanNationalBank("2010-10-16", txt1.toString(), txt2.toString());

        return rt;
    }
    public static void main(String[] args) {

        CurrXRate cr = new CurrXRate();

        /*double d = cr.evaluate("2017-03-10", new Text("USD"), new Text("EUR"));
        System.out.println("D: " + d);*/

       double rt = currXRateYahooNationalBank("2017-10-10", "USD", "EUR");

       double poland = currXRatePolandNationalBank("2017-10-26", "USD");

       double europe= currXRateEuropeanNationalBank("2017-10-25", "USD", "EUR");

       rateMap.forEach((k,v)->System.out.println(k + ": " + v));

    }
}