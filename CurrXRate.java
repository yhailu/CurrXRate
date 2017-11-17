//package com.vertex.ve.udf;

import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.*;
import org.json.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.net.HttpURLConnection;
import javax.net.ssl.HttpsURLConnection;
import java.util.Date;
import java.net.URL;
import java.io.IOException;


@Description(name = "Lookup", value = "Lookup(str,str) - Look up reference value using file table", extended = "Example: \n"
        + " SELECT lookup(column, table) FROM hive_table; ")
public class CurrXRate extends UDF {

    private static String delimiter = "|";
    private static ConcurrentHashMap<String, Double> rateMap = new ConcurrentHashMap<String, Double>();
    private static DateFormat dateToday = new SimpleDateFormat("yyyy-MM-dd");

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



    public static double currXRatePolandNationalBank(String date, String from, String to){

        from = "PLN";//Zloty

        Double mapRate = rateMap.get(from + delimiter + to);
        //if (mapRate != null) return mapRate;
        String url = "http://api.nbp.pl/api/exchangerates/rates/a/" + to + "/"+ date + "/?format=json";

        try {
            String jsonStr = getHTML(url);
            JSONObject jsonObj = new JSONObject(jsonStr);
            String rates = jsonObj.getString("rates");
            rates = rates.replaceAll("\\[", "").replaceAll("\\]","");
            JSONObject jsonObj1 = new JSONObject(rates);
            double n = Double.parseDouble(jsonObj1.getString("mid"));
            rateMap.put(from + delimiter + to, n);
            return 1/n;

        } catch (Exception e) {
            e.printStackTrace();
        }
        double rate = 0.0;

        return rate;
    }

    public static double currXRatePolandNationalBank(String from, String to){

        from = "PLN"; //zloty
        Date date = new Date();
        Double mapRate = rateMap.get(from + delimiter + to);
        //if (mapRate != null) return mapRate;
        String dateRaw = dateToday.format(date);
        String dateNow = dateRaw.replaceAll("/", "-");
        String url = "http://api.nbp.pl/api/exchangerates/rates/a/" + to + "/"+ dateNow + "/?format=json";

        try {
            String jsonStr = getHTML(url);
            JSONObject jsonObj = new JSONObject(jsonStr);
            String rates = jsonObj.getString("rates");
            rates = rates.replaceAll("\\[", "").replaceAll("\\]","");
            JSONObject jsonObj1 = new JSONObject(rates);
            double n = Double.parseDouble(jsonObj1.getString("mid"));
            rateMap.put(from + delimiter + to, n);
            return 1/n;

        } catch (Exception e) {
            e.printStackTrace();
        }
        double rate = 0.0;

        return rate;
    }

    public static double currXRateEuropeanNationalBank(String date, String from, String to){
        /* date parameter must be in format XXXX(year)-XX(month)-XX(day) */


        Double mapRate = rateMap.get(from + delimiter + to);


        Date d = new Date();
        try {
            d = dateToday.parse(date);
        }catch(Exception e){
            System.out.println("Something went wrong:" + d.toString());
        }
        Date goodDate = getPreviousWorkingDay(d);
        String workingDay = dateToday.format(goodDate);
        String dateNow = workingDay.replaceAll("/", "-");
        //System.out.println("dateNow: " + dateNow);
        //if (mapRate != null) return mapRate;
        String url = "https://sdw-wsrest.ecb.europa.eu/service/data/EXR/D." + to + ".EUR.SP00.A?startPeriod=" + dateNow + "&endPeriod=" + dateNow;
        String s = getHTTPS(url);
        try {
            String jsonStr = getHTTPS(url);
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
            return  rate3;


        } catch (Exception e) {
            e.printStackTrace();
        }

        double rate = 0.0;
        return rate;
    }



    public static double currXRateEuropeanNationalBank(String from, String to){
        /* date parameter must be in format XXXX(year)-XX(month)-XX(day) */

        Double mapRate = rateMap.get(from + delimiter + to);
        //if (mapRate != null) return mapRate;
        Date date = new Date();
        Date goodDate = getPreviousWorkingDay(date);
        String workingDay = dateToday.format(goodDate);
        String dateNow = workingDay.replaceAll("/", "-");

        String url = "https://sdw-wsrest.ecb.europa.eu/service/data/EXR/D." + to + ".EUR.SP00.A?startPeriod=" + dateNow + "&endPeriod=" + dateNow;
        String s = getHTTPS(url);
        try {
            String jsonStr = getHTTPS(url);
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
            return  rate3;


        } catch (Exception e) {
            e.printStackTrace();
        }

        double rate = 0.0;
        return rate;
    }

    public static double getFixerRate(String from, String to){
        Date date = new Date();
        Date goodDate = getPreviousWorkingDay(date);
        String workingDay = dateToday.format(goodDate);
        String dateNow = workingDay.replaceAll("/", "-");

        String url = "https://api.fixer.io/" + dateNow + "?base=" + from;

        try {
            String jsonStr = getHTML(url);
            JSONObject jsonObj = new JSONObject(jsonStr);
            String rates = jsonObj.getString("rates");
            rates = rates.replaceAll("\\[", "").replaceAll("\\]","");
            JSONObject jsonObj1 = new JSONObject(rates);
            double n = Double.parseDouble(jsonObj1.getString(to));
            rateMap.put(from + delimiter + to, n);
            return n;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0.0;
    }

    public static double getFixerRate(String date, String from, String to){
        Date d = new Date();
        try {
            d = dateToday.parse(date);
        }catch(Exception e){
            System.out.println("Something went wrong:" + d.toString());
        }
        Date goodDate = getPreviousWorkingDay(d);
        String workingDay = dateToday.format(goodDate);
        String dateNow = workingDay.replaceAll("/", "-");

        String url = "https://api.fixer.io/" + dateNow + "?base=" + from;

        try {
            String jsonStr = getHTML(url);
            JSONObject jsonObj = new JSONObject(jsonStr);
            String rates = jsonObj.getString("rates");
            rates = rates.replaceAll("\\[", "").replaceAll("\\]","");
            JSONObject jsonObj1 = new JSONObject(rates);
            double n = Double.parseDouble(jsonObj1.getString(to));
            rateMap.put(from + delimiter + to, n);
            return n;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0.0;
    }
    public static Date getPreviousWorkingDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        int dayOfWeek;
        do {
            cal.add(Calendar.DAY_OF_MONTH, -1);
            dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        } while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY);

        return cal.getTime();
    }


    public Double evaluate(Text date, Text txt1, Text txt2, Text txt3) {
        if (txt1 == null || txt2 == null) {
            return null;
        }
        String provider = txt3.toString();
        double rate;

        if(provider.toLowerCase().equals("default")) {
            if (date == null) {
                rate = getFixerRate(txt1.toString(), txt2.toString());
                return rate;
            }
            rate = getFixerRate(date.toString(), txt1.toString(), txt2.toString());
            return rate;
        }


        if(provider.toLowerCase().equals("poland")) {
            if (date == null) {
                rate = currXRatePolandNationalBank(txt1.toString(), txt2.toString());
                return rate;
            }
            rate = currXRatePolandNationalBank(date.toString(), txt1.toString(), txt2.toString());
            return rate;
        }if (provider.toLowerCase().equals("europe")){
            if (date == null) {
                rate = currXRateEuropeanNationalBank(txt1.toString(), txt2.toString());
                return rate;
            }
                rate = currXRateEuropeanNationalBank(date.toString(), txt1.toString(), txt2.toString());
                return rate;
        }

        return 0.0;
    }

    public Double evaluate(Text txt1, Text txt2) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return new CurrXRate().evaluate(new Text(dateFormat.format(new Date()).toString()), txt1, txt2, new Text("default"));

    }

    public Double evaluate(Text date, Text txt1, Text txt2) {
        return new CurrXRate().evaluate(date,txt1,txt2,txt2);
    }


    public static void main(String[] args) {


        double euronational = currXRateEuropeanNationalBank("2017-11-10"," ", "USD");
        System.out.println("euronational: " + euronational);
        double euronational2 = currXRateEuropeanNationalBank( " ", "PLN");
        System.out.println("euronational2: " + euronational2);


        double fixerRate = getFixerRate("2017-11-10", "EUR", "USD");
        System.out.println("Rate: " + fixerRate);
        double fixerRate2 = getFixerRate("USD", "EUR");
        System.out.println("Rate: " + fixerRate2);
        double poland = currXRatePolandNationalBank("2017-10-17", " ", "USD");
        System.out.println("PLN->USD:" + poland);
        double polandNoDate = currXRatePolandNationalBank(" ", "USD");
        System.out.println("No Date PLN->USD:" + polandNoDate);
        double PLNEUR = getFixerRate("EUR", "PLN");
        System.out.println("Fixer PLN->EUR:" + PLNEUR);

    }
}
