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
            if( response.isEmpty()){
                return "";
            }
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
            if(response != 200){
                return "";
            }
            //System.out.println("Status Code: "+ response );
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

        if(to.equalsIgnoreCase(from)){
            return 1.0;
        }

        Date d = new Date();
        try {
            d = dateToday.parse(date);
        }catch(Exception e){
            System.out.println("Something went wrong:" + d.toString());
        }
        Date goodDate = getWorkingDay(d);
        String workingDay = dateToday.format(goodDate);
        String dateNow = workingDay.replaceAll("/", "-");
        //if (mapRate != null) return mapRate;
        String url = "http://api.nbp.pl/api/exchangerates/rates/a/" + to + "/"+ dateNow + "/?format=json";
        //System.out.println(url);


        try {
//            Double val = rateMap.get("Poland" + delimiter + from + delimiter + to + delimiter + dateNow);
//            if(val != null){
//                return val;
//            }
            String jsonStr = getHTML(url);
            if (jsonStr.isEmpty()){
                return 0.0;
            }
            CharSequence notfound = "404";
            if(jsonStr.contains(notfound)){
                return 0.0;

            }
            //System.out.println(jsonStr);
            JSONObject jsonObj = new JSONObject(jsonStr);
            String rates = jsonObj.getString("rates");
            rates = rates.replaceAll("\\[", "").replaceAll("\\]","");
            JSONObject jsonObj1 = new JSONObject(rates);
            double n = Double.parseDouble(jsonObj1.getString("mid"));
            double inverse = 1/n;
            double returnVal = (double)Math.round(inverse * 10000d)/ 10000d;
            rateMap.put("Poland" + delimiter + from + delimiter + to + delimiter + dateNow, returnVal);
            return returnVal;

        } catch (Exception e) {

            e.printStackTrace();
        }
        double rate = 0.0;

        return rate;
    }

    public static double currXRatePolandNationalBank(String from, String to){
        from = "PLN"; //zloty
        //Date date = new Date();
        if(to.equalsIgnoreCase(from)){
            return 1.0;
        }
        Date date = new Date();
        Date goodDate = getWorkingDay(date);
        String workingDay = dateToday.format(goodDate);
        String dateNow = workingDay.replaceAll("/", "-");

        String url = "http://api.nbp.pl/api/exchangerates/rates/a/" + to + "/"+ dateNow + "/?format=json";

        try {

            //caching
            Double val = rateMap.get("Poland" + delimiter + from + delimiter + to + delimiter + dateNow);
            if(val != null){
                return val;
            }

            //send request
            String jsonStr = getHTML(url);

            //2 valid request checks
            if (jsonStr.isEmpty()){
                return 0.0;
            }

            CharSequence notfound = "404";
            if(jsonStr.contains(notfound)){
                return 0.0;

            }
            JSONObject jsonObj = new JSONObject(jsonStr);
            String rates = jsonObj.getString("rates");
            rates = rates.replaceAll("\\[", "").replaceAll("\\]","");
            JSONObject jsonObj1 = new JSONObject(rates);
            double n = Double.parseDouble(jsonObj1.getString("mid"));
            double inverse = 1/n;
            double returnVal = (double)Math.round(inverse * 10000d)/ 10000d;
            rateMap.put("Poland" + delimiter + from + delimiter + to + delimiter + dateNow, returnVal);
            return returnVal;


        } catch (Exception e) {
            e.printStackTrace();
        }
        double rate = 0.0;

        return rate;
    }

    public static double currXRateEuropeanNationalBank(String date, String from, String to){
        /* date parameter must be in format XXXX(year)-XX(month)-XX(day) */
        if(to.equalsIgnoreCase(from)){
            return 1.0;
        }

        Date d = new Date();
        try {
            d = dateToday.parse(date);
        }catch(Exception e){
            System.out.println("Something went wrong:" + d.toString());
        }
        Date goodDate = getWorkingDay(d);
        String workingDay = dateToday.format(goodDate);
        String dateNow = workingDay.replaceAll("/", "-");
        //System.out.println("dateNow: " + dateNow);
        //if (mapRate != null) return mapRate;
        String url = "https://sdw-wsrest.ecb.europa.eu/service/data/EXR/D." + to + ".EUR.SP00.A?startPeriod=" + dateNow + "&endPeriod=" + dateNow;
        String s = getHTTPS(url);
        if (s.isEmpty()){
            return 0.0;
        }
        //System.out.println("s"+ s);
        try {
            Double val = rateMap.get("Europe" + delimiter + from + delimiter + to + delimiter + dateNow);
            if(val != null){
                return val;
            }
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
            rateMap.put("Europe" + delimiter + from + delimiter + to + delimiter + dateNow, rate3);
            return  rate3;


        } catch (Exception e) {
            e.printStackTrace();
        }

        double rate = 0.0;
        return rate;
    }



    public static double currXRateEuropeanNationalBank(String from, String to){
        /* date parameter must be in format XXXX(year)-XX(month)-XX(day) */
        if(to.equalsIgnoreCase(from)){
            return 1.0;
        }

        Date date = new Date();
        Date goodDate = getWorkingDay(date);
        String workingDay = dateToday.format(goodDate);
        String dateNow = workingDay.replaceAll("/", "-");

        String url = "https://sdw-wsrest.ecb.europa.eu/service/data/EXR/D." + to + ".EUR.SP00.A?startPeriod=" + dateNow + "&endPeriod=" + dateNow;
        System.out.println(url);
        String s = getHTTPS(url);
        if (s.isEmpty()){
            return 0.0;
        }
        try {
            Double val = rateMap.get("Europe" + delimiter + from + delimiter + to + delimiter + dateNow);
            if(val != null){
                return val;
            }
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
            rateMap.put("Europe" + delimiter + from + delimiter + to + delimiter + dateNow, rate3);
            return  rate3;


        } catch (Exception e){
            e.printStackTrace();

        }

        double rate = 0.0;
        return rate;
    }



    public static double getFixerRate(String from, String to){

        if(to.equalsIgnoreCase(from)){
            return 1.0;
        }
        Date date = new Date();
        Date goodDate = getWorkingDay(date);
        String workingDay = dateToday.format(goodDate);
        String dateNow = workingDay.replaceAll("/", "-");

        String url = "https://api.fixer.io/" + dateNow + "?base=" + from;
        //System.out.println(url);

        try {
            Double val = rateMap.get("Fixer" + delimiter + from + delimiter + to + delimiter + dateNow);
            if(val != null){
                return val;
            }
            String jsonStr = getHTML(url);
            //System.out.println(jsonStr);
            if (jsonStr.isEmpty()){
                return 0.0;
            }
            JSONObject jsonObj = new JSONObject(jsonStr);
            String rates = jsonObj.getString("rates");
            rates = rates.replaceAll("\\[", "").replaceAll("\\]","");
            JSONObject jsonObj1 = new JSONObject(rates);
            double n = Double.parseDouble(jsonObj1.getString(to));
            rateMap.put("Fixer" + delimiter + from + delimiter + to + delimiter + dateNow, n);
            return n;

        } catch (Exception e) {
            e.printStackTrace();
        }
        double rate = 0.0;
        return rate;
    }

    public static double getFixerRate(String date, String from, String to){

        if(to.equalsIgnoreCase(from)){
            return 1.0;
        }

        Date d = new Date();
        try {
            d = dateToday.parse(date);
        }catch(Exception e){
            System.out.println("Something went wrong:" + d.toString());
        }
        Date goodDate = getWorkingDay(d);
        String workingDay = dateToday.format(goodDate);
        String dateNow = workingDay.replaceAll("/", "-");

        String url = "https://api.fixer.io/" + dateNow + "?base=" + from;
        //System.out.println(url);

        try {
            Double val = rateMap.get("Fixer" + delimiter + from + delimiter + to + delimiter + dateNow);
            if(val != null){
                return val;
            }
            String jsonStr = getHTML(url);

            if (jsonStr.isEmpty()){
                return 0.0;
            }
            JSONObject jsonObj = new JSONObject(jsonStr);
            String rates = jsonObj.getString("rates");
            rates = rates.replaceAll("\\[", "").replaceAll("\\]","");
            JSONObject jsonObj1 = new JSONObject(rates);
            double n = Double.parseDouble(jsonObj1.getString(to));
            rateMap.put("Fixer" + delimiter + from + delimiter + to + delimiter + dateNow, n);
            return n;

        } catch (Exception e) {
            e.printStackTrace();
        }
        double rate = 0.0;
        return rate;
    }
    public static Date getPreviousWorkingDay(Date date) {
        //deprecated method
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        int dayOfWeek;
        do {
            cal.add(Calendar.DAY_OF_MONTH, -1);
            dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        } while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY);

        return cal.getTime();
    }

    public static Date getWorkingDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        int dayOfWeek;
        dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);

        while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY){
            cal.add(Calendar.DAY_OF_MONTH, -1);
            dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        }

        return cal.getTime();
    }
    public Double evaluate(Text date, Text txt1, Text txt2, Text txt3) {
        if (txt1 == null || txt2 == null) {
            return 0.0;
        }
        String provider = txt3.toString();
        double rate;

        if(provider.toLowerCase().equals("default")) {
            if (date == null || date.toString().isEmpty()) {
                rate = getFixerRate(txt1.toString(), txt2.toString());
                return rate;
            }
            rate = getFixerRate(date.toString(), txt1.toString(), txt2.toString());
            return rate;
        }


        if(provider.toLowerCase().equals("poland")) {
            if (date == null || date.toString().isEmpty()) {
                rate = currXRatePolandNationalBank(txt1.toString(), txt2.toString());
                return rate;
            }
            rate = currXRatePolandNationalBank(date.toString(), txt1.toString(), txt2.toString());
            return rate;
        }if (provider.toLowerCase().equals("europe")){
            if (date == null || date.toString().isEmpty()) {
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
        return new CurrXRate().evaluate(date,txt1,txt2,new Text("default"));
    }



    public static void main(String[] args) {

//        long startTime = System.currentTimeMillis();

//
        for (int i = 0; i <=15000; i ++) {
            System.out.println(i + "Poland:" + currXRateEuropeanNationalBank("2014-03-10", "EUR", "PLN"));
        }
        System.out.println("done");


//
//        long endTime = System.currentTimeMillis();
//
//        System.out.println("That took " + (endTime - startTime) + " milliseconds");
//
//        //System.out.println(getFixerRate("2015-09-11", "EUR" , "HUF" ));
//        //System.out.println(getFixerRate("2015-09-12", "EUR" , "HUF" ));
//        //System.out.println(getFixerRate("2015-09-10", "EUR" , "HUF" ));
//        //System.out.println(getFixerRate("2015-03-24", "EUR" , "HUF" ));
//        //System.out.println(getFixerRate("2015-09-14", "EUR" , "PLN" ));
//
//
//        long startTime2 = System.currentTimeMillis();
//
//
        //System.out.println("poland: " + currXRatePolandNationalBank("2016-01-06", "", "RON"));
//
//        long endTime2 = System.currentTimeMillis();
//
//        System.out.println("That took " + (endTime2 - startTime2) + " milliseconds");
//
//        long startTime3 = System.currentTimeMillis();
//
//
//        System.out.println("europe: " + currXRateEuropeanNationalBank("2018-01-21", "", "PLN"));
//
//        long endTime3 = System.currentTimeMillis();
//
//        System.out.println("That took " + (endTime3 - startTime3) + " milliseconds");
//
//
//        System.out.println("EUR 1-24-2014: " + currXRatePolandNationalBank("2014-05-01", "", "EUR"));
//        System.out.println("EUR 1-24-2014: " + currXRatePolandNationalBank("2014-04-21", "", "CHF"));
//        System.out.println("test: " + getFixerRate("2014-03-10", "EUR", "HUF"));


        /*
        double euronational3 = currXRatePolandNationalBank("2014-01-06", "", "EUR");
        System.out.println("eur to pln 2017-08-30: " + euronational3);
        System.out.println("EUR 1-24-2014: " + currXRatePolandNationalBank("2014-04-21", "", "EUR"));
        System.out.println("EUR 1-6-2014: " + currXRatePolandNationalBank("2014-01-06", "", "EUR"));
        System.out.println("EUR 1-10-2014: " + currXRatePolandNationalBank("2014-01-10", "", "EUR"));
        System.out.println("EUR 1-17-2014: " + currXRatePolandNationalBank("2014-01-21", "", "EUR"));
        System.out.println("BGN 08-04-2015: " + currXRatePolandNationalBank("2015-08-04", "", "BGN"));
        System.out.println("EUR 1-17-2014: "+  currXRatePolandNationalBank("2014-01-17", "", "EUR"));
        System.out.println("EUR 1-17-2014: "+  currXRatePolandNationalBank("2014-01-17", "", "EUR"));
        System.out.println("BGN 2-02-2014: " + currXRatePolandNationalBank("2014-02-02", "", "BGN"));
        System.out.println("BGN 2-03-2014: " + currXRatePolandNationalBank("2014-02-03", "", "BGN"));
        System.out.println("BGN 2-08-2014: " + currXRatePolandNationalBank("2017-08-03", "", "EUR"));
        System.out.println("HRK: " + currXRatePolandNationalBank("2014-02-20", "", "HRK"));
        System.out.println("Failed Rate: "+currXRatePolandNationalBank("2014-01-06", "", "EUR"));*/
        //broken api calls http://api.nbp.pl/api/exchangerates/rates/a/EUR/2015-01-06/?format=json
        //valid api call http://api.nbp.pl/api/exchangerates/rates/a/EUR/2015-01-07/?format=json
//        for(int i = 0; i < 1215; i ++){
//            double euronational3 = currXRatePolandNationalBank("2017-08-30", "", "EUR");
//            System.out.println("eur to pln 2017-08-30: " + euronational3);
//        }

//        Date d = new Date();
//        try {
//            d = dateToday.parse("2015-01-06");
//        }catch(Exception e){
//            System.out.println("Something went wrong:" + d.toString());
//        }
//        System.out.println("get previous working day: "+getWorkingDay(d));

//        Date date2 = new Date();
//        try {
//            date2 = dateToday.parse("2015-08-01");
//            System.out.println(date2.toString());
//        }catch(Exception e){
//            System.out.println("Something went wrong:" + date2.toString());
//        }
//        System.out.println("get working day: "+getWorkingDay((date2)));



    }
}