package ch.ethz.system.mt.tpch.verify;

import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class DataGenDataCheck {

    HashMap<String, HashMap<String, String>> orderMap = new HashMap<>();
    HashMap<String, HashMap<String, HashSet<String>>> lineItemMap = new HashMap<>();
    HashMap<String, HashSet<String>> customerMap = new HashMap<>();
    HashMap<String, HashSet<String>> supplierMap = new HashMap<>();

    @BeforeSuite
    public void init() throws IOException {
        init_orderMap();
        init_lineItemMap();
        init_custMap();
        init_supplierMap();
    }

    public void init_orderMap() throws IOException {
        Path fileName = Paths.get("output/orders.tbl");
        InputStream in = Files.newInputStream(fileName);
        BufferedReader bf = new BufferedReader(new InputStreamReader(in));
        String line;
        HashMap<String, String> mediator = new HashMap<>();

        String currentTid = "1";
        while ((line = bf.readLine()) != null) {
            StringTokenizer tokenizer = new StringTokenizer(line, "|");
            int count = 0;

            String tid = null, orderid= null, custid = null;
            while (tokenizer.hasMoreTokens()) {
                count ++;
                switch (count) {
                    case 1:
                        tid = tokenizer.nextToken();
                        break;
                    case 2:
                        orderid = tokenizer.nextToken();
                        break;
                    case 3:
                        custid = tokenizer.nextToken();
                        break;
                }

                if (count ==3) {
                    if (!currentTid.equals(tid)) {
                        orderMap.put(currentTid, mediator); // save current tenant data
                        currentTid = tid; // move on to the next tenant
                        mediator = new HashMap<>();
                    }
                    mediator.put(orderid, custid);
                    break;
                }
            }
        }
        orderMap.put(currentTid, mediator); // save the last tenant data into dataMap
        bf.close();
        in.close();
    }

    public void init_lineItemMap() throws IOException {
        Path fileName = Paths.get("output/lineitem.tbl");
        InputStream in = Files.newInputStream(fileName);
        BufferedReader bf = new BufferedReader(new InputStreamReader(in));
        String line;
        HashMap<String, HashSet<String>> mediator = new HashMap<>();
        HashSet<String> set = new HashSet<>();

        String currentTid = "1", currentOrderId = "1";
        while ((line = bf.readLine()) != null) {
            StringTokenizer tokenizer = new StringTokenizer(line, "|");
            int count = 0;

            String tid = null, orderid= null, supplierid = null;
            while (tokenizer.hasMoreTokens()) {
                count ++;
                switch (count) {
                    case 1:
                        tid = tokenizer.nextToken();
                        break;
                    case 2:
                        orderid = tokenizer.nextToken();
                        break;
                    case 3:
                        supplierid = tokenizer.nextToken();
                        break;
                }

                if (count ==3) {
                    //check if order id change
                    if (!currentOrderId.equals(orderid)) {
                        mediator.put(currentOrderId, set); // save current supplier data under same order id
                        currentOrderId = orderid; // move on to the next order id
                        set = new HashSet<>(); //clear the hash set
                    }
                    //check if tenant id change
                    if (!currentTid.equals(tid)) {
                        lineItemMap.put(currentTid, mediator); // save current tenant data
                        currentTid = tid; // move on to the next tenant
                        mediator = new HashMap<>();
                    }
                    set.add(supplierid);

                    break;
                }
            }
        }
        lineItemMap.put(currentTid, mediator); // save the last tenant data into dataMap
        bf.close();
        in.close();
    }

    public void init_custMap() throws IOException {
        Path fileName = Paths.get("output/customer.tbl");
        InputStream in = Files.newInputStream(fileName);
        BufferedReader bf = new BufferedReader(new InputStreamReader(in));
        String line;
        HashSet<String> set = new HashSet<>();

        String currentTid = "1";
        while ((line = bf.readLine()) != null) {
            StringTokenizer tokenizer = new StringTokenizer(line, "|");
            int count = 0;
            String tid = null, custid = null;
            while (tokenizer.hasMoreTokens()) {
                count ++;
                switch (count) {
                    case 1:
                        tid = tokenizer.nextToken();
                        break;
                    case 2:
                        custid = tokenizer.nextToken();
                        break;
                }

                if (count == 2) {
                    if (!currentTid.equals(tid)) {
                        customerMap.put(currentTid, set); // save current tenant data
                        currentTid = tid; // move on to the next tenant
                        set = new HashSet<>();
                    }
                    set.add(custid);
                    break;
                }
            }
        }
        customerMap.put(currentTid, set); //save the last tenant data
        bf.close();
        in.close();
    }

    public void init_supplierMap() throws IOException {
        Path fileName = Paths.get("output/supplier.tbl");
        InputStream in = Files.newInputStream(fileName);
        BufferedReader bf = new BufferedReader(new InputStreamReader(in));
        String line;
        HashSet<String> set = new HashSet<>();

        String currentTid = "1";
        while ((line = bf.readLine()) != null) {
            StringTokenizer tokenizer = new StringTokenizer(line, "|");
            int count = 0;
            String tid = null, supplierId = null;
            while (tokenizer.hasMoreTokens()) {
                count ++;
                switch (count) {
                    case 1:
                        tid = tokenizer.nextToken();
                        break;
                    case 2:
                        supplierId = tokenizer.nextToken();
                        break;
                }

                if (count == 2) {
                    if (!currentTid.equals(tid)) {
                        supplierMap.put(currentTid, set); // save current tenant data
                        currentTid = tid; // move on to the next tenant
                        set = new HashSet<>();
                    }
                    set.add(supplierId);
                    break;
                }
            }
        }
        supplierMap.put(currentTid, set); //save the last tenant data
        bf.close();
        in.close();
    }

    @Test
    public void testMapNotEmpty() {
        Assert.assertNotNull(orderMap);
        Assert.assertNotNull(customerMap);
        Assert.assertNotNull(supplierMap);
        Assert.assertNotNull(lineItemMap);
    }

    @Test(dependsOnMethods = {"testMapNotEmpty"})
    public void testOrderTableDataMatch() {
        boolean dataCorrect = true;
        int errorCount = 0;
        for(String tid: orderMap.keySet()) {
            for(String orderKey: orderMap.get(tid).keySet()) {
                String custKey = orderMap.get(tid).get(orderKey);
                if (!customerIdExists(tid, custKey)) {
                    dataCorrect = false;
                    errorCount++;
                    //System.out.println("Tenant id: " + tid + " with CustomerKey: " + custKey + " cannot be found");
                }
            }
        }
        if (dataCorrect) {
            System.out.println("##################");
            System.out.println("Check complete, all data in Order Table exists and are correct");
            System.out.println("##################");
        } else {
            System.out.println("##################");
            System.out.println(errorCount + " of data cannot match in Order Table");
            System.out.println("##################");
        }
        Assert.assertTrue(dataCorrect);
    }

    public boolean customerIdExists(String tid, String id) {
        if (tid != null && id != null) {
            return customerMap.get(tid).contains(id);
        } else return false;
    }

    @Test(dependsOnMethods = {"testMapNotEmpty"})
    public void testLineItemTableDataMatch() {
        boolean dataCorrect = true;
        int errorCount = 0;
        for(String tid: lineItemMap.keySet()) {
            for(String orderKey: lineItemMap.get(tid).keySet()) {
                if (!orderMap.get(tid).keySet().contains(orderKey)) {
                    dataCorrect = false;
                    errorCount++;
                    System.out.println("Tenant id: " + tid + " with OrderKey: " + orderKey + " cannot be found");
                }
                for(String supplierKey: lineItemMap.get(tid).get(orderKey)) {
                    if (!supplierMap.get(tid).contains(supplierKey)) {
                        dataCorrect = false;
                        errorCount++;
                        //System.out.println("Tenant id: " + tid + " with OrderKey: " + orderKey + " with Supplier Key: " + supplierKey + " cannot be found");
                    }
                }
            }
        }
        if (dataCorrect) {
            System.out.println("##################");
            System.out.println("Check complete, all data in LineItem Table exists and are correct");
            System.out.println("##################");
        } else {
            System.out.println("##################");
            System.out.println(errorCount + " of data cannot match in LineItem Table");
            System.out.println("##################");
        }
        Assert.assertTrue(dataCorrect);
    }
}