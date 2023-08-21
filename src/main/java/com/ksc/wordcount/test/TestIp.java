package com.ksc.wordcount.test;

import java.net.InetAddress;

public class TestIp {
    public static void main(String[] args) {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            System.out.println("Local IP Address : " + inetAddress.getHostAddress());
        } catch (Exception e) {
            e.printStackTrace();
        }
        String shuffle=System.getProperty("java.io.tmpdir")+"/shuffle";
        System.out.println(shuffle);
    }

}
