package com.plf.kettle.base;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.trans.TransMeta;

import java.io.FileInputStream;


public class ParseKettle {
    public static void main(String[] args) throws Exception {
        KettleEnvironment.init();
        String fileName = "C:\\Users\\Breeze\\Desktop\\1.ktr";

        TransMeta meta = new TransMeta(new FileInputStream(fileName),
                null,
                false,
                null,
                null);
        // 获取链路
        System.out.println(meta.getTransHops());

        //System.out.println(meta.getName());

        //获取step xml
        //System.out.println(meta.findStep("pre").getXML());
    }
}
