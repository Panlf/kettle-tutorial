package com.plf.kettle.parse;

import com.alibaba.fastjson.JSONObject;
import groovy.json.JsonOutput;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.springframework.util.CollectionUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 解析KTR文件
 */
public class KtrParse {

    private final static SAXReader saxReader = new SAXReader();

    @Test
    public void test() throws Exception {
        KettleEnvironment.init();

        String path = "C:\\Users\\Breeze\\Desktop\\1.ktr";
        FileInputStream fileInputStream = new FileInputStream(path);

        byte[] bytes = cloneInputStream(fileInputStream);

        List<ChainDto> chainDtoList = getChainName(bytes);
        System.out.println(chainDtoList);
    }

    /**
     * 获取链路
     */
    public static List<ChainDto> getChainName(byte[] bytes) throws Exception {
        List<ChainDto> chainDtoList = new ArrayList<>();

        Map<String, DbInfo> connectionMap = getKtrConnection(bytes);
        InputStream inputStream = new ByteArrayInputStream(bytes);
        TransMeta meta = new TransMeta(inputStream,
                null,
                false,
                null,
                null);
        List<TransHopMeta> transHopMetaList = meta.getTransHops();
        if (!CollectionUtils.isEmpty(transHopMetaList)) {
            for (TransHopMeta transHopMeta : transHopMetaList) {
                ChainDto chainDto = new ChainDto();

                String fromName = new String(transHopMeta.getFromStep().getName().getBytes(StandardCharsets.UTF_8));
                String fromXml = transHopMeta.getFromStep().getXML();
                chainDto.setStartName(fromName);
                chainDto.setStartTableInfo(getTableInfo(fromXml));
                chainDto.setStartDbInfo(connectionMap.get(getConnectionName(fromXml)));

                String toName = new String(transHopMeta.getToStep().getName().getBytes(StandardCharsets.UTF_8));
                String toXml = transHopMeta.getToStep().getXML();
                chainDto.setEndName(toName);
                chainDto.setEndTableInfo(getTableInfo(toXml));
                chainDto.setEndDbInfo(connectionMap.get(getConnectionName(toXml)));
                chainDtoList.add(chainDto);
            }
        }

        return chainDtoList;
    }


    //获取table名称
    public static String getTableInfo(String xml) throws DocumentException {
        Element root = getRootElement(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

        //TableInput  TableOutput  SwitchCase  InsertUpdate  Delete
        String type = root.elementText("type");
        String result = "";

        if (type.equalsIgnoreCase("TableInput")) {
            result = root.elementText("sql");
        } else if (type.equalsIgnoreCase("TableOutput")) {
            result = root.elementText("table");
        } else if (type.equalsIgnoreCase("InsertUpdate") || type.equalsIgnoreCase("Delete")) {
            result = root.element("lookup").elementText("table");
        }
        return result;
    }

    public static String getConnectionName(String xml) throws DocumentException {
        Element root = getRootElement(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
        String result;
        result = root.elementText("connection");
        return result;
    }


    public static Element getRootElement(InputStream inputStream) throws DocumentException {
        Document document = saxReader.read(inputStream);
        return document.getRootElement();
    }

    public static Map<String, DbInfo> getKtrConnection(byte[] bytes) throws Exception {
        Map<String, DbInfo> map = new HashMap<>();
        InputStream inputStream = new ByteArrayInputStream(bytes);
        Element root = getRootElement(inputStream);
        List<Element> elementList = root.elements("connection");
        for (Element e : elementList) {
            DbInfo dbInfo = new DbInfo();
            String connectionName = new String(e.elementText("name").getBytes(), StandardCharsets.UTF_8);
            String ipServer = e.elementText("server");
            String dataBase = e.elementText("database");
            String username = e.elementText("username");
            dbInfo.setName(connectionName);
            dbInfo.setServer(ipServer);
            dbInfo.setDatabase(dataBase);
            dbInfo.setUsername(username);
            map.put(connectionName, dbInfo);
        }
        return map;
    }

    private static byte[] cloneInputStream(InputStream input) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = input.read(buffer)) > -1) {
            byteArrayOutputStream.write(buffer, 0, len);
        }
        byteArrayOutputStream.flush();
        byteArrayOutputStream.close();
        input.close();
        return byteArrayOutputStream.toByteArray();
    }
}
