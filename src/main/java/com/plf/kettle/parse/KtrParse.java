package com.plf.kettle.parse;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.fastjson.JSON;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.springframework.beans.BeanUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 解析KTR文件
 */
public class KtrParse {

    private final static SAXReader saxReader = new SAXReader();
    private final DbType dbType = JdbcConstants.MYSQL;
    private final String maxIDPattern= "select\\s+ifnull\\(max\\(.*\\),0\\)\\s+from\\s.*";
    private final String updateSync = "update\\s+.*set\\s+is_sync=1\\s+where.*";
    Pattern maxIDPatternInstance = Pattern.compile(maxIDPattern);
    Pattern updateSyncInstance = Pattern.compile(updateSync);
    @Test
    public void testSQL(){
        String text = "selEct   iFnUll(max(id),0)    from   test11";
        System.out.println(maxIDPatternInstance.matcher(text).find());

        /*
        String sql = "select a.* from test a,(select max(_id_) _id_ from " +
                " test11 where _id_>? group by xh)" +
                " b where a._id_=b._id_ order by a._id_";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        SQLStatement sqlStatement = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        sqlStatement.accept(visitor);
        visitor.getTables().forEach((k,v)->{
            System.out.println(k);
        });*/
    }

    @Test
    public void test() throws Exception {
        KettleEnvironment.init();

        String path = "C:\\Users\\Breeze\\Desktop\\5.xml";

        FileInputStream fileInputStream = new FileInputStream(path);
        byte[] bytes = cloneInputStream(fileInputStream);
        List<ChainDto> chainDtoList = getChainName(bytes);

        // System.out.println(JSON.toJSONString(chainDtoList));
        // 前置库 -> 共享 链路 解析
        //System.out.println(JSON.toJSONString(parseSwitchCaseChain(chainDtoList)));

        // 前置库  ->  绍兴开放
        System.out.println(JSON.toJSONString(parseIfMaxId(chainDtoList)));

        // 前置库 -> 开发 -> 省开发


        // System.out.println(chainDtoList);
        //System.out.println();
        //KettleChain kettleChain = new KettleChain();
        //kettleChain.setNodeName(chainDtoList.get(2).getStartName());
        //getAfterKettleChain(kettleChain,chainDtoList);
        //getBeforeKettleChain(kettleChain,chainDtoList);

        //System.out.println(JSON.toJSONString(kettleChain));
        /*getJobs(bytes).forEach(x->{
                System.out.println(parseQueryString(x).get("fileName"));
        });*/
    }

    public List<SyncChain> parseIfMaxId(List<ChainDto> chainDtoList) throws Exception {
        List<SyncChain> syncChainList = new ArrayList<>();
        for(ChainDto chainDto:chainDtoList){
            //现在一种链路是增量传输，所以会计算目标表的最大主键ID，对数据链路来说，其实这段链路无意义
            String startSql = chainDto.getStartTableInfo();
            String endSql = chainDto.getEndTableInfo();
            if(!maxIDPatternInstance.matcher(startSql).find() &&
                    !updateSyncInstance.matcher(endSql).find() ){
                SyncChain syncChain = new SyncChain();

                syncChain.setStartDataBase(chainDto.getStartDbInfo().getDatabase());
                syncChain.setStartServer(chainDto.getStartDbInfo().getServer());
                syncChain.setStartTable(String.join(",",getTableFromSql(chainDto.getStartTableInfo())));

                syncChain.setEndDataBase(chainDto.getEndDbInfo().getDatabase());
                syncChain.setEndServer(chainDto.getEndDbInfo().getServer());
                syncChain.setEndTable(String.join(",",getTableFromSql(chainDto.getStartTableInfo())));

                syncChainList.add(syncChain);
            }
        }

        return syncChainList;
    }

    public List<String> getTableFromSql(String sql){
        List<String> result = new ArrayList<>();
        try {
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
            SQLStatement sqlStatement = stmtList.get(0);
            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            sqlStatement.accept(visitor);
            visitor.getTables().forEach((k, v) -> {
                result.add(k.getName());
            });
        }catch (Exception e){
            result.add(sql);
        }
        return result;
    }

    public List<SyncChain> parseSwitchCaseChain(List<ChainDto> chainDtoList){
        List<SyncChain> syncChainList = new ArrayList<>();
        for(ChainDto chainDto:chainDtoList){
            //这边是根据现实情况 特殊处理
            if(chainDto.getStartType().equalsIgnoreCase("SwitchCase")){
                SyncChain syncChain= new SyncChain();
                String dataBase = chainDto.getEndDbInfo().getDatabase();
                String server = chainDto.getEndDbInfo().getServer();
                String table = chainDto.getEndTableInfo();
                syncChain.setEndTable(String.join(",",getTableFromSql(table)));
                syncChain.setEndServer(server);
                syncChain.setEndDataBase(dataBase);
                List<ChainDto> chainBefore = stepBefore(chainDto.getStartName(), chainDtoList);
                if(!CollectionUtils.isEmpty(chainBefore)){
                   for(ChainDto c:chainBefore){
                       SyncChain n = new SyncChain();
                       String startDataBase = c.getStartDbInfo().getDatabase();
                       String startServer = c.getStartDbInfo().getServer();
                       String startTable = c.getStartTableInfo();
                       BeanUtils.copyProperties(syncChain,n);
                       n.setStartServer(startServer);
                       n.setStartTable(String.join(",",getTableFromSql(startTable)));
                       n.setStartDataBase(startDataBase);
                       if(!isExist(syncChainList,n)){
                           syncChainList.add(n);
                       }
                   }
                }
                    //System.out.println(chainDto.getEndName());
                    //System.out.println(JSON.toJSONString(stepBefore(chainDto.getStartName(),chainDtoList)));
            }
        }
        return syncChainList;
    }



    public boolean isExist(List<SyncChain> syncChainList,SyncChain syncChain){
        for(SyncChain s:syncChainList){
            if(s.getStartTable().equalsIgnoreCase(syncChain.getStartTable()) &&
            s.getStartDataBase().equalsIgnoreCase(syncChain.getStartDataBase()) &&
            s.getStartServer().equalsIgnoreCase(syncChain.getStartServer()) &&
            s.getEndDataBase().equalsIgnoreCase(syncChain.getEndDataBase()) &&
            s.getEndServer().equalsIgnoreCase(syncChain.getEndServer()) &&
            s.getEndTable().equalsIgnoreCase(syncChain.getEndTable())){
                return true;
            }
        }
        return false;
    }


    public void getAfterKettleChain(KettleChain kettleChain, List<ChainDto> chainDtoList){
        String nodeName = kettleChain.getNodeName();
        List<KettleChain> afterChain = kettleChain.getAfterChain();
        if(afterChain==null){
            afterChain = new ArrayList<>();
            List<ChainDto> chainList =  stepAfter(nodeName,chainDtoList);
            if(CollectionUtils.isEmpty(chainList)){
                return;
            }
            for(ChainDto chainDto:chainList){
                KettleChain v = new KettleChain();
                v.setNodeName(chainDto.getEndName());
                afterChain.add(v);
            }
            kettleChain.setAfterChain(afterChain);
            getAfterKettleChain(kettleChain,chainDtoList);
        }else{
            for(KettleChain chain:afterChain){
                getAfterKettleChain(chain,chainDtoList);
            }
        }
    }

    public void getBeforeKettleChain(KettleChain kettleChain, List<ChainDto> chainDtoList){
        String nodeName = kettleChain.getNodeName();
        List<KettleChain> beforeChain = kettleChain.getBeforeChain();
        if(beforeChain==null){
            beforeChain = new ArrayList<>();
            List<ChainDto> chainList =  stepBefore(nodeName,chainDtoList);
            if(CollectionUtils.isEmpty(chainList)){
                return;
            }
            for(ChainDto chainDto:chainList){
                KettleChain v = new KettleChain();
                v.setNodeName(chainDto.getStartName());
                beforeChain.add(v);
            }
            kettleChain.setBeforeChain(beforeChain);
            getBeforeKettleChain(kettleChain,chainDtoList);
        }else{
            for(KettleChain chain:beforeChain){
                getBeforeKettleChain(chain,chainDtoList);
            }
        }
    }

    //获取后一步
    public List<ChainDto> stepAfter(String stepAfter,List<ChainDto> chainDtoList){
        List<ChainDto> result = new ArrayList<>();
        for(ChainDto chainDto:chainDtoList){
            String startName = chainDto.getStartName();
            if(stepAfter.equalsIgnoreCase(startName)){
                result.add(chainDto);
            }
        }
        return result;
    }

    //获取前一步
    public List<ChainDto> stepBefore(String stepBefore,List<ChainDto> chainDtoList){
        List<ChainDto> result = new ArrayList<>();
        for(ChainDto chainDto:chainDtoList){
            String endName = chainDto.getEndName();
            if(stepBefore.equalsIgnoreCase(endName)){
                result.add(chainDto);
            }
        }
        return result;
    }

    public static Map<String, String> parseQueryString(String urlString) {
        Map<String, String> params = new HashMap<>();
        try {
            URL url = new URL(urlString);
            String query = url.getQuery(); // 获取URL的查询字符串部分
            if (query != null) {
                for (String param : query.split("&")) {
                    String[] entry = param.split("=");
                    if (entry.length > 1) {
                        String key = entry[0];
                        String value = entry[1];
                        params.put(key, URLDecoder.decode(value, "UTF-8"));
                    }
                }
            }
        } catch (UnsupportedEncodingException | MalformedURLException e) {
            e.printStackTrace();
        }
        return params;
    }

    public static List<String> getJobs(byte[] bytes) throws Exception {
        List<String> transList = new ArrayList<>();
        InputStream inputStream = new ByteArrayInputStream(bytes);
        JobMeta meta = new JobMeta(inputStream,
                null,
                null);

        List<JobHopMeta> jobHopMetaList = meta.getJobhops();
        if (!CollectionUtils.isEmpty(jobHopMetaList)) {
            for (JobHopMeta jobHopMeta : jobHopMetaList) {
                //String fromName = new String(jobHopMeta.getFromEntry().getName().getBytes(StandardCharsets.UTF_8));
                //System.out.println(fromName);
                String fromXml = jobHopMeta.getFromEntry().getXML();
                //System.out.println(getJobTrans(fromXml));
                String result = getJobTrans(fromXml);
                if(!StringUtils.isEmpty(result) && !transList.contains(result)){
                    transList.add(result);
                }
            }
        }
        return transList;
    }

    public static String getJobTrans(String xml) throws DocumentException {
        Element root = getRootElement(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
        String type = root.elementText("type");
        String result = "";
        if (type.equalsIgnoreCase("TRANS") )  {
            result = root.elementText("filename");
        }
        return result;
    }

    /**
     * 获取链路
     */
    public static List<ChainDto> getChainName(byte[] bytes) throws Exception {
        List<ChainDto> chainDtoList = new ArrayList<>();

        Map<String, DbInfo> connectionMap = getKtrConnection(bytes);
        InputStream inputStream = new ByteArrayInputStream(bytes);
        /*String result = new BufferedReader(new InputStreamReader(inputStream))
                .lines().collect(Collectors.joining("\n"));
        System.out.println(result);*/
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
                //若获取为null 为了不影响后续使用new一个空对象进去
                chainDto.setStartDbInfo(connectionMap.get(getConnectionName(fromXml)) == null ? new DbInfo() :  connectionMap.get(getConnectionName(fromXml)));
                chainDto.setStartType(getChainType(fromXml));

                String toName = new String(transHopMeta.getToStep().getName().getBytes(StandardCharsets.UTF_8));
                String toXml = transHopMeta.getToStep().getXML();
                chainDto.setEndName(toName);
                chainDto.setEndTableInfo(getTableInfo(toXml));
                chainDto.setEndType(getChainType(toXml));
                //若获取为null 为了不影响后续使用new一个空对象进去
                chainDto.setEndDbInfo(connectionMap.get(getConnectionName(toXml)) == null ? new DbInfo() :  connectionMap.get(getConnectionName(toXml)));
                chainDtoList.add(chainDto);
            }
        }

        return chainDtoList;
    }



    //获取table名称
    public static String getChainType(String xml) throws DocumentException {
        Element root = getRootElement(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

        //TableInput  TableOutput  SwitchCase  InsertUpdate  Delete ExecSQL
        return root.elementText("type");
    }

    //获取table名称
    public static String getTableInfo(String xml) throws DocumentException {
        Element root = getRootElement(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

        //TableInput  TableOutput  SwitchCase  InsertUpdate  Delete ExecSQL
        String type = root.elementText("type");
        String result = "";

        if (type.equalsIgnoreCase("TableInput") || type.equalsIgnoreCase("ExecSQL")) {
            result = root.elementText("sql");
        } else if (type.equalsIgnoreCase("TableOutput")) {
            result = root.elementText("table");
        } else if (type.equalsIgnoreCase("InsertUpdate") || type.equalsIgnoreCase("Delete")) {
            result = root.element("lookup").elementText("table");
        } else {
            System.out.println(type);
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

    public static byte[] cloneInputStream(InputStream input) throws Exception {
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
