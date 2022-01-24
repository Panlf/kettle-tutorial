package com.plf.kettle.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.www.SlaveServerJobStatus;
import org.pentaho.di.www.SlaveServerTransStatus;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;

public class DispatchKettleWeb {

    private final String kettleXMLDirectory = "C:\\Data\\Soft\\Kettle\\kettlexml";

    private SlaveServer slaveServer = null;

    /**
     * 初始化Kettle服务器
     */
    @BeforeEach
    public void init() {
        try {
            KettleEnvironment.init();
            slaveServer = new SlaveServer();
            slaveServer.setName("slave1");
            slaveServer.setHostname("localhost");
            slaveServer.setPort("8081");
            slaveServer.setUsername("cluster");
            slaveServer.setPassword("cluster");
        }catch (Exception e){
            System.out.println("启动失败,错误是===>"+e.getMessage());
        }
    }




    /**
     * 获取作业的状态
     */
    @Test
    public void getJobStatus(){
        try {
            //只能获取运行的任务
            List<SlaveServerJobStatus> jobStatusList =  slaveServer.getStatus().getJobStatusList();

            //获取作业的状态
            jobStatusList.forEach(
                    x-> System.out.println(x.getStatusDescription())
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    /**
     * 停止作业
     */
    @Test
    public void stopJob(){
        try {
            slaveServer.stopJob("",
                    "");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除作业
     */
    @Test
    public void removeJob(){
        try {
            slaveServer.removeJob("",
                    "");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //===================================== Trans 转换 =====================================
    /**
     * 执行转换
     * @param ktrInputStream
     * @return
     * @throws Exception
     */
    public String startTrans(InputStream ktrInputStream) throws Exception {
        try {
            // 配置任务信息
            TransMeta meta = new TransMeta(ktrInputStream,
                    null,
                    false,
                    null,
                    null);
            TransExecutionConfiguration transExecutionConfiguration = new TransExecutionConfiguration();
            transExecutionConfiguration.setRemoteServer(slaveServer);// 配置远程服务
            // 需要判断是否在执行,否则每执行一次就会产生一个lastCarteObjectId
            String carteObjectId = Trans.sendToSlaveServer(meta, transExecutionConfiguration,
                    null, null);
            return carteObjectId;
        } catch (Exception e) {
            System.out.println("执行任务失败,错误是===>"+e.getMessage());
            return null;
        }
    }

    /**
     * 获取转换日志
     */
    @Test
    public void getTransLog(){
        try {
            SlaveServerTransStatus slaveServerTransStatus =  slaveServer
                    .getTransStatus("tran_data_op_test",
                            "9ebcd353-86c7-463e-8c1e-397d4a533def",
                            1);

            System.out.println(slaveServerTransStatus.getLoggingString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取Trans的状态
     */
    @Test
    public void getTransStatus(){
        try {
            List<SlaveServerTransStatus> transStatusList = slaveServer.getStatus().getTransStatusList();
            //获取转换的
            transStatusList.forEach(
                    x-> System.out.println(x.getStatusDescription())
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行Trans
     */
    @Test
    public void executeOnceTrans(){
        try {
            InputStream ktrInputStream = new FileInputStream(kettleXMLDirectory+"\\op_data_trans.ktr");
            startTrans(ktrInputStream);
        } catch (FileNotFoundException e) {
            System.out.println("读取文件失败,错误是===>"+e.getMessage());
            e.printStackTrace();
        } catch (Exception e){
            System.out.println("执行KJB文件失败,错误是===>"+e.getMessage());
        }
    }
}
