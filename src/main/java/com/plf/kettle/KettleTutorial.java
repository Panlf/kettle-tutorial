package com.plf.kettle;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entries.success.JobEntrySuccess;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.sql.ExecSQLMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassDef;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassMeta;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 调用Kettle的API生成交换或者作业
 * @author panlf
 * @date 2020/12/8
 */
public class KettleTutorial {

    @Test
    public void startJobTest() throws KettleException {
        KettleEnvironment.init();

        String fileName = "src/main/resources/etl/update_insert_jobs.kjb";

        Document document = XMLHandler.loadXMLFile(fileName);
        Node root = document.getDocumentElement();
        JobMeta jobMeta = new JobMeta();
        jobMeta.loadXML(root,fileName, null, null, true,  null);


        Job job = new Job(null, jobMeta);
        // 向Job 脚本传递参数，脚本中获取参数值：${参数名}
        // job.setVariable(paraname, paravalue);
        job.start();
        job.waitUntilFinished();
        if (job.getErrors() > 0) {
            System.out.println("decompress fail!");
        }
    }

    /**
     * 生成Kettle的作业
     */
    @Test
    public void generateCreateJobTest(){
        try{
            KettleEnvironment.init();
            KettleTutorial test =new KettleTutorial();
            JobMeta jobMeta = test.generateJobs();
            String jobsXml = jobMeta.getXML();
            String jobsName = "src/main/resources/etl/update_insert_jobs.kjb";
            File file = new File(jobsName);
            FileUtils.writeStringToFile(file, jobsXml, "UTF-8");
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * 启动Kettle交换任务
     */
    @Test
    public void startTrans(){
        try {
            KettleEnvironment.init();

            String fileName = "src/main/resources/etl/update_insert_trans.ktr";

            Document document = XMLHandler.loadXMLFile(fileName);
            Node root = document.getDocumentElement();
            TransMeta transMeta = new TransMeta();
            transMeta.loadXML(root,fileName, null, null, true, new Variables(), null);

            //调用trans
            Trans trans = new Trans(transMeta);
            trans.prepareExecution(null);
            trans.startThreads();
            trans.waitUntilFinished();
            if (trans.getErrors() != 0) {
                System.out.println("Error");
            }

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    /**
     *  生成带Java脚本的交换
     */
    @Test
    public void generateJavaCodeTransTest(){
        try {
            KettleEnvironment.init();
            KettleTutorial test =new KettleTutorial();
            TransMeta transMeta = test.generateJavaCodeTrans();
            String transXml = transMeta.getXML();
            String transName = "src/main/resources/etl/update_insert_trans.ktr";
            File file = new File(transName);
            FileUtils.writeStringToFile(file, transXml, "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }


    public JobMeta generateJobs() throws KettleXMLException {
        JobMeta jobMeta = new JobMeta();
        jobMeta.setName("job_trans");
        JobEntrySpecial special = new JobEntrySpecial();
        special.setName("开始");
        special.setStart(true);
        special.setDummy(false);
        special.setRepeat(false);
        JobEntryCopy start = new JobEntryCopy(special);
        start.setDrawn();
        start.setLocation(144,160);


        JobEntryTrans trans = new JobEntryTrans();
        trans.setName("START");
        //kjb文件的当前目录
        trans.setFileName("${Internal.Entry.Current.Directory}/update_insert_trans.ktr");
        trans.setTransname("update_insert_Trans");
        JobEntryCopy transCopy = new JobEntryCopy(trans);
        transCopy.setDrawn();
        transCopy.setLocation(336,160);

        jobMeta.addJobEntry(start);
        jobMeta.addJobEntry(transCopy);
        jobMeta.addJobHop(new JobHopMeta(start,transCopy));


        JobEntrySuccess success = new JobEntrySuccess();
        success.setName("成功");
        JobEntryCopy successCopy = new JobEntryCopy(success);
        successCopy.setDrawn();
        successCopy.setLocation(512,160);
        jobMeta.addJobEntry(successCopy);
        jobMeta.addJobHop(new JobHopMeta(transCopy,successCopy));

        jobMeta.setJobstatus(0);
        return jobMeta;
    }

    /**
     * 生成表输入 -- Java代码 -- 表输出的Kettle
     * @return
     */
    public TransMeta generateJavaCodeTrans() {

        //设置serverTimeZone
        Properties properties = new Properties();
        properties.put("EXTRA_OPTION_MYSQL.serverTimezone","GMT+8");
        properties.put("EXTRA_OPTION_MYSQL.useSSL","true");

        TransMeta transMeta = new TransMeta();
        //设置转换名称
        transMeta.setName("tran_data_test");

        DatabaseMeta dataMeta =
                new DatabaseMeta("db1", "MySQL", "Native","localhost", "test", "3306", "root", "root");
        dataMeta.setAttributes(properties);
        dataMeta.setDBPort("3306");
        transMeta.addDatabase(dataMeta);
        DatabaseMeta targetMeta =
                new DatabaseMeta("db2", "MySQL", "Native","localhost", "test", "3306", "root", "root");
        targetMeta.setDBPort("3306");
        targetMeta.setAttributes(properties);
        transMeta.addDatabase(targetMeta);

        //registry是给每个步骤生成一个标识id
        PluginRegistry registry = PluginRegistry.getInstance();
        //第一个表输入步骤(TableInputMeta)
        TableInputMeta tableInput = new TableInputMeta();

        String tableInputPluginId = registry.getPluginId(StepPluginType.class, tableInput);
        //给表输入添加一个DatabaseMeta连接数据库
        DatabaseMeta database = transMeta.findDatabase("db1");
        tableInput.setDatabaseMeta(database);
        String selectSQL = "SELECT * FROM user";
        tableInput.setSQL(selectSQL);
        //添加TableInputMeta到转换中
        StepMeta tableInputMetaStep =
                new StepMeta(tableInputPluginId, "table_input",tableInput);
        //给步骤添加在spoon工具中的显示位置
        tableInputMetaStep.setDraw(true);
        tableInputMetaStep.setLocation(100, 100);
        transMeta.addStep(tableInputMetaStep);


        //StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta
        //第二个步骤
        UserDefinedJavaClassMeta classMeta = new UserDefinedJavaClassMeta();

        //Java代码的数据处理
        String javaCode = "public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException\r\n" +
                "{\r\n" +
                "   Object[] r = getRow();\r\n" +
                "   if (r == null) {\r\n" +
                "       setOutputDone();\r\n" +
                "       return false;\r\n" +
                "   }\r\n" +
                "   if (first){\r\n" +
                "       first = false;\r\n" +
                "   }\r\n" +
                "   r= createOutputRow(r, data.outputRowMeta.size());\r\n" +
                "   String test_value = get(Fields.In, \"username\").getString(r);\r\n" +
                "   String username = test_value+\"-plf\";\r\n" +
                "   get(Fields.Out, \"username\").setValue(r, username);\r\n" +
                "   putRow(data.outputRowMeta, r);\r\n" +
                "   return true;\r\n" +
                "}";

        UserDefinedJavaClassDef classDef =
                new UserDefinedJavaClassDef(UserDefinedJavaClassDef.ClassType.TRANSFORM_CLASS, "processRow", javaCode);

        List<UserDefinedJavaClassDef> definitions = Arrays.asList(classDef);

        classMeta.replaceDefinitions(definitions);

        String javaCodeMetaPluginId = registry.getPluginId(StepPluginType.class, classMeta);
        StepMeta javaCodeStep = new StepMeta(javaCodeMetaPluginId,"java_code_deal_username",classMeta);
        javaCodeStep.setDraw(true);
        javaCodeStep.setLocation(250, 100);
        transMeta.addStep(javaCodeStep);

        //添加hop把两个步骤关联起来
        transMeta.addTransHop(new TransHopMeta(tableInputMetaStep, javaCodeStep));


        //第三个个步骤插入与更新
        InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();
        String insertUpdateMetaPluginId = registry.getPluginId(StepPluginType.class, insertUpdateMeta);
        //添加数据库连接
        DatabaseMeta database_kettle = transMeta.findDatabase("db2");
        insertUpdateMeta.setDatabaseMeta(database_kettle);
        //设置操作的表
        insertUpdateMeta.setTableName("user_copy");

        //设置用来查询的关键字
        insertUpdateMeta.setKeyLookup(new String[]{"id"});
        insertUpdateMeta.setKeyStream(new String[]{"id"});
        insertUpdateMeta.setKeyStream2(new String[]{""});
        insertUpdateMeta.setKeyCondition(new String[]{"="});

        //设置要更新的字段
        String[] updateLookup = {"id","username"} ;
        String[] updateStream = {"id","username"} ;
        Boolean[] updateOrNot = {false,true};
        insertUpdateMeta.setUpdateLookup(updateLookup);
        insertUpdateMeta.setUpdateStream(updateStream);
        insertUpdateMeta.setUpdate(updateOrNot);


        //添加步骤到转换中
        StepMeta insertUpdateStep = new StepMeta(insertUpdateMetaPluginId, "insert_update", insertUpdateMeta);
        insertUpdateStep.setDraw(true);
        insertUpdateStep.setLocation(400, 100);
        transMeta.addStep(insertUpdateStep);


        //添加hop把两个步骤关联起来
        transMeta.addTransHop(new TransHopMeta(javaCodeStep, insertUpdateStep));

        return transMeta;
    }

    public TransMeta generateSQLTrans(){
        //设置serverTimeZone
        Properties properties = new Properties();
        properties.put("EXTRA_OPTION_MYSQL.serverTimezone","GMT+8");
        properties.put("EXTRA_OPTION_MYSQL.useSSL","true");

        TransMeta transMeta = new TransMeta();
        transMeta.setName("data-delete");
        DatabaseMeta dataMeta =
                new DatabaseMeta("db1", "MySQL", "Native","localhost", "test", "3600", "root", "root");
        dataMeta.setAttributes(properties);
        transMeta.addDatabase(dataMeta);

        //registry是给每个步骤生成一个标识id
        PluginRegistry registry = PluginRegistry.getInstance();


        //第一个表输入步骤(TableInputMeta)
        TableInputMeta tableInput = new TableInputMeta();
        String tableInputPluginId = registry.getPluginId(StepPluginType.class, tableInput);
        //给表输入添加一个DatabaseMeta连接数据库
        DatabaseMeta database_test = transMeta.findDatabase("db1");
        tableInput.setDatabaseMeta(database_test);
        String selectSQL = "SELECT id,id as tid from test_copy";
        tableInput.setSQL(selectSQL);
        //添加TableInputMeta到转换中
        StepMeta tableInputMetaStep =
                new StepMeta(tableInputPluginId, "table_input",tableInput);
        //给步骤添加在spoon工具中的显示位置
        tableInputMetaStep.setDraw(true);
        tableInputMetaStep.setLocation(144, 160);
        transMeta.addStep(tableInputMetaStep);

        ExecSQLMeta sqlMeta = new ExecSQLMeta();
        sqlMeta.setDatabaseMeta(database_test);
        sqlMeta.setSql("delete from test_copy where id = (select IF((SELECT id from test where id = ?),0,?))");
        sqlMeta.setArguments(new String[]{"id","tid"});
        sqlMeta.setExecutedEachInputRow(true);
        sqlMeta.setVariableReplacementActive(true);

        String sqlMetaId = registry.getPluginId(StepPluginType.class, sqlMeta);
        StepMeta sqlMetaStep =
                new StepMeta(sqlMetaId, "sql_op_delete",sqlMeta);
        //给步骤添加在spoon工具中的显示位置
        sqlMetaStep.setDraw(true);
        sqlMetaStep.setLocation(288, 160);
        transMeta.addStep(sqlMetaStep);
        //添加hop把两个步骤关联起来
        transMeta.addTransHop(new TransHopMeta(tableInputMetaStep, sqlMetaStep));
        return transMeta;
    }

}
