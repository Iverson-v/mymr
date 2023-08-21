package com.ksc.wordcount.driver;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.datasourceapi.FileFormat;
import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.datasourceapi.UnsplitFileFormat;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.*;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.reduce.ReduceFunction;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

public class WordCountDriver {

    public static void main(String[] args) {
        //配置driver
        DriverEnv.host= "127.0.0.1";
        DriverEnv.port = 4040;
//        String inputPath = "/tmp/input";
//        String outputPath = "/tmp/output";
        String inputPath = "E:/temp/input";
        String outputPath = "E:/temp/output";
        String applicationId = "wordcount_001";//这里就一个job，直接写死
        int reduceTaskNum = 2;//分区数量，影响shuflle文件生成的个数，每个executor生成两个shuffle文件

        //1.切分splitsplit，这个UnsplitFileFormat是每个文件切一个split。
        FileFormat fileFormat = new UnsplitFileFormat();
        PartionFile[]  partionFiles = fileFormat.getSplits(inputPath, 1000);//size可以指定切多少个split

        //获得taskManager，用来管理任务的类
        TaskManager taskManager = DriverEnv.taskManager;

        //2.启动driver端的akka服务
        ActorSystem executorSystem = DriverSystem.getDriverSystem();
        ActorRef driverActorRef = executorSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());

        //3.map阶段
        int mapStageId = 0 ;
        //添加stageId和任务的映射，TaskManager的注册的方法。
        taskManager.registerBlockingQueue(mapStageId, new LinkedBlockingQueue());
        //map需要做的工作
        for (PartionFile partionFile : partionFiles) {
            MapFunction wordCountMapFunction = new MapFunction<String, KeyValue>() {
                //MAP要做的事
                @Override
                public Stream<KeyValue> map(Stream<String> stream) {
                    //todo 学生实现 定义maptask处理数据的规则
                    return stream.flatMap(line->Stream.of(line.split("\\s+"))).
                            map(word->new KeyValue(word,1));
                }
            };
            //封装map任务
            MapTaskContext mapTaskContext = new MapTaskContext(applicationId, "stage_"+mapStageId, taskManager.generateTaskId(),
                    partionFile.getPartionId(), partionFile, fileFormat.createReader(), reduceTaskNum, wordCountMapFunction);

            //Manager添加map任务
            taskManager.addTaskContext(mapStageId,mapTaskContext);
        }

        //提交stageId
        //4.提交这个任务，其中会分配一个executor去执行这个任务。通过rpc发送给一个executor一个任务
        DriverEnv.taskScheduler.submitTask(mapStageId);

        //5.等待所有executor执行结束，只有所有map任务完成finish状态才会往下走。
        DriverEnv.taskScheduler.waitStageFinish(mapStageId);


        //4.reduce阶段
        int reduceStageId = 1 ;
        //添加stageId和任务的映射，TaskManager的注册的方法。
        taskManager.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        //reduce需要做的事，两次循环，两个reduceTaskNum表示两个机器去执行reduce操作，也表示每个reduce之后的shulle写生成两个shuffle文件
        for(int i = 0; i < reduceTaskNum; i++){
            ShuffleBlockId[] stageShuffleIds = taskManager.getStageShuffleIdByReduceId(mapStageId, i);
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {

                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                    HashMap<String, Integer> map = new HashMap<>();
                    //todo 学生实现 定义reducetask处理数据的规则
                    stream.forEach(e->{
                        String key=e.getKey();
                        Integer value=e.getValue();
                        if (map.containsKey(key)){
                            map.put(key,value+map.get(key));
                        }else map.put(key,value);

                    });


                    return map.entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue()));
                }
            };
            PartionWriter partionWriter = fileFormat.createWriter(outputPath, i);
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + reduceStageId,
                    taskManager.generateTaskId(), i, stageShuffleIds, reduceFunction, partionWriter);

            //Manager添加reduce任务
            taskManager.addTaskContext(reduceStageId, reduceTaskContext);
        }

        DriverEnv.taskScheduler.submitTask(reduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(reduceStageId);
        System.out.println("job finished");


    }
}
