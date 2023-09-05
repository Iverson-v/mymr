package com.ksc.wordcount.driver;

import akka.actor.*;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.ksc.urltopn.thrift.UrlTopNAppRequest;
import com.ksc.urltopn.thrift.UrlTopNResult;
import com.ksc.wordcount.conf.MasterConfigReader;
import com.ksc.wordcount.datasourceapi.*;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.merge.MergeFunction;
import com.ksc.wordcount.task.merge.MergeTaskContext;
import com.ksc.wordcount.task.reduce.ReduceFunction;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;
import com.ksc.wordcount.worker.Executor;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UrlTopNDriver {
    public static boolean run(UrlTopNAppRequest urlTopNAppRequest) throws Exception {
//        //清除task内存
        DriverEnv.clear();

        String inputPath = urlTopNAppRequest.getInputPath();
        String outputPath = urlTopNAppRequest.getOuputPath();
        String applicationId = urlTopNAppRequest.applicationId;
        //分区数量，影响shuflle文件生成的个数，每个executor生成两个shuffle文件
        int reduceTaskNum = urlTopNAppRequest.getNumReduceTasks();//2
        int splitSize = urlTopNAppRequest.getSplitSize(); //1024byte
        int topN = urlTopNAppRequest.getTopN();  //10


        //1.切分splitsplit，这个UnsplitFileFormat是每个文件切一个split。
        FileFormat fileFormat = new UrlTopSplitFileFormat();
        PartionFile[]  partionFiles = fileFormat.getSplits(inputPath, splitSize);//size可以指定切多少个split

        //获得taskManager，用来管理任务的类
        TaskManager taskManager = DriverEnv.taskManager;

        //2.启动driver端的akka服务
//        ActorSystem driverSystem = DriverSystem.getDriverSystem();
//
//        ActorRef driverActorRef=null;
//        ActorSelection selection = driverSystem.actorSelection("/user/driverActor");
//        Timeout timeout = new Timeout(Duration.create(5, "seconds"));
//        Future<Object> future = Patterns.ask(selection, new Identify(1), timeout);
//        try {
//            ActorIdentity identity = (ActorIdentity) Await.result(future, timeout.duration());
//            if (identity.getRef() == null) {
//                // actor不存在，创建它
//                driverActorRef = driverSystem.actorOf(Props.create(DriverActor.class), "driverActor");
//            } else {
//                // actor已存在，使用现有的引用
//                driverActorRef = identity.getRef();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        //ActorRef driverActorRef = driverSystem.actorOf(Props.create(DriverActor.class), "driverActor");
//        System.out.println("ServerActor started at: " + driverActorRef.path().toString());


        //Executor.start();//启动executor

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
                    //todo 实现 定义maptask处理数据的规则
                    Pattern urlPattern = Pattern.compile("https?://([\\w-]+\\.)+[\\w-]+(/[\\w-./?%&=]*)?"); // 匹配URL直到不允许的字符为止

                    return stream.flatMap(line -> {
                        Matcher matcher = urlPattern.matcher(line);
                        List<String> urls = new ArrayList<>();
                        while (matcher.find()) {
                            urls.add(matcher.group());
                        }
                        return urls.stream();
                    }).map(url -> new KeyValue(url, 1));
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

        System.out.println("map阶段完成！！-----------------------------------------------------------------------");

        //6.reduce阶段
        int reduceStageId = 1 ;
        //添加stageId和任务的映射，TaskManager的注册的方法。
        taskManager.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        //reduce需要做的事，两次循环，两个reduceTaskNum表示两个机器去执行reduce操作，也表示每个reduce之后的shulle写生成两个shuffle文件
        for(int i = 0; i < reduceTaskNum; i++){
            //这里的shuffleBlockId是给netty用的，需要去请求netty服务端获取shuffle文件
            ShuffleBlockId[] stageShuffleIds = taskManager.getStageShuffleIdByReduceId(mapStageId, i);
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {

                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                    HashMap<String, Integer> map = new HashMap<>();
                    //todo 实现 定义reducetask处理数据的规则
                    stream.forEach(e->{
                        String key=e.getKey();
                        Integer value=e.getValue();
                        if (map.containsKey(key)){
                            map.put(key,value+map.get(key));
                        }else map.put(key,value);

                    });


//                    return map.entrySet().stream()
//                            .map(e -> new KeyValue(e.getKey(), e.getValue()));


                    List<KeyValue<String, Integer>> topnList = map.entrySet().stream()
                            .map(e -> new KeyValue<String, Integer>(e.getKey(), e.getValue()))
                            .sorted((kv1, kv2) -> kv2.getValue() - kv1.getValue())
                            .collect(Collectors.toList());

                    //选择前topn个。
                    List<KeyValue<String, Integer>> rankedList = new ArrayList<>();
                    //当topn小于等于0的时候直接返回空。
                    if (topN<=0)
                        return rankedList.stream();
                    //当总数量少于topn的时候全部返回
                    if (topnList.size()<=topN)
                        return topnList.stream();
                    //选出前topn个。
                    rankedList.add(topnList.get(0));
                    int rank=1;
                    for (int index = 1; index < topnList.size() ; index++) {
                        if (rank==topN)
                            break;
                        if (!Objects.equals(topnList.get(index).getValue(), topnList.get(index - 1).getValue())){
                            rank++;
                        }
                        rankedList.add(topnList.get(index));
                    }
                    return rankedList.stream();

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


        System.out.println("reduce阶段完成！！-----------------------------------------------------------------------");


        //7.merge阶段
        int mergeStageId = 2 ;
        int mergeId=reduceTaskNum;
        //添加stageId和任务的映射，TaskManager的注册的方法。
        taskManager.registerBlockingQueue(mergeStageId, new LinkedBlockingQueue());
        //reduce需要做的事，两次循环，两个reduceTaskNum表示两个机器去执行reduce操作，也表示每个reduce之后的shulle写生成两个shuffle文件

        //这里的shuffleBlockId是给netty用的，需要去请求netty服务端获取shuffle文件
        ShuffleBlockId[] stageShuffleIds = taskManager.getStageShuffleIdById(reduceStageId, 0);




        MergeFunction<String, Integer, String, Integer> mergeFunction = new MergeFunction<String, Integer, String, Integer>() {

            @Override
            public Stream<KeyValue<String, Integer>> merge(Stream<KeyValue<String, Integer>> stream) {
                //todo 实现 定义reducetask处理数据的规则

                List<KeyValue<String, Integer>> topnList = stream
                        .sorted((kv1,kv2)-> kv2.getValue()- kv1.getValue())
                        .collect(Collectors.toList());

                if (topnList.size() > topN) {

                    List<KeyValue<String, Integer>> rankedList = new ArrayList<>();
                    int rank = 1;
                    Integer lastValue = null;
                    for (KeyValue<String, Integer> kv : topnList) {
                        if (lastValue != null && !lastValue.equals(kv.getValue())) {
                            rank++;
                        }
                        if (rank > topN) break;

                        rankedList.add(kv);
                        lastValue = kv.getValue();
                    }
                    return rankedList.stream();

                }else return topnList.stream();

            }
        };
        PartionWriter partionWriter = fileFormat.createWriter(outputPath, mergeId);
        MergeTaskContext mergeTaskContext = new MergeTaskContext(applicationId, "stage_" + mergeStageId,
                taskManager.generateTaskId(), mergeId, stageShuffleIds, mergeFunction, partionWriter);

        //Manager添加merge任务
        taskManager.addTaskContext(mergeStageId, mergeTaskContext);


        DriverEnv.taskScheduler.submitTask(mergeStageId);
        DriverEnv.taskScheduler.waitStageFinish(mergeStageId);



        System.out.println("job finished！！-----------------------------------------------------------------------");
        System.out.println();


        return true;
    }
}
