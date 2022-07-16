package com.aofei.obs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import com.obs.services.ObsClient;
import com.obs.services.model.PutObjectResult;

public class PutFiles {

    private static String fileDir = "/mnt/kettle/photos/female";

    private static String endPoint = "sa.com";
    private static String ak = "A";
    private static String sk = "G";

    private static String bucketName;
    private static int maxValue;
    private static int startValue;
    private static int counter;
    private static FileWriter fileWriter;
    private static ObsClient obsClient;
    private static String sexFlag;

    public static class Task implements Callable<String> {
        public File file;
        public Task(File file){
            this.file = file;
        }

        public String call() throws Exception {
            try {
                if ( null == obsClient) {
                    debug(" start create obs client");
                    debug("ak="+ak);
                    debug("sk="+sk);
                    debug("endPoint="+endPoint);
                    obsClient = new ObsClient(ak, sk, endPoint);
                }
                String fileName = file.getName();
                String fileSuf = "";
                if(fileName.indexOf(".") != -1) {
                    fileSuf = fileName.substring(fileName.indexOf("."));
                    if (".jpg".equals(fileSuf.toLowerCase()) ||".png".equals(fileSuf.toLowerCase())||".jpeg".equals(fileSuf.toLowerCase()) ) {
                        String objectKey = null;
                        if(null == sexFlag || sexFlag.length() == 0) {
                            objectKey = fileName.substring(0, 1) + "_" + getNextValue() + fileSuf;
                        }else {
                            objectKey = sexFlag + "_" + getNextValue() + fileSuf;
                        }
                        if (maxValue == 0) {
                            if(null == sexFlag || sexFlag.length() == 0) {
                                objectKey = fileName.substring(0, 1) + "_" + generateRandomStr(13) + fileSuf;
                            }  else {
                                objectKey = sexFlag + "_" + generateRandomStr(13) + fileSuf;
                            }
                        }
                        debug(" start put");
                        debug("objectKey="+objectKey);
                        debug("bucketName="+bucketName);
                        debug("file="+file.getAbsolutePath());
                        PutObjectResult result = obsClient.putObject(bucketName, objectKey, file);
//                        debug(objectKey+" Success!");
                        save2File(objectKey);
                        //debug(" start close obs client");
                        //obsClient.close();
                    }
                }
            } catch (Exception e){
                e.printStackTrace();
                System.exit(-1);
            }

            String tid = String.valueOf(Thread.currentThread().getId());
//            Thread.sleep(50);
            return tid;

        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int copyCount = 1;
        fileDir = "E:\\felix\\demos\\test\\src\\main\\resources\\obsPutFileDir";

        if (args.length == 4){
            copyCount = Integer.parseInt(args[0]);
            fileDir = args[1];
            bucketName = args[2];
            int threadNum = Integer.parseInt(args[3]);
            //putFiles(copyCount,fileDir,bucketName,threadNum);
        }
        else if(args.length == 5){
            copyCount = Integer.parseInt(args[0]);
            fileDir = args[1];
            int start = Integer.parseInt(args[2]);
            int max = Integer.parseInt(args[3]);
            int threadNum = Integer.parseInt(args[4]);
            putFiles(copyCount,fileDir,start,max,threadNum);
        }
         else if(args.length == 6){
            String ak_= args[0];
            String sk_= args[1];
            String endPoint_= args[2];
            String bucketName_= args[3];
            String middle_ffix_url = args[4];
            fileDir = args[5];
            putFiles(ak_, sk_, endPoint_, bucketName_,middle_ffix_url,fileDir);
        }
        else if(args.length == 9){
            String ak_= args[0];
            String sk_= args[1];
            String endPoint_= args[2];
            String bucketName_= args[3];
            copyCount = Integer.parseInt(args[4]);
            fileDir = args[5];
            int start = Integer.parseInt(args[6]);
            int max = Integer.parseInt(args[7]);
            int threadNum = Integer.parseInt(args[8]);
            putFiles(ak_, sk_, endPoint_, bucketName_,
                    copyCount, fileDir, start, max, threadNum);
        }
        else if(args.length == 10){
            String ak_= args[0];
            String sk_= args[1];
            String endPoint_= args[2];
            String bucketName_= args[3];
            copyCount = Integer.parseInt(args[4]);
            fileDir = args[5];
            int start = Integer.parseInt(args[6]);
            int max = Integer.parseInt(args[7]);
            int threadNum = Integer.parseInt(args[8]);
            String sexFlag_ = args[9];
            putFiles(ak_, sk_, endPoint_, bucketName_,
                    copyCount, fileDir, start, max, threadNum,sexFlag_);
        }
    }

    private static void putFiles(String ak, String sk, String endPoint, String bucketName, String middle_ffix_url, String fileDir) {
        if ( null == obsClient) {
            debug(" start create obs client");
            debug("ak="+ak);
            debug("sk="+sk);
            debug("endPoint="+endPoint);
            obsClient = new ObsClient(ak, sk, endPoint);
        }

        File[] files = new File(fileDir).listFiles();
        System.out.println("total files:"+files.length);
        for(int i=0; i<files.length;i++)
        {
            File file = files[i];
            String filename = file.getName();
            if(filename.indexOf("_")==-1)
            {
                System.out.println("_ not found in "+filename);
                continue;
            }

            int startIndex = filename.indexOf("_")+1;
            int endIndex = filename.indexOf("_")+19;
            String id=filename.substring(startIndex,endIndex);
            String fileSuf = filename.substring(filename.indexOf("."));
            String id_suffix = id.substring(12,18);
            String objectKey;
            int sexFlag = new Integer(id.substring(16,17)).intValue();
            String sexFlagStr;
            if(sexFlag % 2==0)
                sexFlagStr = "2";
            else
                sexFlagStr = "1";
            objectKey =  sexFlagStr+ "_" + middle_ffix_url+"_" + id_suffix+fileSuf;
            System.out.println(objectKey);
            PutObjectResult result = obsClient.putObject(bucketName, objectKey, file);
        }
    }

//    /**
//     * ??÷?±?????????obs
//     * @param copyCount ??÷???????????
//     * @param fileDir ????÷???????????
//     * @param bucketn obs??????
//     * @param threadNum ??????????
//     * @throws InterruptedException
//     * @throws ExecutionException
//     */
//    public static void putFiles(int copyCount,String fileDir,String bucketn,int threadNum) throws InterruptedException, ExecutionException{
//        long st=System.currentTimeMillis();
//        System.out.println("start putFiles copyCount="+copyCount+",fileDir="+fileDir+",bucketn="+bucketn);
//        bucketName  = bucketn;
//        if (threadNum == 0){
//            threadNum = 8;
//        }
//        File[] files = new File(fileDir).listFiles();
//        System.out.println("total files:"+files.length);
//        List<Future<String>> results = new ArrayList<Future<String>>();
//        ExecutorService es = Executors.newFixedThreadPool(threadNum);
//
//        for(int i=0; i<files.length;i++)
//            for(int j=0; j<copyCount;j++) {
//                results.add(es.submit(new Task(files[i])));
//            }
//        es.shutdown();
//        while(true) {
//            try {
//                if (es.isTerminated()) {
//                    if(obsClient!=null)
//                        obsClient.close();
//                    long cost = System.currentTimeMillis() - st;
//                    System.out.println("put files cost=" + cost + "ms");
//                    if(fileWriter!=null)
//                        fileWriter.close();
//                    break;
//                }
//                Thread.sleep(1000);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        System.out.println("end putFiles");
//    }

    /**
     * ??÷?±?????????obs
     * @param copyCount ??÷???????????
     * @param fileDir ????÷???????????
     * @param bucketn obs??????
     * @param start obs key÷?????????????
     * @param max obs key÷?????????????
     * @param threadNum ??????????
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void putFiles(int copyCount,String fileDir,String bucketn,int start,int max,int threadNum) throws InterruptedException, ExecutionException{
        long st=System.currentTimeMillis();
        System.out.println("begin: copyCount="+copyCount+",fileDir="+fileDir+",bucketn="+bucketn+",start="+start+",max="+max+",threadNum="+threadNum);
        bucketName  = bucketn;
        startValue =start;
        maxValue= max;
        if (threadNum == 0){
            threadNum = 8;
        }
        File[] files = new File(fileDir).listFiles();
        System.out.println("total files:"+files.length);
        List<Future<String>> results = new ArrayList<Future<String>>();
        ExecutorService es = Executors.newFixedThreadPool(threadNum);

        for(int i=0; i<files.length;i++)
            for(int j=0; j<copyCount;j++) {
                results.add(es.submit(new Task(files[i])));
            }
        results.add(es.submit(new Task(files[0])));
//        es.shutdown();
//        while(true) {
//            try {
//                if (es.isTerminated()) {
//                    if (null != obsClient) obsClient.close();
//                    long cost = System.currentTimeMillis() - st;
//                    System.out.println("put files cost=" + cost + "ms");
//                    if (null != fileWriter) fileWriter.close();
//                    break;
//                }
//                Thread.sleep(1000);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
        System.out.println("end putFiles");
    }

    public static void putFiles(int copyCount,String fileDir,int start,int max,int threadNum) throws InterruptedException, ExecutionException{
        long st=System.currentTimeMillis();


        String bucketn = null;

        ResourceBundle resourceBundle = null;
        try{
            String proFilePath = System.getProperty("user.home") + "/.kettle/kettle.properties";
            InputStream in = new BufferedInputStream(new FileInputStream(proFilePath));
            resourceBundle = new PropertyResourceBundle(in);
            bucketn = resourceBundle.getString("bucketName");
            ak = resourceBundle.getString("ak");
            sk = resourceBundle.getString("sk");
            endPoint = resourceBundle.getString("endPoint");

        }catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println("begin: copyCount="+copyCount+",fileDir="+fileDir+",bucketn="+bucketn+",start="+start+",max="+max+",threadNum="+threadNum);
        bucketName  = bucketn;
        startValue =start;
        maxValue= max;
        if (threadNum == 0){
            threadNum = 8;
        }
        File[] files = new File(fileDir).listFiles();
        System.out.println("total files:"+files.length);
        List<Future<String>> results = new ArrayList<Future<String>>();
        ExecutorService es = Executors.newFixedThreadPool(threadNum);

        for(int i=0; i<files.length;i++)
            for(int j=0; j<copyCount;j++) {
                results.add(es.submit(new Task(files[i])));
            }
        results.add(es.submit(new Task(files[0])));
        System.out.println("end putFiles");
    }
    /**
     *
     * @param ak_
     * @param sk_
     * @param endPoint_
     * @param bucketName_
     * @param copyCount
     * @param fileDir
     * @param start
     * @param max
     * @param threadNum
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void putFiles(String ak_,String sk_,String endPoint_,String bucketName_,
                                int copyCount,String fileDir,int start,int max,int threadNum) throws InterruptedException, ExecutionException{
        long st=System.currentTimeMillis();
        String bucketn = null;
        ResourceBundle resourceBundle = null;
        try{
            String proFilePath = System.getProperty("user.home") + "/.kettle/kettle.properties";
            InputStream in = new BufferedInputStream(new FileInputStream(proFilePath));
            resourceBundle = new PropertyResourceBundle(in);
            if(null == ak_ || ak_.length() == 0)  {
                ak = resourceBundle.getString("ak");
                System.out.println(" load parameters from kettle.propers file ak");
                save2File(" load parameters from kettle.propers file ak");
            }else{
                ak = ak_;
            }
            if(null == sk_ || sk_.length() == 0)  {
                sk = resourceBundle.getString("sk");
                System.out.println(" load parameters from kettle.propers file sk");
                save2File(" load parameters from kettle.propers file sk");
            }else{
                sk = sk_;
            }
            if(null == endPoint_ || endPoint_.length() == 0)  {
                endPoint = resourceBundle.getString("endPoint");
                System.out.println(" load parameters from kettle.propers file endPoint_");
                save2File(" load parameters from kettle.propers file endPoint");
            }else{
                endPoint = endPoint_;
            }
            if(null == bucketName_ || bucketName_.length() == 0) {
                bucketn = resourceBundle.getString("bucketName");
                System.out.println(" load parameters from kettle.propers file bucketName");
                save2File(" load parameters from kettle.propers file bucketName");
            }else {
                bucketn = bucketName_;
            }

        }catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println("begin: " +
                " ak="+ak+",sk="+sk+",endPoint="+endPoint+",bucketn="+bucketn+
                ",copyCount="+copyCount+",fileDir="+fileDir+",bucketn="+bucketn+
                ",start="+start+",max="+max+",threadNum="+threadNum
        );
        bucketName  = bucketn;
        startValue =start;
        maxValue= max;
        if (threadNum == 0){
            threadNum = 8;
        }
        File[] files = new File(fileDir).listFiles();
        System.out.println("total files:"+files.length);
        List<Future<String>> results = new ArrayList<Future<String>>();
        ExecutorService es = Executors.newFixedThreadPool(threadNum);

        for(int i=0; i<files.length;i++)
            for(int j=0; j<copyCount;j++) {
                results.add(es.submit(new Task(files[i])));
            }
        results.add(es.submit(new Task(files[0])));
        System.out.println("end putFiles");
    }

    public static void putFiles(String ak_,String sk_,String endPoint_,String bucketName_,
                                int copyCount,String fileDir,int start,int max,int threadNum,String sexFlag_) throws InterruptedException, ExecutionException{
        sexFlag = sexFlag_;
        System.out.println("sexFlag:0-man,1-feman");
        putFiles( ak_, sk_, endPoint_, bucketName_,
                copyCount, fileDir, start, max, threadNum);
    }
    /**
     * ??????÷????????÷?????????÷?????÷
     *
     * @param len ?§??
     * @return
     */
    public static String generateRandomStr(int len) {
        //?÷?????¨????????????????
        String generateSource = "0123456789abcdefghigklmnopqrstuvwxyz";
        String rtnStr = "";
        for (int i = 0; i < len; i++) {
            //?????????????±???÷???¨???????°?????÷??
            String nowStr = String.valueOf(generateSource.charAt((int) Math.floor(Math.random() * generateSource.length())));
            rtnStr += nowStr;
            generateSource = generateSource.replaceAll(nowStr, "");
        }
        return rtnStr;
    }
    public static synchronized int getNextValue()
    {
        if(counter>maxValue)
            counter=0;
        return startValue+counter++;
    }

    public static synchronized  void save2File(String f)
    {
        if(fileWriter==null)
        {
            try {
                File file = new File("./logs");
                if (!file.exists()) file.mkdirs();
                fileWriter=new FileWriter("./logs/log"+generateRandomStr(6)+".txt",true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            fileWriter.write(f);
            fileWriter.write(System.getProperty("line.separator"));
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ;
    }

    public static void debug (String s)
    {
        if(isDebug && s!=null)
            System.out.println(s);
    }

    static boolean isDebug=false;
}

