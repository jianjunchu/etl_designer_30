package com.aofei.obs;

import com.obs.services.ObsClient;
import com.obs.services.model.PutObjectResult;
import com.obs.services.model.ObsObject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.concurrent.*;


import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class ReadFiles {

    private static String fileDir = "";

    private static String endPoint = "";
    private static String ak = "";
    private static String sk = "";

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
        if(args.length == 5){
            String ak_= args[0];
            String sk_= args[1];
            String endPoint= args[2];
            String bucketName = args[3];
            String objectKey=args[4];
            try {
                readFile(ak, sk, endPoint, bucketName, objectKey);
            }catch(Exception ex){ex.printStackTrace();}
        }
    }


    public static void readFile(String ak,String sk,String endPoint,String bucketName,String objectKey) throws Exception
    {
        if(objectKey==null || objectKey.length()==0)
        {
            throw new Exception("url not set");
        }
        try {
            if ( null == obsClient) {
                debug(" start read obs client");
                obsClient = new ObsClient(ak, sk, endPoint);
            }
            if(objectKey.indexOf("*") != -1) {
                    ObsObject obsObject = obsClient.getObject(bucketName, objectKey, null);
                ReadableByteChannel rchannel = Channels.newChannel(obsObject.getObjectContent());

                ByteBuffer buffer = ByteBuffer.allocate(4096);
                WritableByteChannel wchannel = Channels.newChannel(new FileOutputStream(new File(objectKey)));

                while (rchannel.read(buffer) != -1)
                {
                    buffer.flip();
                    wchannel.write(buffer);
                    buffer.clear();
                }
                rchannel.close();
                wchannel.close();
                }

        } catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }
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
     * …˙≥…≤ª÷ÿ∏¥ÀÊª˙◊÷∑˚¥Æ∞¸¿®◊÷ƒ∏ ˝◊÷
     *
     * @param len ≥§∂»
     * @return
     */
    public static String generateRandomStr(int len) {
        //◊÷∑˚‘¥£¨ø…“‘∏˘æ›–Ë“™…æºı
        String generateSource = "0123456789abcdefghigklmnopqrstuvwxyz";
        String rtnStr = "";
        for (int i = 0; i < len; i++) {
            //—≠ª∑ÀÊª˙ªÒµ√µ±¥Œ◊÷∑˚£¨≤¢“∆◊ﬂ—°≥ˆµƒ◊÷∑˚
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

    static boolean isDebug=true;
}

