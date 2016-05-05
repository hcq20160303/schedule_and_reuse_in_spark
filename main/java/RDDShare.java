
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.util.Pair;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by hcq on 16-4-1.
 */
public class RDDShare {

    private static final int LENGTHOFQUEUE = 10;
    private static final Map<String, Integer>  TRANSFORMATION_PRIORITY = new HashMap<String, Integer>();
    static {
        TRANSFORMATION_PRIORITY.put("P", 100);
        TRANSFORMATION_PRIORITY.put("GB", 99);
        TRANSFORMATION_PRIORITY.put("J", 98);
        TRANSFORMATION_PRIORITY.put("F", 97);
        TRANSFORMATION_PRIORITY.put("S", 1);
    }

    private static Set<CacheMetaData> repository = new TreeSet<CacheMetaData>(new Comparator<CacheMetaData>() {
        public int compare(CacheMetaData o1, CacheMetaData o2) {
            /**
             * 排序规则：
             * 1. dag树的深度越深越靠前
             * 2. “加载数据”操作符（Scan）越多，则越靠前
             * 3. 操作符P > GB > J > F
             * 为什么需要排序？是为了保证第一次匹配成功的dag就是最大匹配
             */
            if ( o1.dag.head.depth > o2.dag.head.depth){
                return -1;
            }else if ( o1.dag.head.depth < o2.dag.head.depth) {
                return 1;
            }else{
                List<String> o1inputFilenames = o1.dag.head.inputFilename;
                List<String> o2inputFilenames = o2.dag.head.inputFilename;
                if ( o1inputFilenames.size() > o2inputFilenames.size() ){
                    return -1;
                }else if ( o1inputFilenames.size() < o2inputFilenames.size() ){
                    return 1;
                }else {
                    int compare = 0;
                    // 比较输入文件，按文件名排序
                    for ( int i=0; i<o1inputFilenames.size(); i++){
                        if ( (compare = o1inputFilenames.get(i).compareTo(o2inputFilenames.get(i))) != 0 ){
                            return -compare;
                        }
                    }
                    /**
                     * bug7: 需要按操作符排序
                     */
                    List<String> o1allTransformation = o1.dag.head.allTransformation;
                    List<String> o2allTransformation = o2.dag.head.allTransformation;
                    // 按操作符优先级排序
                    for ( int i=0; i<o1allTransformation.size(); i++){
                        if ( (compare = TRANSFORMATION_PRIORITY.get(o1allTransformation.get(i)) - TRANSFORMATION_PRIORITY.get(o2allTransformation.get(i)) ) != 0 ){
                            return -compare;
                        }
                    }
                    return 0;
                }
            }
        }
    });

    private static final List<String> CACHE_TRANSFORMATION = Arrays.asList("P", "GB", "F", "J");

    public static void main(String args[]){

        List<List<String>> dags = new ArrayList<List<String>>(LENGTHOFQUEUE);
        readDagsFromFile(dags);
        List<DAG> daglist = initDAGList(dags);
        int dagListSize = daglist.size();
        int i = 0;
        while ( i < dagListSize ){
            // 重用匹配及改写
            DAG dag = daglist.get(i++);
            // 打印
            String dagstring = javaObjectToJson(dag);
            System.out.println(i + ": " + dagstring);

            List<Pair<RDD, RDD>> nodeList = dagMatcherAndRewriter(dag);
            // 打印
            for (Pair<RDD, RDD> pair : nodeList){
                String rddkey = javaObjectToJson(pair.getKey());
                String rddvalue = javaObjectToJson(pair.getValue());
                System.out.println("rddkey: " + rddkey+"\nrddvalue: "+rddvalue);
            }
            // 挑选缓存的子DAG
            List<DAG> afterCacheDAG = new ArrayList<DAG>();
            getCacheRDD(nodeList, afterCacheDAG);
            daglist.addAll(afterCacheDAG);

            // 计算改写后的代价
            // ...
        }
        // 调度
        // ...
    }

    /**
     *  匹配及改写函数：该函数将一个输入的DAG和缓存当中的所有DAG进行匹配找到可重用的缓存并改写当前的DAG
     */
    public static List<Pair<RDD, RDD>> dagMatcherAndRewriter(DAG dag){
        // 按深度遍历的顺序得到DAG图的各个节点
        List<Integer> indexOfDagScan = new ArrayList<Integer>();
        List<Pair<RDD, RDD>> nodeList = new ArrayList<Pair<RDD, RDD>>();
        deepTraversalDag(null, dag.head, nodeList, indexOfDagScan);
        if ( repository.size() != 0 ){
            /**
             * 将输入dag和仓库一一进行匹配
             */
            Iterator<CacheMetaData> repositoryIterator = repository.iterator();
            while ( repositoryIterator.hasNext() ){
                CacheMetaData cache = repositoryIterator.next();
                // 按深度遍历的顺序得到DAG图的各个节点
                List<Integer> indexOfCacheDagScan = new ArrayList<Integer>();
                List<Pair<RDD, RDD>> cacheNodeList = new ArrayList<Pair<RDD, RDD>>();
                deepTraversalDag(null, cache.dag.head, cacheNodeList, indexOfCacheDagScan);
                // 测试代码
                System.out.println("两个DAG按深度遍历顺序打印节点");
                System.out.print("depth: " + dag.head.depth+"\t");
                for ( int num=0; num<nodeList.size(); num++){
                    System.out.print("data: "+nodeList.get(num).getKey().getData()+"---tran: "+nodeList.get(num).getKey().getTransformation()+"\t");
                }
                System.out.print("\ncachedepth: " + cache.dag.head.depth +"\t");
                for ( int num=0; num<cacheNodeList.size(); num++){
                    System.out.print("data: " + cacheNodeList.get(num).getKey().getData() + "---tran: " + cacheNodeList.get(num).getKey().getTransformation() + "\t");
                }
                System.out.println();

                if ( dag.head.depth >= cache.dag.head.depth && indexOfDagScan.size() >= indexOfCacheDagScan.size()) {
                    /**
                     * 将cache和DAG中的每个Load操作符进行比较
                     */
                    for ( int idOfDagScan : indexOfDagScan ) {
                        /**
                         * Matcher
                         */
                        int index = 0;  // cache中Scan操作的位置
                        /**
                         * bug9: 对于dag中有多个输入的情况，当有一个输入匹配成功，那么后一个输入在dag中的位置则会改变
                         * 因此，变量indexOfDagScan不是固定的，需要根据匹配情况更改.
                         */
                        int indexOfdag = idOfDagScan;  // dag中Scan操作的位置（可能有多个Scan操作）
                        while (index < cacheNodeList.size()) {
                            if (cacheNodeList.get(index).getKey().myEquals(nodeList.get(indexOfdag).getKey())) {
                                index++;
                                indexOfdag++;
                            } else {
                                break;
                            }
                        }
                        /**
                         * Rewriter
                         */
                        if (index == cacheNodeList.size()) {   // 完全匹配则改写DAG
                            RDD rewriter = new RDD("{"+cache.outputFilename+"}", "S", cache);
                            RDD parent = nodeList.get(index - 1).getValue();
                            if (parent.left.myEquals(nodeList.get(index - 1).getKey())) {
                                // 改写左子树
                                parent.left = rewriter;
                            } else {
                                // 改写右子树
                                parent.right = rewriter;
                            }
                            // 将替换的子图从list中删除，同时增加新的子图
                            for ( int remove=idOfDagScan; remove<indexOfdag; remove++){
                                /**
                                 * bug6: List每remove一个，后面的元素就会往前摞，
                                 * 因此，不能使用nodeList.remove(remove);
                                 * 应该使用nodeList.remove(idOfDagScan);
                                 */
                                nodeList.remove(idOfDagScan);
                            }
                            nodeList.add(idOfDagScan, new Pair<RDD, RDD>(rewriter, parent));
                        }
                    }
                }
            }
        }
        return nodeList;
    }

    /**
     * 缓存挑选函数：该函数从输入的DAG当中选择需要缓存的子DAG
     * @param nodesList
     */
    public static void getCacheRDD( List<Pair<RDD, RDD>> nodesList, List<DAG> afterCacheDAG){

        for( Pair<RDD, RDD> rddPair : nodesList ){
            if ( CACHE_TRANSFORMATION.contains(rddPair.getKey().transformation) ){
                RDD node = rddPair.getKey();
                RDD parent = rddPair.getValue();
                /**
                 * bug1: 如果根节点需要保存结果，那么需要注意rddPair.getValue()容易发生空指针异常
                 */
                if ( parent == null ) {
                    break;
                }
                DAG candidatecacheDAG = new DAG( node, true );
                // 将candidatecacheDAG复制一份
                String outputFile = "cache"+System.currentTimeMillis();
                String jsondag = javaObjectToJson(candidatecacheDAG);
                DAG candidatecacheDAGcopy = (DAG)jsonToJavaObject(jsondag, DAG.class);
                // 查找该DAG中从缓存读取数据的RDD并用缓存DAG的head节点替换该RDD
                recoverDAG(null, candidatecacheDAGcopy.head);
                CacheMetaData addCache = new CacheMetaData(candidatecacheDAGcopy, outputFile);
                repository.add(addCache);
                // 拆分旧DAG
                RDD scan = new RDD(outputFile, "S", addCache);
                if (parent.left.myEquals(node)) {
                    parent.left = scan;
                } else {
                    parent.right = scan;
                }
                afterCacheDAG.add(candidatecacheDAG);
            }
        }
    }

    /**
     * 查找该DAG中从缓存读取数据的RDD并用缓存DAG的head节点替换该RDD
     * @param parent
     * @param head
     */
    private static void recoverDAG(RDD parent, RDD head) {
        if ( head == null ){
            return;
        }
        if ( head.left == null && head.right == null && head.fromCache ){
            if ( parent.left.myEquals(head) ){
                parent.left = head.whichCache.dag.head;
            }else{
                parent.right = head.whichCache.dag.head;
            }
            return;
        }
        recoverDAG(head, head.left);
        recoverDAG(head, head.right);
    }

    /**
     *  缓存管理函数：该函数完成缓存的管理工作，当出现以下情况之一触发该操作：
     *  1） 缓存总大小超过设定阈值；
     *  2） 缓存超过设定时间未更新；
     *  3） 缓存中的某个DAG的输入被删除或者被修改。
     */
    public static void cacheManage(){

    }

    /**
     * 深度遍历DAG，并按遍历顺序将节点及其父节点存放到headList变量当中
     * @param head
     * @param headList
     * @param indexOfScan
     */
    public static void deepTraversalDag(RDD parent, RDD head, List<Pair<RDD, RDD>> headList, List<Integer> indexOfScan){
        if ( head == null ){
            return;
        }
        /**
         * bug8: 为节点的深度赋值
         */
        if ( parent != null ){
            head.depth = parent.depth - 1;
        }
        if ( head.left != null ){
            deepTraversalDag(head, head.left, headList, indexOfScan);
        }
        if ( head.right != null ){
            deepTraversalDag(head, head.right, headList, indexOfScan);
        }

        Pair headAndParent = new Pair<RDD, RDD>(head, parent);
        /**
         * 判断RDD的操作是否是表扫描或者读取外部数据
         */
        headList.add(headAndParent);
        if ( head.transformation.equals("S") ){
            indexOfScan.add( headList.indexOf(headAndParent));
            head.inputFilename.add(head.data);
        }
        /**
         * bug2:根节点的allTransformation没有赋值
         */
        head.allTransformation.add(head.transformation);
        // 为父节点增加数据
        if ( parent != null ){
            parent.inputFilename.addAll(head.inputFilename);
            /**
             * bug3:parent.allTransformation没有将子节点的allTransformation加入
             */
            parent.allTransformation.addAll(head.allTransformation);
        }
    }

    /**
     * 求给定Dag的深度
     * @param head：Dag的头结点
     * @return ： Dag的深度
     */
    public static int getDepthOfDag( RDD head ){
        if ( head == null ){
            return 0;
        }
        return Math.max(getDepthOfDag(head.left)+1, getDepthOfDag(head.right)+1);
    }

    private static String javaObjectToJson(Object dag){
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(dag);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return json;
    }

    private static Object jsonToJavaObject(String json, Class base){
        ObjectMapper mapper = new ObjectMapper();
        Object dag = null;
        try {
            dag = mapper.readValue(json, base);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dag;
    }

    /**
     * 初始化一个DAG队列
     * @param dags：队列的数据保存在该变量当中
     */
    public static List<DAG> initDAGList(List<List<String>> dags){
        System.out.println("从String List变换为DAG List");
        List<DAG> dagList = new LinkedList<DAG>();
        for ( int i=0; i<dags.size(); i++){
            int numOfnodes = dags.get(i).size();
            List<RDD> rddList = new ArrayList<RDD>(numOfnodes);
            for ( int num=0; num<numOfnodes; num++){
                RDD rdd = new RDD();
                rddList.add(rdd);
            }
            DAG dag = new DAG();
            // 层次遍历以初始化DAG
            for ( int j=1; j<=Math.ceil(Math.log(numOfnodes+1)/Math.log(2)); j++){
                for ( int num=(int)Math.pow(2, j-1); num<=Math.pow(2, j)-1; num++){
                    if ( num > numOfnodes ){
                        break;
                    }
                    // 为该节点赋值
                    String data = dags.get(i).get(num-1);
                    if ( data.equals("null") ) {
                        rddList.get(num - 1).data = "null";
                        rddList.get(num - 1).transformation = "null";
                    }else{
                        String dt = "{" + data.split("\\{")[1];
                        String trans = data.split("\\{")[0];
                        System.out.println("j: "+j+"num: "+num+"dt: "+dt+"---"+trans);
                        rddList.get(num - 1).data = dt;
                        rddList.get(num - 1).transformation = trans;
                    }
                    // 找到父节点
                    if ( j != 1 && !rddList.get(num-1).data.equals("null") ){   // 非根节点并非空
                        if (  num % 2 == 0 ){
                            rddList.get(num/2-1).left = rddList.get(num-1);
                        }else {
                            rddList.get(num/2-1).right = rddList.get(num-1);
                        }
                    }
                }
            }
            dag.head = rddList.get(0);
            dag.head.depth = (int)(Math.ceil(Math.log(numOfnodes+1)/Math.log(2)));
            dagList.add(dag);
        }
        return dagList;
    }

    /**
     * 从文件中读取dags的数据
     * @param dags: 数据保存在该变量当中
     */
    public static void readDagsFromFile(List<List<String>> dags){
        System.out.println("从文件中读取数据");
        File dagsFile = new File("/home/hcq/software/experiment-in-our-papers/restore/src/main/resources/dags.txt");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(dagsFile));
            String tmpString = null;
            while( (tmpString = reader.readLine()) != null ){
                List<String> dag = Arrays.asList(tmpString.split("    "));
                System.out.println(dag.toString());
                dags.add(dag);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if ( reader != null ){
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class RDD {
        public RDD(){
            inputFilename = new ArrayList<String>();
            allTransformation = new ArrayList<String>();
        }

        public RDD(String data, String transformation, CacheMetaData whichCache){
            inputFilename = new ArrayList<String>();
            allTransformation = new ArrayList<String>();

            this.data = data;
            this.transformation = transformation;
            this.fromCache = true;
            this.whichCache = whichCache;
        }

        String data;            // RDD的输入
        String transformation;  // RDD经过的变换操作
        /**
         * bug4:leaves变量不需要
         */
//        List<RDD> leaves;       // 以该RDD为根节点的子DAG的叶子节点


        int cost;                // rdd的估计执行代价
        boolean candidateCache;  // 是否是需要缓存而生成的新DAG的finalRdd
        List<String> inputFilename;       // 以该RDD为根节点的子DAG的输入
        List<String> allTransformation;   // 以该RDD为根节点的子DAG中所有RDD执行的transformation操作

        public int getDepth() {
            return depth;
        }

        public void setDepth(int depth) {
            this.depth = depth;
        }

        /**
         * bug8: 增加以RDD为根节点的子DAG的深度
         */
        int depth;                   //以RDD为根节点的子DAG的深度
        boolean fromCache;           // 该RDD的输入是否来自于Repository中的缓存
        CacheMetaData whichCache;    // 如果输入来自于Repository，记录下该缓存

        RDD left;
        RDD right;

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public String getTransformation() {
            return transformation;
        }

        public void setTransformation(String transformation) {
            this.transformation = transformation;
        }

        public List<String> getInputFilename() {
            return inputFilename;
        }

        public void setInputFilename(List<String> inputFilename) {
            this.inputFilename = inputFilename;
        }

        public List<String> getAllTransformation() {
            return allTransformation;
        }

        public void setAllTransformation(List<String> allTransformation) {
            this.allTransformation = allTransformation;
        }

        public boolean isFromCache() {
            return fromCache;
        }

        public void setFromCache(boolean fromCache) {
            this.fromCache = fromCache;
        }

        public CacheMetaData getWhichCache() {
            return whichCache;
        }

        public void setWhichCache(CacheMetaData whichCache) {
            this.whichCache = whichCache;
        }

        public RDD getLeft() {
            return left;
        }

        public void setLeft(RDD left) {
            this.left = left;
        }

        public RDD getRight() {
            return right;
        }

        public void setRight(RDD right) {
            this.right = right;
        }

        public boolean myEquals(RDD other){
            if ( other.data.equals(this.data) && other.transformation.equals(this.transformation) ){
                return true;
            }
            return false;
        }
    }

    static class CacheMetaData{
        public CacheMetaData(){}

        public CacheMetaData( DAG dag, String outputFilename ){
            this.dag = dag;
            this.outputFilename = outputFilename;
        }
        DAG dag;                     // 需要缓存的DAG
        String outputFilename;       // dag输出结果的文件名
        Statistics statistic;        // dag的统计信息

        public DAG getDag() {
            return dag;
        }

        public void setDag(DAG dag) {
            this.dag = dag;
        }

        public String getOutputFilename() {
            return outputFilename;
        }

        public void setOutputFilename(String outputFilename) {
            this.outputFilename = outputFilename;
        }

        public Statistics getStatistic() {
            return statistic;
        }

        public void setStatistic(Statistics statistic) {
            this.statistic = statistic;
        }
    }

    class Statistics{
        double sizoOfInputData;  // dag输入数据的大小
        double sizoOfOutputData; // dag输出结果的大小
        double exeTimeOfDag;     // dag执行的时间

        public double getSizoOfInputData() {
            return sizoOfInputData;
        }

        public void setSizoOfInputData(double sizoOfInputData) {
            this.sizoOfInputData = sizoOfInputData;
        }

        public double getSizoOfOutputData() {
            return sizoOfOutputData;
        }

        public void setSizoOfOutputData(double sizoOfOutputData) {
            this.sizoOfOutputData = sizoOfOutputData;
        }

        public double getExeTimeOfDag() {
            return exeTimeOfDag;
        }

        public void setExeTimeOfDag(double exeTimeOfDag) {
            this.exeTimeOfDag = exeTimeOfDag;
        }
    }
}
