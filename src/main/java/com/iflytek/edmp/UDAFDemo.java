package com.iflytek.edmp;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2018/1/2
 * Time: 14:31
 * Description hive自定义函数udaf，主要处理多对一的聚合数据处理
 */

//支持长度可变的数据类型，如map，list

//主要是为了解决先聚合数据情况下，对应维度非一对一下的数据再次sum聚合的问题

//解析器和计算器。
// 解析器负责UDAF的参数检查，操作符的重载以及对于给定的一组参数类型来查找正确的计算器，建议继承AbstractGenericUDAFResolver类
//计算器实现具体的计算逻辑，需要继承GenericUDAFEvaluator抽象类。
//计算器有4种模式，由枚举类GenericUDAFEvaluator.Mode定义：
//public static enum Mode {
//    PARTIAL1, //从原始数据到部分聚合数据的过程（map阶段），将调用iterate()和terminatePartial()方法。
//    PARTIAL2, //从部分聚合数据到部分聚合数据的过程（map端的combiner阶段），将调用merge() 和terminatePartial()方法。
//    FINAL,    //从部分聚合数据到全部聚合的过程（reduce阶段），将调用merge()和 terminate()方法。
//    COMPLETE  //从原始数据直接到全部聚合的过程（表示只有map，没有reduce，map端直接出结果），将调用merge() 和 terminate()方法。
//};

//一般情况下，完整的UDAF逻辑是一个mapreduce过程
public class UDAFDemo extends AbstractGenericUDAFResolver {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(UDFDemo.class.getName());

    //解析器的类型检查确保用户传递正确的参
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly two argument is expected.");
        }

        //原始对象
        ObjectInspector key = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        ObjectInspector value = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[1]);
        if (key.getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,
                    "Argument must be PRIMITIVE, but "
                            + key.getCategory().name()
                            + " was passed.");
        }
        if (value.getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,
                    "Argument must be PRIMITIVE, but "
                            + value.getCategory().name()
                            + " was passed.");
        }

        //java基本数据类型
        PrimitiveObjectInspector keyOI = (PrimitiveObjectInspector) key;
        PrimitiveObjectInspector valueOI = (PrimitiveObjectInspector) value;
        if (keyOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentTypeException(0,
                    "Argument must be String, but "
                            + keyOI.getPrimitiveCategory().name()
                            + " was passed.");
        }
        if (valueOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT){
            throw new UDFArgumentTypeException(0,
                    "Argument must be Int, but "
                            + valueOI.getPrimitiveCategory().name()
                            + " was passed.");
        }
        return new UDAFGroupSumEvaluator();
    }

    /**
     * 实现具体的计算逻辑
     * 这里实现sum聚合函数
     */
    public static class UDAFGroupSumEvaluator extends GenericUDAFEvaluator {

        //Serde实现数据序列化和反序列化以及提供一个
        // 辅助类ObjectInspector帮助使用者访问需要序列化或者反序列化的对象

        //java基本数据类型,原始数据
        PrimitiveObjectInspector keyOI;
        PrimitiveObjectInspector valueOI;

        //在map和reduce之间有一层shuffle，中间结果由hadoop完成shuffle后也需要读取并反序列化成内部的object
        //hive会提供一个StandardStructObjectInspector给用户进行该Object的访问
        StructObjectInspector structOI;
        StructField mapField;
        MapObjectInspector mapFieldOI;

        ObjectInspector outputOI;

        @Override
        // 确定各个阶段输入输出参数的数据格式ObjectInspectors
        // 这个方法返回了UDAF的返回类型，这里确定了sum自定义函数的返回类型是Long类型
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            //如果表达式的值为假，整个程序将退出，并输出一条错误信息。如果表达式的值为真则继续执行后面的语句。
            assert (parameters.length == 2);
            super.init(m, parameters);

            //入参
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                keyOI = (PrimitiveObjectInspector) parameters[0];
                valueOI = (PrimitiveObjectInspector) parameters[1];
            } else {
                structOI = (StandardStructObjectInspector) parameters[0];
                mapField = structOI.getStructFieldRef("map");
                mapFieldOI = (MapObjectInspector) mapField.getFieldObjectInspector();
            }

            // 指定reduce输出数据格式都为Integer类型
            //出参
            if(m == Mode.FINAL) {
                outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            }else {
                // 指定其他阶段输出数据格式都为Map类型
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                ArrayList<String> fname = new ArrayList<String>();
                foi.add(ObjectInspectorFactory
                        .getStandardMapObjectInspector(
                                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                                PrimitiveObjectInspectorFactory.writableIntObjectInspector));
                fname.add("map");
                outputOI =  ObjectInspectorFactory.getStandardStructObjectInspector(
                        fname, foi);
            }
            return outputOI;
        }

        /**
         * 存储数据聚集结果的类
         */
        static class GroupSumAgg implements AggregationBuffer {
            int sum = 0;
            Map<Text,IntWritable> data = new HashMap<Text, IntWritable>();
            Map<String,Integer> result = new HashMap<String,Integer>();

            void put(Text key, IntWritable value){
                data.put(key, value);
            }

            void sumGroup(){
                Iterator<Integer> values = result.values().iterator();
                while (values.hasNext()) {
                    sum += values.next();
                }
            }

        }

        // 保存数据聚集结果的类
        // 创建新的聚合计算的需要的内存，用来存储mapper,combiner,reducer运算过程中的相加总和。
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            GroupSumAgg result = new GroupSumAgg();
            reset(result);
            return result;
        }

        // 重置聚集结果
        // mapreduce支持mapper和reducer的重用，所以为了兼容，也需要做内存的重用
        public void reset(AggregationBuffer agg) throws HiveException {
            GroupSumAgg myAgg = ((GroupSumAgg)agg);
            myAgg.data.clear();
            myAgg.result.clear();
            myAgg.sum = 0;
        }

        // map阶段，迭代处理输入sql传过来的列数据
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 2);
            if(parameters[0]!=null && parameters[1]!=null){
                GroupSumAgg myAgg = (GroupSumAgg) agg;
                Object keyP = ((PrimitiveObjectInspector) keyOI).getPrimitiveJavaObject(parameters[0]);
                Object valueP = ((PrimitiveObjectInspector) valueOI).getPrimitiveJavaObject(parameters[1]);
                myAgg.put(new Text((String)keyP),new IntWritable((Integer)valueP));
            }
        }

        // map与combiner结束返回结果，得到部分数据聚集结果
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            GroupSumAgg myAgg = (GroupSumAgg) agg;
            Map<Text,IntWritable> data = new HashMap<Text, IntWritable>(myAgg.data);
            return data;
        }

        // combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            GroupSumAgg myAgg = (GroupSumAgg) agg;
            if (partial != null) {
                Object partialObject = structOI.getStructFieldData(partial, mapField);
                //shuffle过程需要序列化和反序列化，因此数据类型实现writable
                Map<Text,IntWritable> partialSum  = (Map<Text,IntWritable>) mapFieldOI.getMap(partialObject);
                myAgg.data.putAll(partialSum);
            }
            logger.info("split:---------");
            logger.info("merge:---------");

            //key是text对象，每次new一个对象，在map的key中其实是不同的值，因此可以把数据转换
            for(Map.Entry<Text,IntWritable> entry : myAgg.data.entrySet()){
                logger.info("key:"+entry.getKey().toString());
                logger.info("value:"+entry.getValue().get());
                myAgg.result.put(entry.getKey().toString(),entry.getValue().get());
            }
            logger.info("split:---------");
        }

        // reducer阶段，输出最终结果
        public Object terminate(AggregationBuffer agg) throws HiveException {
            GroupSumAgg myAgg = (GroupSumAgg) agg;
            myAgg.sumGroup();
            return myAgg.sum;
        }
    }
}
