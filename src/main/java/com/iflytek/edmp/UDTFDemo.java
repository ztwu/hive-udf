package com.iflytek.edmp;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * hive自定义函数udtf，主要处理一对多数据处理
 * @author ztwu2
 */
public class UDTFDemo extends GenericUDTF{

	private static final Logger logger = LoggerFactory.getLogger(UDTFDemo.class.getName());

	/**
	 * 返回UDTF的返回行的信息（返回个数，类型）
	 */
	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		ArrayList<String> fieldNames = new ArrayList<String>();  
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();  
        fieldNames.add("name");
        fieldNames.add("age");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
	}
	
	@Override
	public void process(Object[] args) throws HiveException {
		String inputStr = args[0].toString();

		logger.info("传入参数:"+inputStr);

		String[] result = new String[2];
		if(StringUtils.isNotBlank(inputStr)){
			try{
				JSONArray jsonArray = JSONArray.parseArray(inputStr);
				if(jsonArray!=null&&jsonArray.size()>0){
					for(int i=0;i<jsonArray.size();i++){
						JSONObject object = jsonArray.getJSONObject(i);
						if(object!=null){
							result[0] = object.getString("name");
							result[1] = object.getString("age");
							logger.info("返回第一列数据:"+result[0]);
							logger.info("返回第二列数据:"+result[1]);
							forward(result);
						}
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
	}

}
