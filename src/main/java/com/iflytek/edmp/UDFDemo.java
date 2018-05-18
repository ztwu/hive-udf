package com.iflytek.edmp;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/8/25
 * Time: 16:57
 * Description hive自定义函数udf，主要处理一对一数据处理
 */

public class UDFDemo extends UDF {

    private static final Logger logger = LoggerFactory.getLogger(UDFDemo.class.getName());

    public String evaluate(String fields, String key) {
        logger.info("输入参数 fields:"+fields);
        logger.info("输入参数 key:"+key);
        if (StringUtils.isNotBlank(fields)&&StringUtils.isNotBlank(key)) {
            String result = null;
            JSONObject object = JSONObject.parseObject(fields);
            if(object!=null){
                result = object.getString(key);
                logger.info("解析json数组返回对应key值 "+key+":"+result);
            }
            return result;
        } else {
            return null;
        }
    }

}
