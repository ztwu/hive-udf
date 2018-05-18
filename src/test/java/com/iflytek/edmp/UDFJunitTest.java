package com.iflytek.edmp;

import org.junit.Test;

public class UDFJunitTest {

    @Test
    public void testUDF() throws Exception {
        String fields = "{name:'ztwu2',age:25}";
        String key = "name";
        UDFDemo udf = new UDFDemo();
        assert udf.evaluate(fields,key).equals("ztwu2");
    }

    @Test
    public void testUDTF() throws Exception {
        UDTFDemo udtf = new UDTFDemo();
        udtf.process(new String[]{"[{name:'ztwu1',age:25,sex:'男'},{name:'ztwu2',age:25,sex:'男'}]"});
    }


} 
