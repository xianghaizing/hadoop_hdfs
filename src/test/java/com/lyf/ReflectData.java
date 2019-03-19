package com.lyf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Created by xlf on 2019/3/19.
 */
public class ReflectData extends Mapper<LongWritable, Text, Text, LongWritable> {


    @Test
    public void test01() {

        ReflectData st = new ReflectData();
        Class clazz = st.getClass();
        //getSuperclass()获得该类的父类
        System.out.println(clazz.getSuperclass());
        //getGenericSuperclass()获得带有泛型的父类
        //Type是 Java 编程语言中所有类型的公共高级接口。它们包括原始类型、参数化类型、数组类型、类型变量和基本类型。
        Type type = clazz.getGenericSuperclass();
        System.out.println(type);
        //ParameterizedType参数化类型，即泛型
        ParameterizedType p = (ParameterizedType) type;
        //getActualTypeArguments获取参数化类型的数组，泛型可能有多个
        Class c = (Class) p.getActualTypeArguments()[0];
        System.out.println(c);

    }

}
