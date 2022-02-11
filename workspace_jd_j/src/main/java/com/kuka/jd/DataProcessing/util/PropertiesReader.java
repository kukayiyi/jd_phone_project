package com.kuka.jd.DataProcessing.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

// 此类用来读取connection.properties中的数据
public class PropertiesReader {
    private final Properties prop;
    public PropertiesReader() throws IOException {
        this.prop = new Properties();
        this.prop.load(new FileInputStream(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("connection.properties")).getPath()));

    }

    public String getValue(String key){
        return this.prop.getProperty(key);
    }


}
