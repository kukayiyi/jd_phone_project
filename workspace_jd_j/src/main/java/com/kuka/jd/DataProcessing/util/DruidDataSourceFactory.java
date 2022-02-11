package com.kuka.jd.DataProcessing.util;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.ibatis.datasource.DataSourceFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

// 此类为druid连接池文件
public class DruidDataSourceFactory implements DataSourceFactory {
    private Properties prop;

    @Override
    public void setProperties(Properties properties) {
        this.prop=properties;
    }

    @Override
    public DataSource getDataSource() {
        DruidDataSource druid = new DruidDataSource();
        druid.setDriverClassName(this.prop.getProperty("driver"));
        druid.setUrl(this.prop.getProperty("url"));
        druid.setUsername(this.prop.getProperty("username"));
        druid.setPassword(this.prop.getProperty("password"));
//        //连接池的最大数据库连接数。设为0表示无限制
//        druid.setMaxActive(Integer.parseInt(this.prop.getProperty("maxactive")));
//        //初始化连接:连接池启动时创建的初始化连接数量
//        druid.setInitialSize(Integer.parseInt(this.prop.getProperty("initialsize")));
        try {
            druid.init();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return druid;
    }

}
