package com.kuka.jd.DataProcessing.util;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;

// 此类用来创建mysql和hive的session
public class SessionBuilder {
    private static final String configPath = "mybatis-config.xml";
    //用连接池大批量insert数据

    //多数据源切换：读哪个环境，可以当做参数传进来，读hive还是mysql
    public static SqlSession getSession(String db) {
        SqlSession session = null;
        try {
            InputStream is = Resources.getResourceAsStream(configPath);
            SqlSessionFactory factory = new SqlSessionFactoryBuilder().build(is,db.equals("mysql")?"mysql":"hive");
            session = factory.openSession();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return session;
    }

    public static void closeSession(SqlSession session){
        session.close();
    }
}
