package com.kuka.jd.DataProcessing.dao;

import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface mysqlTableDao {
    List<String> findTable(@Param("tableName") String tableName);
    void createTable(@Param("tableName") String tableName, @Param("fields") String[] fields);
}
