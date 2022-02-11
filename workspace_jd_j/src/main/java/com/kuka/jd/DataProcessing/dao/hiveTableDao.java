package com.kuka.jd.DataProcessing.dao;

import org.apache.ibatis.annotations.Param;

import java.util.ArrayList;
import java.util.List;

public interface hiveTableDao {
    List<String> findTable(@Param("tableName") String tableName);
    void createListTable();
    void createCommentTable();
    void createCsvTable(@Param("tableName") String tableName, @Param("column") String[] column);
    void loadCsv(@Param("HdfsPath") String HdfsPath, @Param("tableName") String tableName);

}
