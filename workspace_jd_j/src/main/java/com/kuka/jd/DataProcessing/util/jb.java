package com.kuka.jd.DataProcessing.util;

import com.huaban.analysis.jieba.JiebaSegmenter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


// 结巴分词
public class jb {
    public static List<String> analysis(String sentence){
        if (sentence==null) return Arrays.asList("");
        JiebaSegmenter jiebaSegmenter = new JiebaSegmenter();
        return jiebaSegmenter.sentenceProcess(sentence.replaceAll("[，,。”“ ！：（）就；、了呢吧啊没这和用买到挺给多真比说点它在？的一个直才送下+刚有&们看更拿不够倍哈我等也很还是都但]",""));
    }
}
